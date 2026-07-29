package io.parapet.tests.intg.pario

import io.parapet.core.Clock
import io.parapet.core.snapshot.*
import io.parapet.effect.ParIO
import io.parapet.effect.ParIO.given
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import scala.concurrent.duration.*

/** Integration coverage for [[SnapshotManager]] under a real-concurrency effect (ParIO): the background writer runs in
  * a fiber, which a synchronous test effect cannot exercise. Covers both the synchronous [[SnapshotManager.create]]
  * logic (ids, lineage, seeding) and the async [[SnapshotManager.createAsync]] / [[SnapshotManager.close]] path.
  */
class SnapshotManagerIntgSpec extends AnyFunSuite:

  private val refA = ProcessRef[Event]("snap-a")
  private val refB = ProcessRef[Event]("snap-b")

  /** Minimal owner-serialized state: a string. */
  final private class Stateful(initial: String) extends Snapshotable:
    var state: String                     = initial
    def serialize(): Array[Byte]          = state.getBytes(UTF_8)
    def restore(snapshot: Snapshot): Unit =
      state = new String(snapshot.data, UTF_8)

  extension [A](fa: ParIO[A]) private def run(): A = fa.unsafeRunSync()

  private def storage(dir: Path): SnapshotStorageLocal[ParIO] =
    new SnapshotStorageLocal[ParIO](SnapshotStorageLocal.Config(dir))

  /** Runs `test` with a fresh worker-backed manager over `dir`, closing it afterwards. */
  private def withManager[A](dir: Path = Files.createTempDirectory("snap-mgr-intg"), clock: Clock = Clock())(
      test: (Path, SnapshotManager[ParIO]) => A
  ): A =
    val manager = SnapshotManager[ParIO](storage(dir), clock).run()
    try test(dir, manager)
    finally manager.close.run()

  test("create persists a snapshot with runtime metadata") {
    val dir = Files.createTempDirectory("snap-mgr-intg")
    val mgr = SnapshotManager[ParIO](storage(dir), new Clock.Mock(1234.millis)).run()
    try
      val snapshot = mgr.create(refA, new Stateful("hello"), seq = 42L).run()
      snapshot.metadata.processRef shouldBe refA
      snapshot.metadata.seq shouldBe 42L
      snapshot.metadata.createdAt shouldBe 1234L
      storage(dir).read(refA, snapshot.metadata.id).run().map(_.metadata) shouldBe Some(snapshot.metadata)
    finally mgr.close.run()
  }

  test("ids are per process and continue after what storage already holds across a restart") {
    val dir = Files.createTempDirectory("snap-mgr-intg")
    withManager(dir) { (_, first) =>
      first.create(refA, new Stateful("a"), 1L).run().metadata.id shouldBe 1L
      first.create(refA, new Stateful("b"), 2L).run().metadata.id shouldBe 2L
      first.create(refB, new Stateful("c"), 3L).run().metadata.id shouldBe 1L // independent chain
    }
    withManager(dir) { (_, second) => // "restart": fresh manager, same storage
      second.create(refA, new Stateful("d"), 4L).run().metadata.id shouldBe 3L
      second.create(refB, new Stateful("e"), 5L).run().metadata.id shouldBe 2L
    }
  }

  test("lineage: parentId chains; restoring an older snapshot forks without reusing ids") {
    withManager() { (_, manager) =>
      val process = new Stateful("v1")
      val s1      = manager.create(refA, process, 10L).run()
      process.state = "v2"
      val s2 = manager.create(refA, process, 20L).run()

      s1.metadata.parentId shouldBe 0L
      s2.metadata.parentId shouldBe s1.metadata.id

      manager.restore(process, s1).run()
      process.state shouldBe "v1"

      process.state = "v1-fork"
      val s3 = manager.create(refA, process, 25L).run()
      s3.metadata.parentId shouldBe s1.metadata.id // forked from s1
      s3.metadata.id should be > s2.metadata.id    // does not reuse s2's id
    }
  }

  test("createAsync followed by close stores every enqueued snapshot") {
    withManager() { (dir, manager) =>
      (1 to 20).foreach(i => manager.createAsync(refA, new Stateful(s"s$i"), i.toLong).run())
      manager.close.run() // flushes the queue, then stops the worker

      // close guarantees the backlog is on disk: the latest snapshot reflects the last createAsync
      val latest = storage(dir).latest(refA).run().getOrElse(fail("no snapshot stored"))
      latest.metadata.id shouldBe 20L
      new String(latest.data, UTF_8) shouldBe "s20"
    }
  }

  test("create fails on an out-of-order or duplicate seq (serial-per-process contract)") {
    withManager() { (_, manager) =>
      manager.create(refA, new Stateful("a"), 10L).run()

      assertThrows[IllegalStateException](manager.create(refA, new Stateful("b"), 5L).run())  // seq < previous
      assertThrows[IllegalStateException](manager.create(refA, new Stateful("c"), 10L).run()) // seq == previous

      // the failed calls burned no id: the next in-order snapshot is id 2
      manager.create(refA, new Stateful("d"), 11L).run().metadata.id shouldBe 2L
    }
  }

  test("createAsync after close is a no-op") {
    val dir     = Files.createTempDirectory("snap-mgr-intg")
    val manager = SnapshotManager[ParIO](storage(dir), Clock()).run()
    manager.create(refA, new Stateful("before"), 1L).run()
    manager.close.run()

    manager.createAsync(refA, new Stateful("after"), 2L).run() // dropped, worker stopped

    storage(dir).latest(refA).run().map(_.metadata.id) shouldBe Some(1L)
  }
