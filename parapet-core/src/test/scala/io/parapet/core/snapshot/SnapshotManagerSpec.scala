package io.parapet.core.snapshot

import io.parapet.core.Clock
import io.parapet.core.TestUtils.{given, *}
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import scala.concurrent.duration.*

object SnapshotManagerSpec:
  /** Minimal owner-serialized state: a string. */
  final class Stateful(initial: String, version: Long = 1L) extends Snapshotable:
    var state: String = initial

    override def schemaVersion: Long = version

    def serialize(): Array[Byte] = state.getBytes(UTF_8)

    def restore(snapshot: Snapshot): Unit =
      state = new String(snapshot.data, UTF_8)

class SnapshotManagerSpec extends AnyFunSuite:
  import SnapshotManagerSpec.*

  private val refA = ProcessRef[Event]("snap-a")
  private val refB = ProcessRef[Event]("snap-b")

  private def newManager(
      dir: Path = Files.createTempDirectory("manager-spec"),
      clock: Clock = Clock()
  ): (Path, SnapshotManager[TestIO]) =
    val storage = new SnapshotStorageLocal[TestIO](SnapshotStorageLocal.Config(dir))
    // apply (no background worker): these tests exercise the synchronous create/restore path
    (dir, SnapshotManager[TestIO](storage, clock).unsafeRun())

  test("assembles a snapshot from process bytes and runtime metadata, and persists it") {
    val clock          = new Clock.Mock(1234.millis)
    val (dir, manager) = newManager(clock = clock)
    val process        = new Stateful("hello", version = 7L)

    val snapshot = manager.create(refA, process, seq = 42L).unsafeRun()

    snapshot.metadata.processRef shouldBe refA
    snapshot.metadata.seq shouldBe 42L
    snapshot.metadata.createdAt shouldBe 1234L
    snapshot.metadata.schemaVersion shouldBe 7L
    new String(snapshot.data, UTF_8) shouldBe "hello"

    // persisted, not just assembled
    val storage = new SnapshotStorageLocal[TestIO](SnapshotStorageLocal.Config(dir))
    storage.read(refA, snapshot.metadata.id).unsafeRun().map(_.metadata) shouldBe Some(snapshot.metadata)
  }

  test("ids are per process: each chain counts independently, identity is (ref, id)") {
    val (_, manager) = newManager()
    val a1           = manager.create(refA, new Stateful("a"), seq = 1L).unsafeRun()
    val b1           = manager.create(refB, new Stateful("b"), seq = 2L).unsafeRun()
    val a2           = manager.create(refA, new Stateful("c"), seq = 3L).unsafeRun()

    a1.metadata.id shouldBe 1L
    b1.metadata.id shouldBe 1L // same id, different ref - no global counter
    a2.metadata.id shouldBe 2L
  }

  test("ids continue after what storage already holds - a restarted manager never reuses an id") {
    val (dir, first) = newManager()
    first.create(refA, new Stateful("a"), seq = 1L).unsafeRun().metadata.id shouldBe 1L
    first.create(refA, new Stateful("b"), seq = 2L).unsafeRun().metadata.id shouldBe 2L

    val (_, second) = newManager(dir) // "restart": fresh counters, same storage
    second.create(refA, new Stateful("c"), seq = 3L).unsafeRun().metadata.id shouldBe 3L
    second.create(refB, new Stateful("d"), seq = 4L).unsafeRun().metadata.id shouldBe 1L // other refs unaffected
  }

  test("lineage: first snapshot has parentId 0; later ones chain per process") {
    val (_, manager) = newManager()
    val a1           = manager.create(refA, new Stateful("a1"), seq = 1L).unsafeRun()
    val b1           = manager.create(refB, new Stateful("b1"), seq = 2L).unsafeRun()
    val a2           = manager.create(refA, new Stateful("a2"), seq = 3L).unsafeRun()

    a1.metadata.parentId shouldBe 0L
    b1.metadata.parentId shouldBe 0L
    a2.metadata.parentId shouldBe a1.metadata.id
  }

  test("restore re-establishes state and re-anchors lineage - a fork is visible and collision-free") {
    val (_, manager) = newManager()
    val process      = new Stateful("v1")
    val s1           = manager.create(refA, process, seq = 10L).unsafeRun()
    process.state = "v2"
    val s2 = manager.create(refA, process, seq = 20L).unsafeRun()

    manager.restore(process, s1).unsafeRun() // go back to the older snapshot
    process.state shouldBe "v1"

    process.state = "v1-fork"
    val s3 = manager.create(refA, process, seq = 25L).unsafeRun()
    s3.metadata.parentId shouldBe s1.metadata.id // forked from s1, not chained after s2
    s3.metadata.id should be > s2.metadata.id    // fork does not reuse s2's id (store would reject it)
  }

  test("restoring an older snapshot on a fresh manager still continues past every stored id") {
    // first run: build ids 1..3 on disk
    val (dir, first) = newManager()
    val p1           = new Stateful("v1")
    val s1           = first.create(refA, p1, seq = 10L).unsafeRun()
    first.create(refA, p1, seq = 20L).unsafeRun() // id 2
    first.create(refA, p1, seq = 30L).unsafeRun() // id 3

    // restart: fresh manager, restore the OLDEST snapshot (point-in-time), then continue
    val (_, second) = newManager(dir)
    val p2          = new Stateful("x")
    second.restore(p2, s1).unsafeRun() // restore id 1 while ids 2,3 are on disk
    p2.state shouldBe "v1"

    // next snapshot must not collide with the stored ids 2 or 3
    val next = second.create(refA, p2, seq = 40L).unsafeRun()
    next.metadata.id shouldBe 4L
    next.metadata.parentId shouldBe s1.metadata.id // forked from the restored snapshot
  }
