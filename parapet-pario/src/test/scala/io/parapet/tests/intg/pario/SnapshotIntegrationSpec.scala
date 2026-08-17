package io.parapet.tests.intg.pario

import io.parapet.core.Events.{Restored, Start}
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.snapshot.SnapshotConfig
import io.parapet.core.Process
import io.parapet.core.snapshot.{Snapshot, SnapshotStorageLocal, Snapshotable}
import io.parapet.effect.ParIO
import io.parapet.effect.ParIO.given
import io.parapet.testutils.EventStore
import io.parapet.tests.intg.BasicParIOSpec
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.ByteBuffer
import java.nio.file.Files

/** End-to-end: with snapshotting enabled, the scheduler snapshots a [[Snapshotable]] process to disk as it consumes
  * deliveries. Runs on the real ParIO runtime (the background snapshot writer needs a real fiber).
  */
class SnapshotIntegrationSpec extends AnyFunSuite with BasicParIOSpec:

  import SnapshotIntegrationSpec.*
  import dsl.*

  test("a Snapshotable process is snapshotted to disk while it runs") {
    val dir        = Files.createTempDirectory("snapshot-intg")
    val counterRef = ProcessRef[Event]("snap-counter")
    val store      = new EventStore[ParIO, Event]

    val config = ParConfig.default.copy(
      // snapshot after every delivery so the final snapshot deterministically reflects the last event
      snapshot = SnapshotConfig(enabled = true, dataDir = dir.toString, maxEventsPerSnapshot = 1)
    )

    val counter = new Counter(counterRef, store)
    val driver  = onStart(((1 to 5).map(i => Add(i) ~> counterRef) :+ (Probe ~> counterRef)).reduce(_ ++ _))

    // await 6 acks (5 Adds + Probe); store.await cancels + joins the app fiber, which flushes the snapshot writer
    // in the scheduler's shutdown finalizer
    unsafeRun(store.await(6, createApp(ct.pure(Seq(counter, driver)), config0 = config).run))

    counter.count shouldBe 15L

    // the state was snapshotted to disk: the latest snapshot holds the final count
    val storage = new SnapshotStorageLocal[ParIO](SnapshotStorageLocal.Config(dir))
    val latest  = storage.latest(counterRef).unsafeRunSync().getOrElse(fail("no snapshot was written"))
    decodeCount(latest.data) shouldBe 15L
    latest.metadata.processRef shouldBe counterRef
  }

  test("a restarted app restores the process, delivers Restored then Start, and continues the seq") {
    val dir        = Files.createTempDirectory("snapshot-restart")
    val counterRef = ProcessRef[Event]("snap-restart-counter")
    val config     = ParConfig.default.copy(
      snapshot = SnapshotConfig(enabled = true, dataDir = dir.toString, maxEventsPerSnapshot = 1)
    )
    val storage = new SnapshotStorageLocal[ParIO](SnapshotStorageLocal.Config(dir))

    // --- run 1: build state and snapshot it ---
    val store1  = new EventStore[ParIO, Event]
    val driver1 = onStart(((1 to 5).map(i => Add(i) ~> counterRef) :+ (Probe ~> counterRef)).reduce(_ ++ _))
    unsafeRun(store1.await(6, createApp(ct.pure(Seq(new Counter(counterRef, store1), driver1)), config0 = config).run))
    val run1Seq = storage.latest(counterRef).unsafeRunSync().getOrElse(fail("no snapshot")).metadata.seq

    // --- run 2: restart over the same data dir ---
    val store2   = new EventStore[ParIO, Event]
    val counter2 = new Counter(counterRef, store2, recordStart = true) // fresh instance, count starts at 0
    val driver2  = onStart(Probe ~> counterRef)
    unsafeRun(store2.await(3, createApp(ct.pure(Seq(counter2, driver2)), config0 = config).run))

    counter2.count shouldBe 15L // state came back from disk
    store2.get(counterRef) shouldBe Seq(RestoredWith(15L), StartedWith(15L), Acked(15L))

    val run2Seq = storage.latest(counterRef).unsafeRunSync().getOrElse(fail("no snapshot")).metadata.seq
    run2Seq should be > run1Seq // seq continued past the previous run
  }

object SnapshotIntegrationSpec:

  final case class Add(n: Int)               extends Event
  case object Probe                          extends Event
  final case class Acked(count: Long)        extends Event
  final case class RestoredWith(count: Long) extends Event
  final case class StartedWith(count: Long)  extends Event

  private def encodeCount(count: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(count).array()
  private def decodeCount(bytes: Array[Byte]): Long = ByteBuffer.wrap(bytes).getLong

  /** A counting process that opts into snapshotting; its serialized state is just the running count. */
  final class Counter(
      override val ref: ProcessRef[Event],
      store: EventStore[ParIO, Event],
      recordStart: Boolean = false
  ) extends Process[ParIO, Event, Event]
      with Snapshotable:

    import dsl.*

    @volatile var count: Long = 0

    def serialize(): Array[Byte]          = encodeCount(count)
    def restore(snapshot: Snapshot): Unit = count = decodeCount(snapshot.data)

    def handle: Receive =
      case Start    => if recordStart then eval(store.add(ref, StartedWith(count))) else unit
      case Restored => eval(store.add(ref, RestoredWith(count)))
      case Add(n)   => eval { count += n; store.add(ref, Acked(count)) }
      case Probe    => eval(store.add(ref, Acked(count)))
