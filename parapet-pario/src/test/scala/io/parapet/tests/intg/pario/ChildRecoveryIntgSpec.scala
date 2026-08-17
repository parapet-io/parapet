package io.parapet.tests.intg.pario

import io.parapet.core.Events.{Restored, Start}
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Process
import io.parapet.core.snapshot.{Snapshot, SnapshotConfig, Snapshotable}
import io.parapet.effect.ParIO
import io.parapet.effect.ParIO.given
import io.parapet.testutils.EventStore
import io.parapet.tests.intg.BasicParIOSpec
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

/** Recovery recursion: a restored parent re-spawns the children its state lists, and each child is restored from its
  * own snapshot. Uses snapshots only (journal off), so the re-fold is a no-op and the test isolates the re-spawn.
  */
class ChildRecoveryIntgSpec extends AnyFunSuite with BasicParIOSpec:

  import ChildRecoveryIntgSpec.*
  import dsl.*

  test("a restored parent re-spawns its children and they are restored from their own snapshots") {
    val dir        = Files.createTempDirectory("child-recovery")
    val managerRef = ProcessRef[Event]("crec-manager")
    val workerRef  = ProcessRef[Event]("worker-w1")
    val config     = ParConfig.default.copy(
      snapshot = SnapshotConfig(enabled = true, dataDir = dir.toString, maxEventsPerSnapshot = 1)
    )

    // run 1: create worker-w1 and add 5 to it, so the manager (knows w1) and the worker (count=5) both snapshot to disk
    val store1  = new EventStore[ParIO, Event]
    val driver1 = onStart(CreateWorker("w1") ~> managerRef ++ (AddTo("w1", 5) ~> managerRef))
    unsafeRun(store1.await(1, createApp(ct.pure(Seq(new Manager(managerRef, store1), driver1)), config0 = config).run))

    // run 2: restart; recovery restores the manager, its Restored handler re-spawns worker-w1, and the recursion
    // restores that child to its snapshot (count=5), which its own Restored handler reports.
    val store2  = new EventStore[ParIO, Event]
    val driver2 = onStart(unit)
    unsafeRun(store2.await(1, createApp(ct.pure(Seq(new Manager(managerRef, store2), driver2)), config0 = config).run))

    store2.get(workerRef) should contain(RestoredWith(5L))
  }

object ChildRecoveryIntgSpec:

  final case class CreateWorker(id: String)  extends Event
  final case class AddTo(id: String, n: Int) extends Event
  final case class WAdd(n: Int)              extends Event
  final case class Acked(count: Long)        extends Event
  final case class RestoredWith(count: Long) extends Event

  private def encodeCount(c: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(c).array()
  private def decodeCount(b: Array[Byte]): Long = ByteBuffer.wrap(b).getLong

  /** A dynamic, snapshotable child: its state is a running count. */
  final class Worker(override val ref: ProcessRef[Event], store: EventStore[ParIO, Event])
      extends Process[ParIO, Event, Event]
      with Snapshotable:

    import dsl.*

    @volatile var count: Long = 0

    def serialize(): Array[Byte]          = encodeCount(count)
    def restore(snapshot: Snapshot): Unit = count = decodeCount(snapshot.data)

    def handle: Receive =
      case Start    => unit
      case Restored => eval(store.add(ref, RestoredWith(count)))
      case WAdd(n)  => eval { count += n; store.add(ref, Acked(count)) }

  /** A static, snapshotable parent: remembers its worker refs and re-spawns them on Restored. */
  final class Manager(override val ref: ProcessRef[Event], store: EventStore[ParIO, Event])
      extends Process[ParIO, Event, Event]
      with Snapshotable:

    import dsl.*

    @volatile var workers: Map[String, ProcessRef[Event]] = Map.empty

    def serialize(): Array[Byte]          = workers.keys.mkString(",").getBytes(UTF_8)
    def restore(snapshot: Snapshot): Unit =
      workers = new String(snapshot.data, UTF_8)
        .split(",")
        .filter(_.nonEmpty)
        .map(id => id -> ProcessRef[Event](s"worker-$id"))
        .toMap

    def handle: Receive =
      case Start    => unit
      case Restored =>
        workers.values.toList.foldLeft(unit)((acc, wref) => acc ++ register(ref, new Worker(wref, store)).map(_ => ()))
      case CreateWorker(id) =>
        val wref = ProcessRef[Event](s"worker-$id")
        eval(workers += id -> wref) ++ register(ref, new Worker(wref, store)).map(_ => ())
      case AddTo(id, n) => WAdd(n) ~> workers(id)
