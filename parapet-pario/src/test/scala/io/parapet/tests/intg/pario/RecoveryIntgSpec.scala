package io.parapet.tests.intg.pario

import io.parapet.core.Events.{Initialize, Restored, Start}
import io.parapet.{ParConfig, Process}
import io.parapet.effect.ParIO
import io.parapet.effect.ParIO.given
import io.parapet.journal.{EventCodec, EventCodecRegistry, JournalConfig, JournalStoreLocal}
import io.parapet.snapshot.{Snapshot, SnapshotConfig, SnapshotStorageLocal, Snapshotable}
import io.parapet.testutils.EventStore
import io.parapet.tests.intg.BasicParIOSpec
import io.parapet.{Event, ProcessRef, ReplayBoundary}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.ByteBuffer
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

class RecoveryIntgSpec extends AnyFunSuite with BasicParIOSpec:

  import RecoveryIntgSpec.*
  import dsl.*

  test("a restart re-folds the recorded journal to reconstruct process state (no snapshot)") {
    val dir    = Files.createTempDirectory("recovery-intg")
    val ref    = ProcessRef[Event]("rec-counter")
    val config = ParConfig.default.copy(journal = JournalConfig(enabled = true, dataDir = dir.toString, batchSize = 1))

    // Run 1: three Add deliveries are recorded to the journal.
    val store1   = new EventStore[ParIO, Event]
    val counter1 = new Counter(ref, store1)
    val driver1  = onStart((1 to 3).map(i => Add(i) ~> ref).reduce(_ ++ _))
    unsafeRun(store1.await(3, createApp(ct.pure(Seq(counter1, driver1)), config0 = config, eventCodecs0 = codecs).run))
    counter1.count shouldBe 6L

    // Run 2 (same data dir, fresh instance, no snapshot): boot re-folds the journal onto the counter before it goes
    // live, so its state is reconstructed from the record alone - the driver sends nothing.
    val store2   = new EventStore[ParIO, Event]
    val counter2 = new Counter(ref, store2, recordInitialize = true, recordStart = true)
    val driver2  = onStart(unit)
    unsafeRun(store2.await(5, createApp(ct.pure(Seq(counter2, driver2)), config0 = config, eventCodecs0 = codecs).run))
    counter2.count shouldBe 6L
    store2.get(ref) shouldBe Seq(InitializedWith(0L), Acked(1L), Acked(3L), Acked(6L), StartedWith(6L))
  }

  test("a registration marker recovers a child created by a replayed parent event") {
    val dir       = Files.createTempDirectory("recovery-child-intg")
    val parentRef = ProcessRef[Event]("rec-parent")
    val childRef  = ProcessRef[Event]("rec-child")
    val config    = ParConfig.default.copy(
      journal = JournalConfig(enabled = true, dataDir = dir.toString, batchSize = 1)
    )

    val store1  = new EventStore[ParIO, Event]
    val parent1 = new Parent(parentRef, childRef, store1)
    val driver1 = onStart(Spawn ~> parentRef)
    unsafeRun(
      store1.await(
        1,
        createApp(ct.pure(Seq(parent1, driver1)), config0 = config, eventCodecs0 = dynamicCodecs).run
      )
    )

    val entries = new JournalStoreLocal[ParIO](JournalStoreLocal.Config(dir)).read(0L).unsafeRunSync()
    entries.map(_.receiver) shouldBe Vector(parentRef, parentRef, childRef)
    entries.map(_.tag) shouldBe Vector(SpawnCodec.tag, "parapet.registered", AddCodec.tag)

    val store2  = new EventStore[ParIO, Event]
    val parent2 = new Parent(parentRef, childRef, store2, recordChildStart = true)
    val driver2 = onStart(unit)
    unsafeRun(
      store2.await(
        2,
        createApp(ct.pure(Seq(parent2, driver2)), config0 = config, eventCodecs0 = dynamicCodecs).run
      )
    )

    store2.get(childRef) shouldBe Seq(Acked(7L), StartedWith(7L))
  }

  test("recovery rejects a child delivery that appears before its registration marker") {
    val dir       = Files.createTempDirectory("recovery-missing-marker")
    val parentRef = ProcessRef[Event]("missing-marker-parent")
    val childRef  = ProcessRef[Event]("missing-marker-child")
    val config    = ParConfig.default.copy(
      journal = JournalConfig(enabled = true, dataDir = dir.toString, batchSize = 1)
    )

    val store1 = new EventStore[ParIO, Event]
    unsafeRun(
      store1.await(
        1,
        createApp(
          ct.pure(Seq(new Parent(parentRef, childRef, store1), onStart(Spawn ~> parentRef))),
          config0 = config,
          eventCodecs0 = dynamicCodecs
        ).run
      )
    )

    val entries = new JournalStoreLocal[ParIO](JournalStoreLocal.Config(dir)).read(0L).unsafeRunSync()
    val marker  = entries.find(_.tag == "parapet.registered").getOrElse(fail("registration marker not recorded"))
    deleteSingleEntrySegment(dir, marker.seq)

    val error = intercept[IllegalStateException] {
      unsafeRun(
        createApp(
          ct.pure(Seq(new Parent(parentRef, childRef, new EventStore[ParIO, Event]), onStart(unit))),
          config0 = config,
          eventCodecs0 = dynamicCodecs
        ).run
      )
    }

    error.getMessage should include(s"process $childRef received entry")
    error.getMessage should include("before recovery")
  }

  test("recovery prepares a child whose registration marker was lost from the journal tail") {
    val dir       = Files.createTempDirectory("recovery-lost-marker-tail")
    val parentRef = ProcessRef[Event]("tail-parent")
    val childRef  = ProcessRef[Event]("tail-child")
    val config    = ParConfig.default.copy(
      journal = JournalConfig(enabled = true, dataDir = dir.toString, batchSize = 1)
    )

    val store1 = new EventStore[ParIO, Event]
    unsafeRun(
      store1.await(
        1,
        createApp(
          ct.pure(Seq(new Parent(parentRef, childRef, store1), onStart(Spawn ~> parentRef))),
          config0 = config,
          eventCodecs0 = dynamicCodecs
        ).run
      )
    )

    val entries    = new JournalStoreLocal[ParIO](JournalStoreLocal.Config(dir)).read(0L).unsafeRunSync()
    val marker     = entries.find(_.tag == "parapet.registered").getOrElse(fail("registration marker not recorded"))
    val childEntry = entries.find(_.receiver == childRef).getOrElse(fail("child delivery not recorded"))
    deleteSingleEntrySegment(dir, marker.seq)
    deleteSingleEntrySegment(dir, childEntry.seq)

    val store2 = new EventStore[ParIO, Event]
    unsafeRun(
      store2.await(
        2,
        createApp(
          ct.pure(
            Seq(
              new Parent(
                parentRef,
                childRef,
                store2,
                recordChildStart = true,
                recordChildInitialize = true
              ),
              onStart(unit)
            )
          ),
          config0 = config,
          eventCodecs0 = dynamicCodecs
        ).run,
        timeout = 5.seconds
      )
    )

    store2.get(childRef) shouldBe Seq(InitializedWith(0L), StartedWith(0L))
  }

  test("a replay boundary skips IO while its replayable child is recovered and replayed") {
    val dir         = Files.createTempDirectory("recovery-boundary")
    val boundaryRef = ProcessRef.root[Event]("storage")
    val childRef    = boundaryRef.child[Event]("state")
    val ioCalls     = new AtomicInteger(0)
    val config      = ParConfig.default.copy(
      journal = JournalConfig(enabled = true, dataDir = dir.toString, batchSize = 1)
    )

    val store1 = new EventStore[ParIO, Event]
    unsafeRun(
      store1.await(
        1,
        createApp(
          ct.pure(
            Seq(
              new StorageBoundary(boundaryRef, childRef, store1, ioCalls, recreateChild = false),
              onStart(Store(9) ~> boundaryRef)
            )
          ),
          config0 = config,
          eventCodecs0 = boundaryCodecs
        ).run
      )
    )
    ioCalls.get() shouldBe 1

    val store2 = new EventStore[ParIO, Event]
    unsafeRun(
      store2.await(
        3,
        createApp(
          ct.pure(
            Seq(
              new StorageBoundary(boundaryRef, childRef, store2, ioCalls, recreateChild = true),
              onStart(unit)
            )
          ),
          config0 = config,
          eventCodecs0 = boundaryCodecs
        ).run
      )
    )

    ioCalls.get() shouldBe 1
    store2.get(childRef) shouldBe Seq(InitializedWith(0L), Acked(9L), StartedWith(9L))
  }

  test("a snapshot restores state and the journal tail is not re-folded on top of it") {
    val dir        = Files.createTempDirectory("recovery-snap-journal")
    val journalDir = dir.resolve("journal").toString
    val snapDir    = dir.resolve("snapshots").toString
    val ref        = ProcessRef[Event]("snap-journal-counter")
    val config     = ParConfig.default.copy(
      journal = JournalConfig(enabled = true, dataDir = journalDir, batchSize = 1),
      snapshot = SnapshotConfig(enabled = true, dataDir = snapDir, maxEventsPerSnapshot = 1)
    )

    // Run 1: three Adds are both journaled and (every delivery) snapshotted.
    val store1   = new EventStore[ParIO, Event]
    val counter1 = new SnapCounter(ref, store1)
    val driver1  = onStart((1 to 3).map(i => Add(i) ~> ref).reduce(_ ++ _))
    unsafeRun(store1.await(3, createApp(ct.pure(Seq(counter1, driver1)), config0 = config, eventCodecs0 = codecs).run))
    counter1.count shouldBe 6L

    // A snapshot covering at least the first delivery must exist, so recovery restores a non-empty prefix.
    val snapSeq = new SnapshotStorageLocal[ParIO](SnapshotStorageLocal.Config(Path.of(snapDir)))
      .latest(ref)
      .unsafeRunSync()
      .map(_.metadata.seq)
      .getOrElse(0L)
    snapSeq should be > 0L

    // Run 2: state comes back from the snapshot; entries at or below the snapshot boundary must NOT be re-folded.
    // `recordAcked = false` silences replay-time acks so the awaited event count is deterministic regardless of
    // exactly where the snapshot boundary landed.
    val store2   = new EventStore[ParIO, Event]
    val counter2 = new SnapCounter(ref, store2, recordAcked = false, recordStart = true)
    unsafeRun(
      store2.await(1, createApp(ct.pure(Seq(counter2, onStart(unit))), config0 = config, eventCodecs0 = codecs).run)
    )

    counter2.count shouldBe 6L                    // restored once - NOT re-folded to more than 6
    store2.get(ref) shouldBe Seq(StartedWith(6L)) // went live with the full, correct state
  }

  test("replay does not append new entries to the journal") {
    val dir    = Files.createTempDirectory("recovery-no-append")
    val ref    = ProcessRef[Event]("no-append-counter")
    val config = ParConfig.default.copy(journal = JournalConfig(enabled = true, dataDir = dir.toString, batchSize = 1))

    val store1  = new EventStore[ParIO, Event]
    val driver1 = onStart((1 to 3).map(i => Add(i) ~> ref).reduce(_ ++ _))
    unsafeRun(
      store1.await(
        3,
        createApp(ct.pure(Seq(new Counter(ref, store1), driver1)), config0 = config, eventCodecs0 = codecs).run
      )
    )
    val before = new JournalStoreLocal[ParIO](JournalStoreLocal.Config(dir)).read(0L).unsafeRunSync().map(_.seq)

    // Run 2 recovers (replay re-folds the three Adds) then goes live; the driver sends nothing.
    val store2   = new EventStore[ParIO, Event]
    val counter2 = new Counter(ref, store2, recordStart = true)
    unsafeRun(
      store2.await(4, createApp(ct.pure(Seq(counter2, onStart(unit))), config0 = config, eventCodecs0 = codecs).run)
    )
    val after = new JournalStoreLocal[ParIO](JournalStoreLocal.Config(dir)).read(0L).unsafeRunSync().map(_.seq)

    counter2.count shouldBe 6L
    after shouldBe before // replay recorded nothing new
  }

  test("a replayed sender does not re-emit its downstream sends") {
    val dir      = Files.createTempDirectory("recovery-suppress")
    val relayRef = ProcessRef[Event]("relay")
    val childRef = ProcessRef[Event]("relay-child")
    val config = ParConfig.default.copy(journal = JournalConfig(enabled = true, dataDir = dir.toString, batchSize = 1))

    // Run 1: driver -> relay: Emit(5); relay -> child: Add(5). Both deliveries are journaled.
    val store1  = new EventStore[ParIO, Event]
    val relay1  = new Relay(relayRef, childRef)
    val child1  = new Counter(childRef, store1)
    val driver1 = onStart(Emit(5) ~> relayRef)
    unsafeRun(
      store1.await(
        1,
        createApp(ct.pure(Seq(relay1, child1, driver1)), config0 = config, eventCodecs0 = relayCodecs).run
      )
    )

    // Run 2: replay drives Emit -> relay (which re-sends Add -> child, and that send must be suppressed) and the
    // recorded Add -> child. If the send were not suppressed the child would fold Add(5) twice.
    val store2 = new EventStore[ParIO, Event]
    val child2 = new Counter(childRef, store2, recordStart = true)
    unsafeRun(
      store2.await(
        2,
        createApp(
          ct.pure(Seq(new Relay(relayRef, childRef), child2, onStart(unit))),
          config0 = config,
          eventCodecs0 = relayCodecs
        ).run
      )
    )

    child2.count shouldBe 5L // folded once, not 10
    store2.get(childRef) shouldBe Seq(Acked(5L), StartedWith(5L))
  }

  test("recovery decodes journal entries written by an earlier codec version") {
    val dir    = Files.createTempDirectory("recovery-versioned")
    val ref    = ProcessRef[Event]("versioned-counter")
    val config = ParConfig.default.copy(journal = JournalConfig(enabled = true, dataDir = dir.toString, batchSize = 1))

    // Run 1: recorded with the v1 codec.
    val store1  = new EventStore[ParIO, Event]
    val driver1 = onStart((1 to 3).map(i => Add(i) ~> ref).reduce(_ ++ _))
    unsafeRun(
      store1.await(
        3,
        createApp(ct.pure(Seq(new Counter(ref, store1), driver1)), config0 = config, eventCodecs0 = codecs).run
      )
    )
    val versions = new JournalStoreLocal[ParIO](JournalStoreLocal.Config(dir))
      .read(0L)
      .unsafeRunSync()
      .filter(_.tag == AddCodec.tag)
      .map(_.schemaVersion)
      .distinct
    versions shouldBe Vector(1)

    // Run 2: the registry now holds a v2 codec for the same tag; its decode must read the v1 bytes.
    val store2   = new EventStore[ParIO, Event]
    val counter2 = new Counter(ref, store2, recordStart = true)
    unsafeRun(
      store2.await(4, createApp(ct.pure(Seq(counter2, onStart(unit))), config0 = config, eventCodecs0 = codecsV2).run)
    )

    counter2.count shouldBe 6L
  }

object RecoveryIntgSpec:

  final case class Add(n: Int)                  extends Event
  final case class Acked(count: Long)           extends Event
  final case class InitializedWith(count: Long) extends Event
  final case class StartedWith(count: Long)     extends Event
  case object Spawn                             extends Event
  final case class Store(n: Int)                extends Event
  final case class RestoredWith(count: Long)    extends Event
  final case class Emit(n: Int)                 extends Event

  object AddCodec extends EventCodec:
    val tag: String                            = "add"
    val version: Int                           = 1
    def encode(event: Event): Try[Array[Byte]] = event match
      case Add(n) => Success(ByteBuffer.allocate(4).putInt(n).array())
      case other  => Failure(new IllegalArgumentException(s"cannot encode $other"))
    def decode(version: Int, bytes: Array[Byte]): Try[Event] = Success(Add(ByteBuffer.wrap(bytes).getInt))

  object SpawnCodec extends EventCodec:
    val tag: String                            = "spawn"
    val version: Int                           = 1
    def encode(event: Event): Try[Array[Byte]] = event match
      case Spawn => Success(Array.emptyByteArray)
      case other => Failure(new IllegalArgumentException(s"cannot encode $other"))
    def decode(version: Int, bytes: Array[Byte]): Try[Event] = Success(Spawn)

  object StoreCodec extends EventCodec:
    val tag: String                            = "store"
    val version: Int                           = 1
    def encode(event: Event): Try[Array[Byte]] = event match
      case Store(n) => Success(ByteBuffer.allocate(4).putInt(n).array())
      case other    => Failure(new IllegalArgumentException(s"cannot encode $other"))
    def decode(version: Int, bytes: Array[Byte]): Try[Event] = Success(Store(ByteBuffer.wrap(bytes).getInt))

  /** Same tag as [[AddCodec]] but schema version 2 (8-byte payload). Decodes both versions, so it can read entries
    * recorded by the v1 codec.
    */
  object AddCodecV2 extends EventCodec:
    val tag: String                            = "add"
    val version: Int                           = 2
    def encode(event: Event): Try[Array[Byte]] = event match
      case Add(n) => Success(ByteBuffer.allocate(8).putLong(n.toLong).array())
      case other  => Failure(new IllegalArgumentException(s"cannot encode $other"))
    def decode(version: Int, bytes: Array[Byte]): Try[Event] = version match
      case 1     => Success(Add(ByteBuffer.wrap(bytes).getInt))
      case 2     => Success(Add(ByteBuffer.wrap(bytes).getLong.toInt))
      case other => Failure(new IllegalArgumentException(s"unsupported add version $other"))

  object EmitCodec extends EventCodec:
    val tag: String                            = "emit"
    val version: Int                           = 1
    def encode(event: Event): Try[Array[Byte]] = event match
      case Emit(n) => Success(ByteBuffer.allocate(4).putInt(n).array())
      case other   => Failure(new IllegalArgumentException(s"cannot encode $other"))
    def decode(version: Int, bytes: Array[Byte]): Try[Event] = Success(Emit(ByteBuffer.wrap(bytes).getInt))

  val codecs: EventCodecRegistry = EventCodecRegistry(classOf[Add] -> AddCodec)

  val codecsV2: EventCodecRegistry = EventCodecRegistry(classOf[Add] -> AddCodecV2)

  val dynamicCodecs: EventCodecRegistry =
    EventCodecRegistry(classOf[Add] -> AddCodec, classOf[Spawn.type] -> SpawnCodec)

  val boundaryCodecs: EventCodecRegistry =
    EventCodecRegistry(classOf[Add] -> AddCodec, classOf[Store] -> StoreCodec)

  val relayCodecs: EventCodecRegistry =
    EventCodecRegistry(classOf[Add] -> AddCodec, classOf[Emit] -> EmitCodec)

  final class StorageBoundary(
      override val ref: ProcessRef[Event],
      childRef: ProcessRef[Event],
      store: EventStore[ParIO, Event],
      ioCalls: AtomicInteger,
      recreateChild: Boolean
  ) extends Process[ParIO, Event, Event]
      with ReplayBoundary:

    import dsl.*

    def handle: Receive =
      case Initialize if recreateChild =>
        register(ref, new Counter(childRef, store, recordStart = true, recordInitialize = true))
      case Initialize => unit
      case Start      => unit
      case Store(n)   =>
        register(ref, new Counter(childRef, store)) ++
          suspend(ParIO.delay(ioCalls.incrementAndGet())).void ++
          (Add(n) ~> childRef)

  final class Parent(
      override val ref: ProcessRef[Event],
      childRef: ProcessRef[Event],
      store: EventStore[ParIO, Event],
      recordChildStart: Boolean = false,
      recordChildInitialize: Boolean = false
  ) extends Process[ParIO, Event, Event]:

    import dsl.*

    def handle: Receive =
      case Start => unit
      case Spawn =>
        register(ref, new Counter(childRef, store, recordChildStart, recordChildInitialize)) ++ (Add(7) ~> childRef)

  /** A journaled counter: each `Add` folds into `count` and acks, so the recorded deliveries alone reconstruct it. */
  final class Counter(
      override val ref: ProcessRef[Event],
      store: EventStore[ParIO, Event],
      recordStart: Boolean = false,
      recordInitialize: Boolean = false
  ) extends Process[ParIO, Event, Event]:

    import dsl.*

    @volatile var count: Long          = 0
    @volatile var initialized: Boolean = false

    def handle: Receive =
      case Initialize =>
        eval {
          initialized = true
          if recordInitialize then store.add(ref, InitializedWith(count))
        }
      case Start  => if recordStart then eval(store.add(ref, StartedWith(count))) else unit
      case Add(n) =>
        eval {
          require(initialized, "Add was delivered before Initialize")
          count += n
          store.add(ref, Acked(count))
        }

  /** A snapshotable counter: its state is a running count, so it can be restored from a snapshot instead of re-folded.
    */
  final class SnapCounter(
      override val ref: ProcessRef[Event],
      store: EventStore[ParIO, Event],
      recordAcked: Boolean = true,
      recordStart: Boolean = false,
      recordRestored: Boolean = false
  ) extends Process[ParIO, Event, Event]
      with Snapshotable:

    import dsl.*

    @volatile var count: Long = 0

    def serialize(): Array[Byte]          = ByteBuffer.allocate(8).putLong(count).array()
    def restore(snapshot: Snapshot): Unit = count = ByteBuffer.wrap(snapshot.data).getLong

    def handle: Receive =
      case Initialize => unit
      case Restored   => if recordRestored then eval(store.add(ref, RestoredWith(count))) else unit
      case Start      => if recordStart then eval(store.add(ref, StartedWith(count))) else unit
      case Add(n)     => eval { count += n; if recordAcked then store.add(ref, Acked(count)) }

  /** Relays each `Emit(n)` as an `Add(n)` to a fixed child - a sender whose send is exercised during replay. */
  final class Relay(
      override val ref: ProcessRef[Event],
      childRef: ProcessRef[Event]
  ) extends Process[ParIO, Event, Event]:

    import dsl.*

    def handle: Receive =
      case Start   => unit
      case Emit(n) => Add(n) ~> childRef

  private def deleteSingleEntrySegment(dir: Path, seq: Long): Unit =
    val value = f"$seq%020d"
    Files.delete(dir.resolve(s"$value-$value.jrnl"))
