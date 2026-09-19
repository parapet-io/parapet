package io.parapet.tests.intg.pario

import io.parapet.Event.Start
import io.parapet.{ParConfig, ParIOApp, Process}
import io.parapet.effect.ParIO
import io.parapet.effect.ParIO.given
import io.parapet.journal.{
  EventCodec,
  EventCodecRegistry,
  JournalConfig,
  JournalEntry,
  JournalStore,
  JournalStoreLocal,
  JournalWriteMode
}
import io.parapet.snapshot.{Snapshot, Snapshotable}
import io.parapet.testutils.EventStore
import io.parapet.tests.intg.BasicParIOSpec
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CountDownLatch, TimeUnit, TimeoutException}
import scala.util.{Failure, Success, Try}

class JournalRecordingIntgSpec extends AnyFunSuite with BasicParIOSpec:

  import JournalRecordingIntgSpec.*
  import dsl.*

  test("business deliveries to a recoverable process are recorded to the journal, in seq order") {
    val dir        = Files.createTempDirectory("journal-intg")
    val counterRef = ProcessRef[Event]("jrnl-counter")
    val store      = new EventStore[ParIO, Event]

    val config = ParConfig.default.copy(
      journal = JournalConfig(
        enabled = true,
        dataDir = dir.toString,
        batchSize = 1,
        writeMode = JournalWriteMode.WriteAhead
      )
    )
    val counter = new Counter(counterRef, store)
    val driver  = onStart(((1 to 3).map(i => Add(i) ~> counterRef) :+ (Probe ~> counterRef)).reduce(_ ++ _))

    unsafeRun(store.await(4, createApp(ct.pure(Seq(counter, driver)), config0 = config, eventCodecs0 = codecs).run))
    counter.count shouldBe 6L

    val entries = new JournalStoreLocal[ParIO](JournalStoreLocal.Config(dir)).read(0L).unsafeRunSync()

    entries.map(e => codecs.codecForTag(e.tag).get.decode(e.schemaVersion, e.event).get) shouldBe
      Vector(Add(1), Add(2), Add(3), Probe)
    entries.map(_.receiver).distinct shouldBe Vector(counterRef) // driver is not recoverable; Start is not journalled
    entries.map(_.seq) shouldBe entries.map(_.seq).sorted        // ascending
  }

  test("write-ahead mode makes the delivery durable before invoking its handler") {
    val dir          = Files.createTempDirectory("journal-write-ahead")
    val counterRef   = ProcessRef[Event]("write-ahead-counter")
    val events       = new EventStore[ParIO, Event]
    val durableStore = new JournalStoreLocal[ParIO](JournalStoreLocal.Config(dir))
    val gatedStore   = new GatedJournalStore(durableStore)
    val counter      = new Counter(counterRef, events)
    val driver       = onStart(Add(1) ~> counterRef)
    val appConfig    = ParConfig.default.copy(
      journal = JournalConfig(
        enabled = true,
        dataDir = dir.toString,
        batchSize = 64,
        writeMode = JournalWriteMode.WriteAhead
      )
    )
    val app = new ParIOApp:
      override val config: ParConfig                                             = appConfig
      override def processes(args: Array[String]): ParIO[Seq[Process[ParIO, ?]]] =
        ParIO.pure(Seq(counter, driver))
      override def eventCodecs: EventCodecRegistry     = codecs
      override def journalStorage: JournalStore[ParIO] = gatedStore

    val outcome = new AtomicReference[Try[Unit]]()
    val thread  = new Thread(
      () => outcome.set(Try(app.unsafeRun(events.await(1, app.run)))),
      "write-ahead-runtime"
    )

    try
      thread.start()
      gatedStore.awaitAppend()

      counter.count shouldBe 0L
      durableStore.read(0L).unsafeRunSync() shouldBe empty

      gatedStore.release()
      thread.join(TimeUnit.SECONDS.toMillis(10L))
      thread.isAlive shouldBe false
      Option(outcome.get()).getOrElse(fail("runtime terminated without reporting an outcome")).get

      counter.count shouldBe 1L
      durableStore.read(0L).unsafeRunSync().map(_.seq) shouldBe Vector(1L)
    finally
      gatedStore.release()
      if thread.isAlive then thread.interrupt()
      thread.join(1000L)
  }

  test("a restart continues the delivery seq past the journal instead of overwriting recorded history") {
    val dir        = Files.createTempDirectory("journal-restart")
    val counterRef = ProcessRef[Event]("jrnl-counter")
    // Journal only, no snapshots: nothing but the journal seed can stop the seq from restarting at 1 on the second boot.
    val config = ParConfig.default.copy(
      journal = JournalConfig(
        enabled = true,
        dataDir = dir.toString,
        batchSize = 1,
        writeMode = JournalWriteMode.WriteAhead
      )
    )

    def runOnce(): Unit =
      val store   = new EventStore[ParIO, Event]
      val counter = new Counter(counterRef, store)
      val driver  = onStart(((1 to 3).map(i => Add(i) ~> counterRef) :+ (Probe ~> counterRef)).reduce(_ ++ _))
      unsafeRun(store.await(4, createApp(ct.pure(Seq(counter, driver)), config0 = config, eventCodecs0 = codecs).run))

    runOnce() // records seqs 1..4
    runOnce() // must continue at 5..8, not reuse 1..4 and overwrite the segments

    val entries = new JournalStoreLocal[ParIO](JournalStoreLocal.Config(dir)).read(0L).unsafeRunSync()
    entries.map(_.seq) shouldBe (1L to 8L).toVector
    entries.map(_.id).distinct.size shouldBe 8 // envelope ids continue across the restart, so none collide
  }

object JournalRecordingIntgSpec:

  final private class GatedJournalStore(delegate: JournalStore[ParIO]) extends JournalStore[ParIO]:
    private val appendEntered = new CountDownLatch(1)
    private val allowAppend   = new CountDownLatch(1)

    def awaitAppend(): Unit =
      if !appendEntered.await(10L, TimeUnit.SECONDS) then throw new TimeoutException("journal append did not start")

    def release(): Unit = allowAppend.countDown()

    override def append(entries: Vector[JournalEntry]): ParIO[Unit] =
      ParIO
        .blocking {
          appendEntered.countDown()
          if !allowAppend.await(10L, TimeUnit.SECONDS) then
            throw new TimeoutException("journal append was not released")
        }
        .flatMap(_ => delegate.append(entries))

    override def read(afterSeq: Long): ParIO[Vector[JournalEntry]] = delegate.read(afterSeq)

    override def maxSeq: ParIO[Option[Long]] = delegate.maxSeq

    override def maxEnvelopeId: ParIO[Option[Long]] = delegate.maxEnvelopeId

    override def truncate(upToSeq: Long): ParIO[Unit] = delegate.truncate(upToSeq)

  final case class Add(n: Int)        extends Event
  case object Probe                   extends Event
  final case class Acked(count: Long) extends Event

  object AddCodec extends EventCodec:
    val tag: String                            = "add"
    val version: Int                           = 1
    def encode(event: Event): Try[Array[Byte]] = event match
      case Add(n) => Success(ByteBuffer.allocate(4).putInt(n).array())
      case other  => Failure(new IllegalArgumentException(s"cannot encode $other"))
    def decode(version: Int, bytes: Array[Byte]): Try[Event] = Success(Add(ByteBuffer.wrap(bytes).getInt))

  object ProbeCodec extends EventCodec:
    val tag: String                            = "probe"
    val version: Int                           = 1
    def encode(event: Event): Try[Array[Byte]] = event match
      case Probe => Success(Array.emptyByteArray)
      case other => Failure(new IllegalArgumentException(s"cannot encode $other"))
    def decode(version: Int, bytes: Array[Byte]): Try[Event] = Success(Probe)

  val codecs: EventCodecRegistry = EventCodecRegistry(classOf[Add] -> AddCodec, classOf[Probe.type] -> ProbeCodec)

  private def encodeCount(count: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(count).array()
  private def decodeCount(bytes: Array[Byte]): Long = ByteBuffer.wrap(bytes).getLong

  /** A recoverable counter: snapshots its running count. */
  final class Counter(override val ref: ProcessRef[Event], store: EventStore[ParIO, Event])
      extends Process[ParIO, Event]
      with Snapshotable:

    import dsl.*

    @volatile var count: Long = 0

    def serialize(): Array[Byte]          = encodeCount(count)
    def restore(snapshot: Snapshot): Unit = count = decodeCount(snapshot.data)

    def handle: Receive =
      case Start  => unit
      case Add(n) => eval { count += n; store.add(ref, Acked(count)) }
      case Probe  => eval(store.add(ref, Acked(count)))
