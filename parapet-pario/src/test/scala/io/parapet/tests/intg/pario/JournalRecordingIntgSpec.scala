package io.parapet.tests.intg.pario

import io.parapet.core.Events.Start
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Process
import io.parapet.core.journal.{EventCodec, JournalConfig, JournalStoreLocal}
import io.parapet.core.snapshot.{Snapshot, Snapshotable}
import io.parapet.effect.ParIO
import io.parapet.effect.ParIO.given
import io.parapet.testutils.EventStore
import io.parapet.tests.intg.BasicParIOSpec
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.ByteBuffer
import java.nio.file.Files
import scala.util.{Failure, Success, Try}

class JournalRecordingIntgSpec extends AnyFunSuite with BasicParIOSpec:

  import JournalRecordingIntgSpec.*
  import dsl.*

  test("business deliveries to a recoverable process are recorded to the journal, in seq order") {
    val dir        = Files.createTempDirectory("journal-intg")
    val counterRef = ProcessRef[Event]("jrnl-counter")
    val store      = new EventStore[ParIO, Event]

    val config  = ParConfig.default.copy(journal = JournalConfig(enabled = true, dataDir = dir.toString, batchSize = 1))
    val counter = new Counter(counterRef, store)
    val driver  = onStart(((1 to 3).map(i => Add(i) ~> counterRef) :+ (Probe ~> counterRef)).reduce(_ ++ _))

    unsafeRun(store.await(4, createApp(ct.pure(Seq(counter, driver)), config0 = config).run))
    counter.count shouldBe 6L

    val entries = new JournalStoreLocal[ParIO](JournalStoreLocal.Config(dir)).read(0L).unsafeRunSync()

    entries.map(e => counter.decode(e.event).get) shouldBe Vector(Add(1), Add(2), Add(3), Probe)
    entries.map(_.receiver).distinct shouldBe Vector(counterRef) // driver is not recoverable; Start is not journalled
    entries.map(_.seq) shouldBe entries.map(_.seq).sorted        // ascending
  }

object JournalRecordingIntgSpec:

  final case class Add(n: Int)        extends Event
  case object Probe                   extends Event
  final case class Acked(count: Long) extends Event

  private def encodeCount(count: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(count).array()
  private def decodeCount(bytes: Array[Byte]): Long = ByteBuffer.wrap(bytes).getLong

  /** A recoverable counter: snapshots its running count and codes its inbound events. */
  final class Counter(override val ref: ProcessRef[Event], store: EventStore[ParIO, Event])
      extends Process[ParIO, Event, Event]
      with Snapshotable
      with EventCodec:

    import dsl.*

    @volatile var count: Long = 0

    def serialize(): Array[Byte]          = encodeCount(count)
    def restore(snapshot: Snapshot): Unit = count = decodeCount(snapshot.data)

    def encode(event: Event): Try[Array[Byte]] = event match
      case Add(n) => Success(ByteBuffer.allocate(5).put(0.toByte).putInt(n).array())
      case Probe  => Success(Array(1.toByte))
      case other  => Failure(new IllegalArgumentException(s"cannot encode $other"))

    def decode(bytes: Array[Byte]): Try[Event] =
      val buf = ByteBuffer.wrap(bytes)
      buf.get() match
        case 0   => Success(Add(buf.getInt))
        case 1   => Success(Probe)
        case tag => Failure(new IllegalArgumentException(s"unknown tag $tag"))

    def handle: Receive =
      case Start  => unit
      case Add(n) => eval { count += n; store.add(ref, Acked(count)) }
      case Probe  => eval(store.add(ref, Acked(count)))
