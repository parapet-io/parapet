package io.parapet.tests.intg.pario

import io.parapet.core.Events.Start
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Process
import io.parapet.core.journal.{EventCodec, EventCodecRegistry, JournalConfig}
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
    val counter2 = new Counter(ref, store2)
    val driver2  = onStart(unit)
    unsafeRun(store2.await(3, createApp(ct.pure(Seq(counter2, driver2)), config0 = config, eventCodecs0 = codecs).run))
    counter2.count shouldBe 6L
  }

object RecoveryIntgSpec:

  final case class Add(n: Int)        extends Event
  final case class Acked(count: Long) extends Event

  object AddCodec extends EventCodec:
    val tag: String                            = "add"
    val version: Int                           = 1
    def encode(event: Event): Try[Array[Byte]] = event match
      case Add(n) => Success(ByteBuffer.allocate(4).putInt(n).array())
      case other  => Failure(new IllegalArgumentException(s"cannot encode $other"))
    def decode(version: Int, bytes: Array[Byte]): Try[Event] = Success(Add(ByteBuffer.wrap(bytes).getInt))

  val codecs: EventCodecRegistry = EventCodecRegistry(classOf[Add] -> AddCodec)

  /** A journaled counter: each `Add` folds into `count` and acks, so the recorded deliveries alone reconstruct it. */
  final class Counter(override val ref: ProcessRef[Event], store: EventStore[ParIO, Event])
      extends Process[ParIO, Event, Event]:

    import dsl.*

    @volatile var count: Long = 0

    def handle: Receive =
      case Start  => unit
      case Add(n) => eval { count += n; store.add(ref, Acked(count)) }
