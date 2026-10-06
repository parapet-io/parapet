package io.parapet.tests.intg

import io.parapet.Event.Start
import io.parapet.exceptions.RecoveryContractViolation
import io.parapet.journal.JournalConfig
import io.parapet.tests.intg.RecoveryContractSpec._
import io.parapet.testutils.EventStore
import io.parapet.{Event, EventCodec, ParConfig, Process, Replayable}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files
import scala.util.{Failure, Try}

/** Recovery contract: a replayable process may not perform an operation whose outcome cannot yet be recovered.
  *
  * `RecoveryContractViolation` deliberately bypasses the process error handler and the dead-letter path (see
  * `DslInterpreter.interpret` and `Scheduler.runEffect`), so it surfaces by failing the application rather than as a
  * `DeadLetter`.
  */
abstract class RecoveryContractSpec[F[_]] extends AnyFlatSpec with IntegrationSpec[F] {

  import dsl._

  private given EventCodec[Event] with
    val tag: String  = "recovery-contract"
    val version: Int = 1

    def encode(event: Event): Try[Array[Byte]] =
      Failure(new IllegalArgumentException(s"unsupported recovery-contract event: $event"))

    def decode(version: Int, bytes: Array[Byte]): Try[Event] =
      Failure(new IllegalArgumentException("the recovery-contract codec does not decode business events"))

  "A replayable process" should "fail when it uses Suspend" in
    expectViolation(new Process[F, Event] with Replayable:
      def handle: Receive = { case Start => suspend(ct.delay(())) })

  it should "fail when it uses Fork" in
    expectViolation(new Process[F, Event] with Replayable:
      def handle: Receive = { case Start => fork(eval(())).map(_ => ()) })

  it should "fail when it uses Race" in
    expectViolation(new Process[F, Event] with Replayable:
      def handle: Receive = { case Start => race(eval(()), eval(())).map(_ => ()) })

  it should "fail registration when its input protocol has no codec" in {
    val process = new Process[F, Unencoded.type] with Replayable:
      def handle: Receive = { case Start => unit }

    val error = intercept[Throwable] {
      unsafeRun(createApp(ct.pure(Seq(process)), config0 = journalOn()).run)
    }

    withClue(s"expected a missing-codec failure somewhere in: $error\n") {
      relatedErrors(error)
        .exists(error => Option(error.getMessage).exists(_.contains("does not provide an event codec"))) shouldBe true
    }
  }

  "An ordinary process" should "be allowed to use Suspend while the journal is on" in {
    val eventStore = new EventStore[F, Event]
    val process    = new Process[F, Event] {
      def handle: Receive = { case Start =>
        suspend(ct.delay(())) ++ eval(eventStore.add(ref, Executed))
      }
    }

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(process)), config0 = journalOn()).run))

    eventStore.size shouldBe 1
  }

  "A process" should "be allowed to use Suspend when the journal is off" in {
    val eventStore = new EventStore[F, Event]
    val regular    = new Process[F, Event] {
      def handle: Receive = { case Start =>
        suspend(ct.delay(())) ++ eval(eventStore.add(ref, Executed))
      }
    }

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(regular))).run))

    eventStore.size shouldBe 1
  }

  /** Boots `process` with the journal enabled and asserts the application fails with a contract violation. */
  private def expectViolation(process: Process[F, Event]): Unit = {
    // No event is emitted: the application failure must win EventStore.await's race.
    val events = new EventStore[F, Event]
    val error  = intercept[Throwable] {
      unsafeRun(events.await(1, createApp(ct.pure(Seq(process)), config0 = journalOn()).run))
    }
    // Backends wrap differently (fiber joins, cancellation), so look through the whole chain.
    withClue(s"expected a RecoveryContractViolation somewhere in: $error\n") {
      relatedErrors(error).exists(_.isInstanceOf[RecoveryContractViolation]) shouldBe true
    }
  }

  private def journalOn(): ParConfig =
    ParConfig.default.copy(
      journal = JournalConfig(
        enabled = true,
        dataDir = Files.createTempDirectory("recovery-contract").toString
      )
    )
}

object RecoveryContractSpec {

  case object Executed  extends Event
  case object Unencoded extends Event

  /** The error itself plus every cause and suppressed error reachable from it, without revisiting. */
  def relatedErrors(error: Throwable): Seq[Throwable] = {
    val seen                           = scala.collection.mutable.ListBuffer.empty[Throwable]
    def walk(current: Throwable): Unit =
      if (current != null && !seen.exists(_ eq current)) {
        seen += current
        walk(current.getCause)
        current.getSuppressed.foreach(walk)
      }
    walk(error)
    seen.toSeq
  }
}
