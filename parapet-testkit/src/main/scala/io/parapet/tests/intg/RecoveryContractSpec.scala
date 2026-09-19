package io.parapet.tests.intg

import io.parapet.Event.Start
import io.parapet.exceptions.RecoveryContractViolation
import io.parapet.journal.JournalConfig
import io.parapet.tests.intg.RecoveryContractSpec._
import io.parapet.testutils.EventStore
import io.parapet.{Event, ParConfig, Process, ReplayBoundary}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Recovery contract: once the journal is on, a process may not perform a nondeterministic operation unless it is a
  * [[ReplayBoundary]], because replay could not reproduce the result.
  *
  * `RecoveryContractViolation` deliberately bypasses the process error handler and the dead-letter path (see
  * `DslInterpreter.interpret` and `Scheduler.runEffect`), so it surfaces by failing the application rather than as a
  * `DeadLetter`.
  */
abstract class RecoveryContractSpec[F[_]] extends AnyFlatSpec with IntegrationSpec[F] {

  import dsl._

  "A recovery-enabled process" should "fail when it uses Suspend" in
    expectViolation(Process[F](_ => { case Start =>
      suspend(ct.delay(()))
    }))

  it should "fail when it uses Fork" in
    expectViolation(Process[F](_ => { case Start =>
      fork(eval(())).map(_ => ())
    }))

  it should "fail when it uses Race" in
    expectViolation(Process[F](_ => { case Start =>
      race(eval(()), eval(())).map(_ => ())
    }))

  "A ReplayBoundary process" should "be allowed to use Suspend while the journal is on" in {
    val eventStore = new EventStore[F, Event]
    val boundary   = new Process[F, Event] with ReplayBoundary {
      def handle: Receive = { case Start =>
        suspend(ct.delay(())) ++ eval(eventStore.add(ref, Executed))
      }
    }

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(boundary)), config0 = journalOn()).run))

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

  case object Executed extends Event

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
