package io.parapet.core.intg

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow.{empty => emptyFlow, _}
import io.parapet.core.intg.EventRecoverySpec._
import org.scalatest.Matchers._
import org.scalatest.OptionValues._
import org.scalatest.{FlatSpec, Inside}

class EventRecoverySpec extends FlatSpec with Inside with IntegrationSpec {

  "Failed events" should "be redelivered n times" in {
    val deliveryCount = new AtomicInteger()
    val p = Process[IO] {
      case Request =>
        eval(deliveryCount.incrementAndGet()) ++
          suspend(IO.raiseError(new RuntimeException("unexpected error")))
    }

    val program = Request ~> p ++ terminate
    run(program, p)

    deliveryCount.get() shouldBe 6
  }

  "'Failure' event" should "be delivered after maximum redelivery attempts has been reached" in {
    val receivedFailedEvent = new AtomicReference[Option[Failure]](None)
    val server = Process[IO] {
      case Request =>
        suspend(IO.raiseError(new RuntimeException("server is unavailable")))
    }
    val client = Process.apply1[IO](_ => {
      case Start => Request ~> server
      case e: Failure =>
        eval(receivedFailedEvent.compareAndSet(None, Some(e))) ++ terminate
    })
    run(emptyFlow, client, server)

    receivedFailedEvent.get() should matchPattern {
      case Some(Failure(server.ref, Request, _: EventDeliveryException)) =>
    }
  }

  "Process" should "be able to recover failed events" in {
    val clientReceivedResponse = new AtomicReference[Option[Response]](None)
    val server = Process[IO] {
      case Request =>
        suspend(IO.raiseError(new RuntimeException("server is unavailable")))
    }
    val client = Process.apply1[IO](self => {
      case Start => Request ~> server
      case Failure(server.ref, Request, _: EventDeliveryException) =>
        Response(500) ~> self // fallback, recover from failure
      case r @ Response(500) =>
        eval(clientReceivedResponse.compareAndSet(None, Some(r))) ++ terminate
    })
    run(emptyFlow, client, server)

    clientReceivedResponse.get().value shouldBe Response(500)
  }

  "Unrecoverable events" should "be sent to the parapet-deadletter process" in {
    val receivedDeadLetter = new AtomicReference[Option[DeadLetter]](None)

    val deadLetterProcess = new DeadLetterProcess[IO] {
      override val handle: Receive = {
        case e: DeadLetter =>
          eval(receivedDeadLetter.compareAndSet(None, Some(e))) ++ terminate
      }
    }

    val server = Process[IO] {
      case Request =>
        suspend(IO.raiseError(new RuntimeException("server is unavailable")))
    }

    val client = Process.apply1[IO](_ => {
      case Start => Request ~> server
      case Failure(server.ref, Request, _: EventDeliveryException) =>
        suspend(IO.raiseError(new RuntimeException("can't recover")))
    })

    new SpecApp(emptyFlow, Array(client, server), Some(deadLetterProcess))
      .unsafeRun()

    inside(receivedDeadLetter.get()) {
      case Some(DeadLetter(Failure(processFailedToRecover, failureEvent, recoveryError))) =>
        processFailedToRecover shouldBe client.ref
        recoveryError should matchPattern { case _: EventRecoveryException => }
        recoveryError.getCause should matchPattern {
          case ex: RuntimeException if ex.getMessage == "can't recover" =>
        }
        inside(failureEvent) {
          case Failure(failedProcess, sourceEvent, cause) =>
            failedProcess shouldBe server.ref
            sourceEvent shouldBe Request
            cause should matchPattern { case _: EventDeliveryException => }
            cause.getCause should matchPattern {
              case ex: RuntimeException
                  if ex.getMessage == "server is unavailable" =>
            }
        }
    }
  }

  "Failed events sent by system process" should "be sent to parapet-deadletter" in {
    val receivedDeadLetter = new AtomicReference[Option[DeadLetter]](None)

    val deadLetterProcess = new DeadLetterProcess[IO] {
      override val handle: Receive = {
        case e: DeadLetter =>
          eval(receivedDeadLetter.compareAndSet(None, Some(e))) ++ terminate
      }
    }

    val p = Process.apply1[IO](_ => {
      case Start =>
        suspend(IO.raiseError(new RuntimeException("process is unavailable")))
    })

    new SpecApp(emptyFlow, Array(p), Some(deadLetterProcess)).unsafeRun()

    inside(receivedDeadLetter.get()) {
      case Some(DeadLetter(Failure(pRef, event, error))) =>
        pRef shouldBe p.ref
        event shouldBe Start
        error should matchPattern { case _: EventDeliveryException => }
        error.getCause should matchPattern {
          case ex: RuntimeException
              if ex.getMessage == "process is unavailable" =>
        }
    }
  }

  "Process with no recovery logic" should "send failed event to parapet-deadletter" in {
    val receivedDeadLetter = new AtomicReference[Option[DeadLetter]](None)

    val deadLetterProcess = new DeadLetterProcess[IO] {
      override val handle: Receive = {
        case e: DeadLetter =>
          eval(receivedDeadLetter.compareAndSet(None, Some(e))) ++ terminate
      }
    }

    val server = Process[IO] {
      case Request =>
        suspend(IO.raiseError(new RuntimeException("server is unavailable")))
    }

    val client = Process.apply1[IO](_ => {
      case Start => Request ~> server
    })

    new SpecApp(emptyFlow, Array(client, server), Some(deadLetterProcess))
      .unsafeRun()

    inside(receivedDeadLetter.get()) {
      case Some(DeadLetter(Failure(processFailedToRecover, failureEvent, recoveryError))) =>
        processFailedToRecover shouldBe client.ref
        recoveryError should matchPattern { case _: EventRecoveryException => }
        recoveryError.getMessage shouldBe s"recovery logic isn't defined in process[id=${client.ref}]"
        inside(failureEvent) {
          case Failure(failedProcess, sourceEvent, cause) =>
            failedProcess shouldBe server.ref
            sourceEvent shouldBe Request
            cause should matchPattern { case _: EventDeliveryException => }
            cause.getCause should matchPattern {
              case ex: RuntimeException
                  if ex.getMessage == "server is unavailable" =>
            }
        }
    }
  }

}

object EventRecoverySpec {

  case object Request extends Event
  case class Response(code: Int) extends Event

  class SystemProcess(val handle: ReceiveF[IO]) extends Process[IO] {
    override val ref: ProcessRef = ProcessRef.SystemRef
  }

}
