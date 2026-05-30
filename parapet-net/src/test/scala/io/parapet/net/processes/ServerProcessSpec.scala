package io.parapet.net.processes

import io.parapet.ProcessRef
import io.parapet.core.Events.Stop
import io.parapet.core.TestUtils.*
import io.parapet.core.TestUtils.given
import io.parapet.net.transport.{Message, ReceiveResult, RoutedMessage, RoutingId, ServerTransport, TransportError}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.util.concurrent.ConcurrentHashMap

class ServerProcessSpec extends AnyFunSuite:
  test("server process reports unknown correlation ids to the reply sender") {
    var replied = false

    val transport = new ServerTransport[TestIO]:
      def receive: TestIO[ReceiveResult[RoutedMessage]] =
        TestIO.pure(ReceiveResult.Idle)

      def reply(routingId: RoutingId, message: Message): TestIO[Either[TransportError, Unit]] =
        TestIO.delay {
          replied = true
          Right(())
        }

      def close: TestIO[Unit] =
        TestIO.unit

    val sink    = ProcessRef[ServerProcess.Received | ServerProcess.Failed]("sink")
    val process = new ServerProcess[TestIO](transport, sink)
    val fixture = new RuntimeFixture
    val sender  = ProcessRef[ServerProcess.Failed]("reply-sender")

    fixture.runWithSender(sender, process(ServerProcess.Reply("missing-correlation-id", "pong".getBytes)))

    replied shouldBe false
    fixture.captured.toList should have size 1
    fixture.captured.head.receiver shouldBe sender
    fixture.captured.head.event match
      case ServerProcess.Failed(TransportError.ProtocolViolation(message)) =>
        message should include("missing-correlation-id")
      case other =>
        fail(s"unexpected event: $other")
  }

  test("server process reports reply transport failures to the reply sender") {
    val correlationId = "request-1"
    val routingId     = RoutingId("client-1")
    val error         = TransportError.UnknownRoute(routingId)

    val transport = new ServerTransport[TestIO]:
      def receive: TestIO[ReceiveResult[RoutedMessage]] =
        TestIO.pure(ReceiveResult.Idle)

      def reply(routingId: RoutingId, message: Message): TestIO[Either[TransportError, Unit]] =
        TestIO.pure(Left(error))

      def close: TestIO[Unit] =
        TestIO.unit

    val sink    = ProcessRef[ServerProcess.Received | ServerProcess.Failed]("sink")
    val process = new ServerProcess[TestIO](transport, sink)
    val fixture = new RuntimeFixture
    val sender  = ProcessRef[ServerProcess.Failed]("reply-sender")

    pendingRequests(process).put(correlationId, routingId)
    fixture.runWithSender(sender, process(ServerProcess.Reply(correlationId, "pong".getBytes)))

    fixture.captured.toList should have size 1
    fixture.captured.head.receiver shouldBe sender
    fixture.captured.head.event shouldBe ServerProcess.Failed(error)
  }

  test("server process closes the transport on stop") {
    var closed = false

    val transport = new ServerTransport[TestIO]:
      def receive: TestIO[ReceiveResult[RoutedMessage]] =
        TestIO.pure(ReceiveResult.Idle)

      def reply(routingId: RoutingId, message: Message): TestIO[Either[TransportError, Unit]] =
        TestIO.pure(Right(()))

      def close: TestIO[Unit] =
        TestIO.delay {
          closed = true
        }

    val sink    = ProcessRef[ServerProcess.Received | ServerProcess.Failed]("sink")
    val process = new ServerProcess[TestIO](transport, sink)
    val fixture = new RuntimeFixture

    fixture.run(process(Stop))

    closed shouldBe true
  }

  private def pendingRequests(process: ServerProcess[TestIO]): ConcurrentHashMap[String, RoutingId] =
    val field = process.getClass.getDeclaredFields.find(_.getName.contains("requests")).get
    field.setAccessible(true)
    field.get(process).asInstanceOf[ConcurrentHashMap[String, RoutingId]]
