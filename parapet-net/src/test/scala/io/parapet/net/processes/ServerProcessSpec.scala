package io.parapet.net.processes

import io.parapet.ProcessRef
import io.parapet.core.Events.Stop
import io.parapet.core.TestUtils.*
import io.parapet.core.TestUtils.given
import io.parapet.net.transport.{Message, ReceiveResult, RoutedMessage, RoutingId, ServerTransport, TransportError}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

class ServerProcessSpec extends AnyFunSuite:
  test("server process sends replies through the transport") {
    val routingId = RoutingId("client-1")
    val response  = Message.single("pong".getBytes)
    var replies   = Vector.empty[(RoutingId, Message)]

    val transport = new ServerTransport[TestIO]:
      def receive: TestIO[ReceiveResult[RoutedMessage]] =
        TestIO.pure(ReceiveResult.Idle)

      def reply(routingId: RoutingId, message: Message): TestIO[Either[TransportError, Unit]] =
        TestIO.delay {
          replies = replies :+ (routingId -> message)
          Right(())
        }

      def close: TestIO[Unit] =
        TestIO.unit

    val sink    = ProcessRef[ServerProcess.Received | ServerProcess.Failed]("sink")
    val process = new ServerProcess[TestIO](transport, sink)
    val fixture = new RuntimeFixture

    fixture.run(process(ServerProcess.Reply(routingId, response)))

    replies shouldBe Vector(routingId -> response)
    fixture.captured shouldBe empty
  }

  test("server process reports reply transport failure to the sink") {
    val routingId = RoutingId("missing-client")
    val error     = TransportError.UnknownRoute(routingId)

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

    fixture.run(process(ServerProcess.Reply(routingId, Message.single("pong".getBytes))))

    fixture.captured.toList should have size 1
    fixture.captured.head.receiver shouldBe sink
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
