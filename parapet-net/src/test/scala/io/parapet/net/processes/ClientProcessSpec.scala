package io.parapet.net.processes

import io.parapet.ProcessRef
import io.parapet.core.TestUtils.*
import io.parapet.core.TestUtils.given
import io.parapet.net.transport.{Message, TransportError}
import io.parapet.net.transport.ClientTransport
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

class ClientProcessSpec extends AnyFunSuite:
  test("client process replies to the sender with the transport response") {
    val client = new ClientTransport[TestIO]:
      def request(message: Message): TestIO[Either[TransportError, Message]] =
        TestIO.pure(Right(Message(message.correlationId, "pong".getBytes)))

      def close: TestIO[Unit] =
        TestIO.unit

    val process = new ClientProcess[TestIO](client, ProcessRef("net-client"))
    val fixture = new RuntimeFixture
    val sender  = ProcessRef[ClientProcess.Response | ClientProcess.Failed]("reply")

    fixture.runWithSender(
      sender = sender,
      program = process(ClientProcess.Request("ping".getBytes))
    )

    fixture.captured.toList should have size 1
    fixture.captured.head.receiver shouldBe sender
    fixture.captured.head.event match
      case ClientProcess.Response(data) =>
        data.toSeq shouldBe "pong".getBytes.toSeq
      case other =>
        fail(s"unexpected reply event: $other")
  }

  test("client process replies to the sender with transport failure") {
    val timeout = TransportError.TimedOut("request")
    val client  = new ClientTransport[TestIO]:
      def request(message: Message): TestIO[Either[TransportError, Message]] =
        TestIO.pure(Left(timeout))

      def close: TestIO[Unit] =
        TestIO.unit

    val process = new ClientProcess[TestIO](client, ProcessRef("net-client"))
    val fixture = new RuntimeFixture
    val sender  = ProcessRef[ClientProcess.Response | ClientProcess.Failed]("reply")

    fixture.runWithSender(
      sender = sender,
      program = process(ClientProcess.Request("ping".getBytes))
    )

    fixture.captured.toList should have size 1
    fixture.captured.head.receiver shouldBe sender
    fixture.captured.head.event shouldBe ClientProcess.Failed(timeout)
  }
