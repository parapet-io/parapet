package io.parapet.net.processes

import io.parapet.ProcessRef
import io.parapet.core.TestUtils.*
import io.parapet.core.TestUtils.given
import io.parapet.core.api.Cmd
import io.parapet.net.RequestResponseClient
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

class TcpClientProcessSpec extends AnyFunSuite:
  test("client process forwards network reply to a local process") {
    val client = new RequestResponseClient[Id]:
      def request(payload: Array[Byte]): Option[Array[Byte]] =
        Some("pong".getBytes)

      def close: Unit = ()

    val process = new TcpClientProcess[Id](client, ProcessRef("tcp-client"))
    val execution = new Execution()
    val replyTo = ProcessRef("reply")

    process(Cmd.netClient.Send("ping".getBytes, Some(replyTo))).foldMap(new IdInterpreter(execution))

    execution.trace.toList should have size 1
    execution.trace.head.target shouldBe replyTo
    execution.trace.head.event match
      case Cmd.netClient.Rep(Some(payload)) =>
        payload.toSeq shouldBe "pong".getBytes.toSeq
      case other =>
        fail(s"unexpected reply event: $other")
  }
