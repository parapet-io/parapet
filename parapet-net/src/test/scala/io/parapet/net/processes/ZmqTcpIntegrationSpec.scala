package io.parapet.net.processes

import cats.effect.IO
import io.parapet.core.Events.Start
import io.parapet.core.Process
import io.parapet.net.transport.zmq.{ZmqTcpClient, ZmqTcpClientConfig, ZmqTcpServer, ZmqTcpServerConfig}
import io.parapet.net.{Endpoint, TransportProtocol}
import io.parapet.net.transport.Message
import io.parapet.tests.intg.cats.BasicCatsEffectSpec
import io.parapet.testutils.EventStore
import io.parapet.{Event, ProcessRef}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*

import java.net.ServerSocket

class ZmqTcpIntegrationSpec extends AnyFlatSpec with BasicCatsEffectSpec:

  import dsl.*

  "TcpClient -> TcpServer over ZMQ" should "round-trip a request/reply on localhost" in {
    val port    = ZmqTcpIntegrationSpec.freePort()
    val bind    = Endpoint(TransportProtocol.Tcp, "*", port)
    val connect = Endpoint(TransportProtocol.Tcp, "127.0.0.1", port)

    val store = new EventStore[IO, Event]

    unsafeRun {
      ZmqTcpServer.make[IO](ZmqTcpServerConfig(bind, receiveTimeoutMs = 100)).use { tcpServer =>
        ZmqTcpClient.make[IO](ZmqTcpClientConfig(connect, receiveTimeoutMs = 2000)).use { tcpClient =>
          val serverProcess =
            new ServerProcess[IO](tcpServer, ProcessRef[ServerProcess.Received | ServerProcess.Failed]("echo-sink"))
          val clientProcess = new ClientProcess[IO](tcpClient)

          val echo = new Process[IO, ServerProcess.Received | ServerProcess.Failed, ServerProcess.Reply] {
            override val ref: ProcessRef[ServerProcess.Received | ServerProcess.Failed] = ProcessRef("echo-sink")
            override def handle: Receive                                                = {
              case ServerProcess.Received(routingId, message) =>
                val data  = message.parts.headOption.getOrElse(Array.emptyByteArray)
                val reply = new String(data, "UTF-8").toUpperCase.getBytes("UTF-8")
                ServerProcess.Reply(routingId, Message.single(reply)) ~> serverProcess.ref
              case failed @ ServerProcess.Failed(_) =>
                eval(store.add(ref, failed))
            }
          }

          val driver = new Process[IO, Event, Event] {
            override val ref: ProcessRef[Event] = ProcessRef("driver")
            override def handle: Receive        = {
              case Start =>
                ClientProcess.Request(Message.single("ping".getBytes("UTF-8"))) ~> clientProcess.ref
              case response @ ClientProcess.Response(_) =>
                eval(store.add(ref, response))
              case failed @ ClientProcess.Failed(_) =>
                eval(store.add(ref, failed))
            }
          }

          store.await(
            expectedSize = 1,
            op = createApp(ct.pure(Seq(echo, serverProcess, clientProcess, driver))).run
          )
        }
      }
    }

    val replies = store.get(ProcessRef("driver"))
    replies should have size 1
    replies.head match
      case ClientProcess.Response(message) =>
        new String(message.parts.head, "UTF-8") shouldBe "PING"
      case other =>
        fail(s"unexpected reply: $other")
  }

object ZmqTcpIntegrationSpec:
  /** Picks a free TCP port by opening and immediately closing a server socket on port 0. */
  def freePort(): Int =
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
