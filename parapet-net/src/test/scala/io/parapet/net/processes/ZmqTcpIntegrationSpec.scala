package io.parapet.net.processes

import cats.effect.IO
import io.parapet.core.Events.Start
import io.parapet.core.Process
import io.parapet.net.transport.zmq.{
  ZmqTcpClient,
  ZmqTcpClientConfig,
  ZmqTcpDuplexConfig,
  ZmqTcpDuplexTransport,
  ZmqTcpServer,
  ZmqTcpServerConfig
}
import io.parapet.net.{Endpoint, TransportProtocol}
import io.parapet.net.transport.{
  DuplexTransport,
  Message,
  ReceiveResult,
  RoutedMessage,
  ServerTransport,
  TransportError
}
import io.parapet.tests.intg.cats.BasicCatsEffectSpec
import io.parapet.testutils.EventStore
import io.parapet.{Event, ProcessRef}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*

import java.net.ServerSocket
import scala.concurrent.duration.*

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
              case ServerProcess.Received(correlationId, data) =>
                val reply = new String(data, "UTF-8").toUpperCase.getBytes("UTF-8")
                ServerProcess.Reply(correlationId, reply) ~> serverProcess.ref
              case failed @ ServerProcess.Failed(_) =>
                eval(store.add(ref, failed))
            }
          }

          val driver = new Process[IO, Event, Event] {
            override val ref: ProcessRef[Event] = ProcessRef("driver")
            override def handle: Receive        = {
              case Start =>
                ClientProcess.Request("ping".getBytes("UTF-8")) ~> clientProcess.ref
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
      case ClientProcess.Response(data) =>
        new String(data, "UTF-8") shouldBe "PING"
      case other =>
        fail(s"unexpected reply: $other")
  }

  "ZmqTcpDuplexTransport" should "exchange correlated messages with a DEALER peer" in {
    val port    = ZmqTcpIntegrationSpec.freePort()
    val bind    = Endpoint(TransportProtocol.Tcp, "*", port)
    val connect = Endpoint(TransportProtocol.Tcp, "127.0.0.1", port)

    unsafeRun {
      ZmqTcpServer
        .make[IO](ZmqTcpServerConfig(bind, receiveTimeoutMs = 100))
        .use { server =>
          ZmqTcpDuplexTransport.make[IO](ZmqTcpDuplexConfig(connect, receiveTimeoutMs = 100)).use { client =>
            val payload = "ping".getBytes("UTF-8")

            for
              _       <- IO.sleep(100.millis)
              sent    <- client.send(Message("request-1", payload))
              _       <- IO.delay(sent shouldBe Right(()))
              inbound <- ZmqTcpIntegrationSpec.awaitServer(server)
              _       <- IO.delay {
                inbound.message.correlationId shouldBe "request-1"
                new String(inbound.message.payload, "UTF-8") shouldBe "ping"
              }
              _        <- server.reply(inbound.routingId, Message("request-1", "PONG".getBytes("UTF-8")))
              response <- ZmqTcpIntegrationSpec.awaitClient(client)
              _        <- IO.delay {
                response.correlationId shouldBe "request-1"
                new String(response.payload, "UTF-8") shouldBe "PONG"
              }
            yield ()
          }
        }
    }
  }

  it should "create one-shot server routes for each received message" in {
    val port    = ZmqTcpIntegrationSpec.freePort()
    val bind    = Endpoint(TransportProtocol.Tcp, "*", port)
    val connect = Endpoint(TransportProtocol.Tcp, "127.0.0.1", port)

    unsafeRun {
      ZmqTcpServer
        .make[IO](ZmqTcpServerConfig(bind, receiveTimeoutMs = 100))
        .use { server =>
          ZmqTcpDuplexTransport.make[IO](ZmqTcpDuplexConfig(connect, receiveTimeoutMs = 100)).use { client =>
            val request1 = Message("request-1", "ping-1".getBytes("UTF-8"))
            val request2 = Message("request-2", "ping-2".getBytes("UTF-8"))
            val reply1   = Message("request-1", "PONG-1".getBytes("UTF-8"))
            val reply2   = Message("request-2", "PONG-2".getBytes("UTF-8"))

            for
              _     <- IO.sleep(100.millis)
              sent1 <- client.send(request1)
              sent2 <- client.send(request2)
              _     <- IO.delay {
                sent1 shouldBe Right(())
                sent2 shouldBe Right(())
              }
              inbound1 <- ZmqTcpIntegrationSpec.awaitServer(server)
              inbound2 <- ZmqTcpIntegrationSpec.awaitServer(server)
              _        <- IO.delay {
                Set(inbound1.message.correlationId, inbound2.message.correlationId) shouldBe Set(
                  "request-1",
                  "request-2"
                )
                inbound1.routingId should not be inbound2.routingId
              }
              replied1  <- server.reply(inbound1.routingId, reply1)
              duplicate <- server.reply(inbound1.routingId, reply1)
              replied2  <- server.reply(inbound2.routingId, reply2)
              _         <- IO.delay {
                replied1 shouldBe Right(())
                duplicate shouldBe Left(TransportError.UnknownRoute(inbound1.routingId))
                replied2 shouldBe Right(())
              }
              response1 <- ZmqTcpIntegrationSpec.awaitClient(client)
              response2 <- ZmqTcpIntegrationSpec.awaitClient(client)
              _         <- IO.delay {
                Set(response1.correlationId, response2.correlationId) shouldBe Set("request-1", "request-2")
                Set(new String(response1.payload, "UTF-8"), new String(response2.payload, "UTF-8")) shouldBe
                  Set("PONG-1", "PONG-2")
              }
            yield ()
          }
        }
    }
  }

  it should "detect REQ and DEALER peer envelopes on the same server" in {
    val port    = ZmqTcpIntegrationSpec.freePort()
    val bind    = Endpoint(TransportProtocol.Tcp, "*", port)
    val connect = Endpoint(TransportProtocol.Tcp, "127.0.0.1", port)

    unsafeRun {
      ZmqTcpServer.make[IO](ZmqTcpServerConfig(bind, receiveTimeoutMs = 100)).use { server =>
        ZmqTcpClient.make[IO](ZmqTcpClientConfig(connect, receiveTimeoutMs = 2000)).use { reqClient =>
          ZmqTcpDuplexTransport.make[IO](ZmqTcpDuplexConfig(connect, receiveTimeoutMs = 100)).use { dealerClient =>
            val reqRequest    = Message("req-1", "req".getBytes("UTF-8"))
            val dealerRequest = Message("dealer-1", "dealer".getBytes("UTF-8"))
            val reqReply      = Message("req-1", "REQ".getBytes("UTF-8"))
            val dealerReply   = Message("dealer-1", "DEALER".getBytes("UTF-8"))

            for
              reqFiber   <- reqClient.request(reqRequest).start
              _          <- IO.sleep(100.millis)
              dealerSent <- dealerClient.send(dealerRequest)
              _          <- IO.delay(dealerSent shouldBe Right(()))
              inbound1   <- ZmqTcpIntegrationSpec.awaitServer(server)
              inbound2   <- ZmqTcpIntegrationSpec.awaitServer(server)
              _          <- ZmqTcpIntegrationSpec.replyByCorrelation(server, inbound1, reqReply, dealerReply)
              _          <- ZmqTcpIntegrationSpec.replyByCorrelation(server, inbound2, reqReply, dealerReply)
              reqResult  <- reqFiber.joinWithNever
              dealerResp <- ZmqTcpIntegrationSpec.awaitClient(dealerClient)
              _          <- IO.delay {
                reqResult match
                  case Right(response) =>
                    response.correlationId shouldBe "req-1"
                    new String(response.payload, "UTF-8") shouldBe "REQ"
                  case Left(error) =>
                    fail(s"REQ client failed: $error")

                dealerResp.correlationId shouldBe "dealer-1"
                new String(dealerResp.payload, "UTF-8") shouldBe "DEALER"
              }
            yield ()
          }
        }
      }
    }
  }

  it should "expire server routes that are not replied to within the route ttl" in {
    val port    = ZmqTcpIntegrationSpec.freePort()
    val bind    = Endpoint(TransportProtocol.Tcp, "*", port)
    val connect = Endpoint(TransportProtocol.Tcp, "127.0.0.1", port)

    unsafeRun {
      ZmqTcpServer
        .make[IO](
          ZmqTcpServerConfig(
            bind,
            receiveTimeoutMs = 100,
            routeTtlMs = 50
          )
        )
        .use { server =>
          ZmqTcpDuplexTransport.make[IO](ZmqTcpDuplexConfig(connect, receiveTimeoutMs = 100)).use { client =>
            val request = Message("request-ttl", "ping".getBytes("UTF-8"))
            val reply   = Message("request-ttl", "PONG".getBytes("UTF-8"))

            for
              _       <- IO.sleep(100.millis)
              sent    <- client.send(request)
              _       <- IO.delay(sent shouldBe Right(()))
              inbound <- ZmqTcpIntegrationSpec.awaitServer(server)
              _       <- IO.sleep(100.millis)
              replied <- server.reply(inbound.routingId, reply)
              _       <- IO.delay(replied shouldBe Left(TransportError.UnknownRoute(inbound.routingId)))
            yield ()
          }
        }
    }
  }

  "DuplexProcess -> ServerProcess over ZMQ" should "preserve correlation ids across replies" in {
    val port    = ZmqTcpIntegrationSpec.freePort()
    val bind    = Endpoint(TransportProtocol.Tcp, "*", port)
    val connect = Endpoint(TransportProtocol.Tcp, "127.0.0.1", port)
    val store   = new EventStore[IO, Event]

    unsafeRun {
      ZmqTcpServer
        .make[IO](ZmqTcpServerConfig(bind, receiveTimeoutMs = 100))
        .use { serverTransport =>
          ZmqTcpDuplexTransport.make[IO](ZmqTcpDuplexConfig(connect, receiveTimeoutMs = 100)).use { duplexTransport =>
            val serverProcess =
              new ServerProcess[IO](
                serverTransport,
                ProcessRef[ServerProcess.Received | ServerProcess.Failed]("duplex-echo")
              )
            val duplexProcess = new DuplexProcess[IO](duplexTransport)

            val echo = new Process[IO, ServerProcess.Received | ServerProcess.Failed, ServerProcess.Reply] {
              override val ref: ProcessRef[ServerProcess.Received | ServerProcess.Failed] = ProcessRef("duplex-echo")
              override def handle: Receive                                                = {
                case ServerProcess.Received(correlationId, data) =>
                  val reply = new String(data, "UTF-8").toUpperCase.getBytes("UTF-8")
                  ServerProcess.Reply(correlationId, reply) ~> serverProcess.ref
                case failed @ ServerProcess.Failed(_) =>
                  eval(store.add(ref, failed))
              }
            }

            val driver = new Process[IO, Event, Event] {
              override val ref: ProcessRef[Event] = ProcessRef("duplex-driver")
              override def handle: Receive        = {
                case Start =>
                  DuplexProcess.Request("ping".getBytes("UTF-8")) ~> duplexProcess.ref
                case response @ DuplexProcess.Response(_) =>
                  eval(store.add(ref, response))
                case failed @ DuplexProcess.Failed(_) =>
                  eval(store.add(ref, failed))
              }
            }

            store.await(
              expectedSize = 1,
              op = createApp(ct.pure(Seq(echo, serverProcess, duplexProcess, driver))).run
            )
          }
        }
    }

    val replies = store.get(ProcessRef("duplex-driver"))
    replies should have size 1
    replies.head match
      case DuplexProcess.Response(data) =>
        new String(data, "UTF-8") shouldBe "PING"
      case other =>
        fail(s"unexpected reply: $other")
  }

  it should "route concurrent replies back to the original sender processes" in {
    val port    = ZmqTcpIntegrationSpec.freePort()
    val bind    = Endpoint(TransportProtocol.Tcp, "*", port)
    val connect = Endpoint(TransportProtocol.Tcp, "127.0.0.1", port)
    val store   = new EventStore[IO, Event]

    unsafeRun {
      ZmqTcpServer
        .make[IO](ZmqTcpServerConfig(bind, receiveTimeoutMs = 100))
        .use { serverTransport =>
          ZmqTcpDuplexTransport.make[IO](ZmqTcpDuplexConfig(connect, receiveTimeoutMs = 100)).use { duplexTransport =>
            val serverProcess =
              new ServerProcess[IO](
                serverTransport,
                ProcessRef[ServerProcess.Received | ServerProcess.Failed]("duplex-multi-echo")
              )
            val duplexProcess = new DuplexProcess[IO](duplexTransport)

            val echo = new Process[IO, ServerProcess.Received | ServerProcess.Failed, ServerProcess.Reply] {
              override val ref: ProcessRef[ServerProcess.Received | ServerProcess.Failed] =
                ProcessRef("duplex-multi-echo")

              override def handle: Receive = {
                case ServerProcess.Received(correlationId, data) =>
                  val reply = new String(data, "UTF-8").toUpperCase.getBytes("UTF-8")
                  ServerProcess.Reply(correlationId, reply) ~> serverProcess.ref
                case failed @ ServerProcess.Failed(_) =>
                  eval(store.add(ref, failed))
              }
            }

            def client(processName: String, payload: String): Process[IO, Event, Event] =
              new Process[IO, Event, Event] {
                override val ref: ProcessRef[Event] = ProcessRef(processName)

                override def handle: Receive = {
                  case Start =>
                    DuplexProcess.Request(payload.getBytes("UTF-8")) ~> duplexProcess.ref
                  case response @ DuplexProcess.Response(_) =>
                    eval(store.add(ref, response))
                  case failed @ DuplexProcess.Failed(_) =>
                    eval(store.add(ref, failed))
                }
              }

            store.await(
              expectedSize = 2,
              op = createApp(
                ct.pure(
                  Seq(
                    echo,
                    serverProcess,
                    duplexProcess,
                    client("duplex-client-a", "ping-a"),
                    client("duplex-client-b", "ping-b")
                  )
                )
              ).run
            )
          }
        }
    }

    def responseFor(ref: ProcessRef[Event]): String =
      store.get(ref).toList match
        case DuplexProcess.Response(data) :: Nil => new String(data, "UTF-8")
        case other                               => fail(s"unexpected events for ${ref.value}: $other")

    responseFor(ProcessRef("duplex-client-a")) shouldBe "PING-A"
    responseFor(ProcessRef("duplex-client-b")) shouldBe "PING-B"
  }

  it should "interleave many concurrent receives and replies on the ROUTER socket without corruption" in {
    val clientCount = 25
    val port        = ZmqTcpIntegrationSpec.freePort()
    val bind        = Endpoint(TransportProtocol.Tcp, "*", port)
    val connect     = Endpoint(TransportProtocol.Tcp, "127.0.0.1", port)
    val store       = new EventStore[IO, Event]

    unsafeRun {
      ZmqTcpServer
        .make[IO](ZmqTcpServerConfig(bind, receiveTimeoutMs = 100))
        .use { serverTransport =>
          ZmqTcpDuplexTransport.make[IO](ZmqTcpDuplexConfig(connect, receiveTimeoutMs = 100)).use { duplexTransport =>
            val serverProcess =
              new ServerProcess[IO](
                serverTransport,
                ProcessRef[ServerProcess.Received | ServerProcess.Failed]("stress-echo")
              )
            val duplexProcess = new DuplexProcess[IO](duplexTransport)

            val echo = new Process[IO, ServerProcess.Received | ServerProcess.Failed, ServerProcess.Reply] {
              override val ref: ProcessRef[ServerProcess.Received | ServerProcess.Failed] = ProcessRef("stress-echo")
              override def handle: Receive                                                = {
                case ServerProcess.Received(correlationId, data) =>
                  val reply = new String(data, "UTF-8").toUpperCase.getBytes("UTF-8")
                  ServerProcess.Reply(correlationId, reply) ~> serverProcess.ref
                case failed @ ServerProcess.Failed(_) =>
                  eval(store.add(ref, failed))
              }
            }

            def client(index: Int): Process[IO, Event, Event] =
              new Process[IO, Event, Event] {
                override val ref: ProcessRef[Event] = ProcessRef(s"stress-client-$index")
                override def handle: Receive        = {
                  case Start =>
                    DuplexProcess.Request(s"ping-$index".getBytes("UTF-8")) ~> duplexProcess.ref
                  case response @ DuplexProcess.Response(_) =>
                    eval(store.add(ref, response))
                  case failed @ DuplexProcess.Failed(_) =>
                    eval(store.add(ref, failed))
                }
              }

            val clients = (0 until clientCount).map(client)

            store.await(
              expectedSize = clientCount,
              op = createApp(ct.pure(Seq(echo, serverProcess, duplexProcess) ++ clients)).run,
              timeout = 30.seconds
            )
          }
        }
    }

    (0 until clientCount).foreach { index =>
      store.get(ProcessRef(s"stress-client-$index")).toList match
        case DuplexProcess.Response(data) :: Nil =>
          new String(data, "UTF-8") shouldBe s"PING-$index"
        case other =>
          fail(s"unexpected events for stress-client-$index: $other")
    }
  }

object ZmqTcpIntegrationSpec:
  /** Picks a free TCP port by opening and immediately closing a server socket on port 0. */
  def freePort(): Int =
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()

  def awaitServer(server: ServerTransport[IO], attempts: Int = 50): IO[RoutedMessage] =
    if attempts <= 0 then IO.raiseError(new AssertionError("timed out waiting for ZMQ server message"))
    else
      server.receive.flatMap {
        case ReceiveResult.Received(value) => IO.pure(value)
        case ReceiveResult.Idle            => IO.sleep(25.millis).flatMap(_ => awaitServer(server, attempts - 1))
        case ReceiveResult.Failed(error)   => IO.raiseError(new AssertionError(s"server receive failed: $error"))
      }

  def awaitClient(client: DuplexTransport[IO], attempts: Int = 50): IO[Message] =
    if attempts <= 0 then IO.raiseError(new AssertionError("timed out waiting for ZMQ client message"))
    else
      client.receive.flatMap {
        case ReceiveResult.Received(value) => IO.pure(value)
        case ReceiveResult.Idle            => IO.sleep(25.millis).flatMap(_ => awaitClient(client, attempts - 1))
        case ReceiveResult.Failed(error)   => IO.raiseError(new AssertionError(s"client receive failed: $error"))
      }

  def replyByCorrelation(
      server: ServerTransport[IO],
      routed: RoutedMessage,
      reqReply: Message,
      dealerReply: Message
  ): IO[Unit] =
    val reply =
      routed.message.correlationId match
        case "req-1"    => reqReply
        case "dealer-1" => dealerReply
        case other      => throw new AssertionError(s"unexpected correlation id: $other")

    server.reply(routed.routingId, reply).flatMap {
      case Right(())   => IO.unit
      case Left(error) => IO.raiseError(new AssertionError(s"reply failed: $error"))
    }
