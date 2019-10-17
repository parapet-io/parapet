package io.parapet.messaging

import java.util.concurrent.atomic.AtomicReference

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, Fiber}
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import io.parapet.core.Peer.PeerInfo
import io.parapet.core.{Connection, InStream, OutStream, Stream, StreamSource}
import io.parapet.messaging.ZmqConnection.Status.Status
import io.parapet.messaging.ZmqConnection._
import io.parapet.protobuf.protocol.{CmdType, Command, NewStream, NewStreamResponse}
import org.zeromq.ZMQ.Socket
import org.zeromq._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

// many-to-one: many processes
// we need to create a proxy, many-to-one: ? -> proxy -> dealer
// load balancing

// p1 calls newStream
// p1 sends request & waits for response
// p1 receives new stream request
//
class ZmqConnection[F[_] : Concurrent](
                                        peerInfo: PeerInfo,
                                        zmqContext: ZContext,
                                        val socket: Socket,
                                        _ready: Deferred[F, Unit]
                                      ) extends Connection[F] {
  private val ct: Concurrent[F] = implicitly[Concurrent[F]]

  private val thisAddr = Utils.getAddress(peerInfo)

  val streamSources = new java.util.concurrent.ConcurrentHashMap[String, ZmqStreamSource[F]]()


  override def newSteam(protocolId: String): F[StreamSource[F]] = {
    for {
      _ <- _ready.get
      sourceEither <- createOrGetSource(protocolId)
      source <-
        sourceEither match {
          case Left(source) =>
            for {
              _ <- ct.delay(println("==========sendNewStreamMsg"))
              newStreamResponse <- sendNewStreamMsg(protocolId, source.inStreamSource.address)
              _ <- if (newStreamResponse.ok) {
                openStream(protocolId, source, newStreamResponse.streamAddress.get)
              } else {
                rejectStream(protocolId, newStreamResponse.errorMsg.getOrElse("unknown"))
              }

            } yield source
          case Right(source) => ct.pure(source)
        }
    } yield source
  }


  def ready: F[Unit] = {
    ct.handleError(_ready.complete(()))(_ => ())
  }

  def createOrGetSource(protocolId: String): F[Either[ZmqStreamSource[F], ZmqStreamSource[F]]] = {
    // optimistic check
    if (streamSources.containsKey(protocolId)) {
      ct.pure(Right(streamSources.get(protocolId)))
    } else {
      for {
        ready <- Deferred[F, Result]
        sourceEither <- ct.delay {
          val newSource = new ZmqStreamSource[F](new InStreamSource[F](protocolId, zmqContext, "*"), ready)
          val source = streamSources.putIfAbsent(protocolId, newSource)
          if (source == null) {
            Left(newSource)
          } else {
            newSource.inStreamSource.closeFrontendSocket()
            Right(source)
          }
        }
      } yield sourceEither
    }
  }

  private def sendNewStreamMsg(protocolId: String, streamAddr: String): F[NewStreamResponse] = {
    ct.delay {
      val newStreamMsg = NewStream(
        protocolId = protocolId,
        peerAddress = thisAddr,
        streamAddress = streamAddr
      )
      val cmd = Command(
        cmdType = CmdType.NEW_STREAM,
        data = Option(newStreamMsg.toByteString)
      )
      socket.send(cmd.toByteArray, 0)

      println(s"ZmqConnection::sent NewStream[peer=$thisAddr, router/in-stream=$streamAddr] to ${socket.getLastEndpoint}")
    } >> ct.delay {
      NewStreamResponse.parseFrom(socket.recv())
    }
  }



  def rejectStream(protocolId: String, reason: String): F[Unit] = {
    streamSources.remove(protocolId).ready(Failure(reason))
  }

  def openStream(protocolId: String, streamSource: ZmqStreamSource[F], streamAddress: String): F[Unit] = {
    streamSource.open(new OutStreamSource[F](protocolId, streamAddress, zmqContext))
  }

  override def close: F[Unit] = ct.unit // todo
}

object ZmqConnection {

  object Status extends Enumeration {
    type Status = Value
    val Connecting, Open, Closed = Value
  }

  sealed trait Result

  object Success extends Result

  case class Failure(err: String) extends Result


  class ZmqStreamSource[F[_] : Concurrent](
                                            val inStreamSource: InStreamSource[F],
                                            _ready: Deferred[F, Result] // change to Either[String, Unit]
                                          ) extends StreamSource[F] {
    self =>
    private val ct: Concurrent[F] = implicitly[Concurrent[F]]

    private val _status: AtomicReference[Status] = new AtomicReference[Status]()

    private var _outStreamSource: OutStreamSource[F] = _

    def ready(res: Result): F[Unit] = ct.handleError(_ready.complete(res))(_ => ())

    private def outStreamSource(os: OutStreamSource[F]): Unit = _outStreamSource = os

    def outStreamSource: Option[OutStreamSource[F]] = Option(_outStreamSource)

    private def createStream[S <: Stream[F]](thunk: => F[S]): F[S] = {
      for {
        res <- _ready.get
        stream <- res match {
          case Success => ct.suspend(thunk)
          case Failure(err) => ct.raiseError[S](new IllegalStateException(s"Stream cannot be created. err=$err"))
        }
      } yield stream
    }

    override def in: F[InStream[F]] = {
      createStream(inStreamSource.stream)
    }

    override def out: F[OutStream[F]] = {
      createStream(outStreamSource.get.stream)
    }

    def open(os: OutStreamSource[F]): F[Unit] = {
      ct.delay(_status.compareAndSet(null, Status.Open)).flatMap {
        case true => ct.delay(outStreamSource(os)) >> openSources >> ct.delay(println(s"Stream source $self has been opened")) >> ready(Success)
        case false => ct.raiseError(new IllegalStateException(s"stream source cannot be opened in this state: ${_status.get()}"))
      }
    }

    override def close: F[Unit] = {
      ct.delay(_status.compareAndSet(Status.Open, Status.Closed)).flatMap {
        case true => inStreamSource.close >> _outStreamSource.close
        case false => ct.raiseError(new IllegalStateException(s"stream source cannot be opened in this state: ${_status.get()}"))
      }
    }

    private def openSources: F[Unit] = {
      inStreamSource.open >> _outStreamSource.open
    }

    def outStreamSourceAddress: String = {
      outStreamSource.map(_.backend.getLastEndpoint).getOrElse("?")
    }

    def getInStreamSourceAddress: String = inStreamSource.address

    override def toString: String = {
      s"[inAdd=$getInStreamSourceAddress, outAddr=$outStreamSourceAddress]"
    }
  }

  // ========= Out stream ============
  class ZMQOutStream[F[_] : Concurrent](addr: String, zmqCtx: ZContext) extends OutStream[F] {
    private val ct: Concurrent[F] = implicitly[Concurrent[F]]
    private val socket = zmqCtx.createSocket(SocketType.DEALER)
    socket.connect(addr)

    override def write(data: Array[Byte]): F[Unit] = {
      ct.delay {
        socket.send(data)
        ()
      }
    }

    override def close: F[Unit] = ct.unit
  }

  class OutStreamSource[F[_] : Concurrent](protocolId: String, peerAddr: String, zmqCtx: ZContext) {

    private val ct: Concurrent[F] = implicitly[Concurrent[F]]
    private var frontend: Socket = _ // connects stream DEALER socket to this socket: DEALER -> DEALER
    var backend: Socket = _ //  connects to ROUTER socket: DEALER -> ROUTER
    private var fiber: Fiber[F, Unit] = _

    val address = s"inproc://$protocolId-out"

    def open: F[Unit] = {
      val op = for {
        _ <- ct.delay {
          frontend = zmqCtx.createSocket(SocketType.DEALER)
          frontend.bind(address)

          backend = zmqCtx.createSocket(SocketType.DEALER)
          backend.connect(peerAddr)
          println(s"OutStreamSource[backend = $peerAddr]")
        }
        f <- ct.start(ct.delay {
          ZMQ.proxy(frontend, backend, null)
          ()
        })
        _ <- ct.delay(fiber = f)
      } yield ()
      op >> ct.delay(println("opened OutStreamSource"))
    }

    def stream: F[OutStream[F]] = ct.delay(new ZMQOutStream[F](address, zmqCtx))

    def close: F[Unit] = {
      // todo
      ct.unit
    }
  }

  // ========= In stream ============
  class ZMQInStream[F[_] : Concurrent](addr: String, zmqCtx: ZContext) extends InStream[F] {
    private val ct: Concurrent[F] = implicitly[Concurrent[F]]

    private val socket = zmqCtx.createSocket(SocketType.PULL)
    socket.connect(addr)

    override def read: F[Array[Byte]] = {
      ct.delay {
        val msg = ZMsg.recvMsg(socket)
        require(msg.size() == 2, "invalid message format")
        msg.pop() // client id
        msg.pop.getData
      }
    }

    override def close: F[Unit] = {
      ct.delay {
        socket.close()
      }
    }
  }

  class InStreamSource[F[_] : Concurrent](
                                           protocolId: String,
                                           zmqCtx: ZContext,
                                           frontendHost: String, // should be an external IP address of this peer
                                           debugEnabled: Boolean = false) {

    private val ct: Concurrent[F] = implicitly[Concurrent[F]]

    private val backendAddress = s"inproc://$protocolId-in"
    private val captureAddr = s"inproc://$protocolId-in-capture"

    private val frontend = createFrontendSocket
    private var backend: Socket = _

    private var fiber: Fiber[F, Unit] = _

    private val _status = new AtomicReference[Status]()

    private val streams = new java.util.concurrent.ConcurrentLinkedQueue[InStream[F]]()

    // use for debugging
    private lazy val capture = {
      val s = zmqCtx.createSocket(SocketType.DEALER)
      s.bind(captureAddr)
      s
    }

    def address: String = frontend.getLastEndpoint

    // use for debugging
    def readFromCapture(): F[Unit] = {

      def read(s: Socket): F[Unit] = {
        val msg = ZMsg.recvMsg(s)
        msg.pop() // client id
        val data = msg.popString()
        ct.delay(println(s"Capture socket: '$data'")) >> read(s)
      }

      if (debugEnabled) {
        ct.start(ct.delay {
          val s = zmqCtx.createSocket(SocketType.DEALER)
          s.connect(captureAddr)
          s
        }.flatMap(s => read(s))).void
      } else ct.unit

    }

    def open: F[Boolean] = {
      ct.delay(_status.compareAndSet(null, Status.Open)).flatMap {
        case true => for {
          _ <- ct.delay {
            backend = zmqCtx.createSocket(SocketType.PUSH)
            backend.bind(backendAddress)
          }
          _ <- readFromCapture()
          f <- ct.start(ct.delay {
            ZMQ.proxy(frontend, backend, if (debugEnabled) capture else null)
            ()
          })
          _ <- ct.delay(fiber = f)
        } yield true
        case false => ct.pure(false)
      }
    }

    def stream: F[InStream[F]] = {
      ct.delay {
        val stream = new ZMQInStream[F](backendAddress, zmqCtx)
        streams.add(stream)
        stream
      }
    }

    private def createFrontendSocket: Socket = {
      @tailrec
      def create(attemptsLeft: Int): Socket = {
        try {
          val socket = zmqCtx.createSocket(SocketType.ROUTER)
          val port = Utils.findFreePort()
          socket.bind(s"tcp://$frontendHost:$port")
          socket
        } catch {
          case _: ZMQException =>
            if (attemptsLeft <= 0) throw new IllegalStateException("cannot create a socket")
            else create(attemptsLeft - 1)
          case e => throw e
        }

      }

      create(100)
    }

    private def closeStreams: F[Unit] = {
      streams.asScala.toList.map(_.close).sequence.void
    }

    def closeFrontendSocket(): Unit = {
      if (_status.compareAndSet(null, Status.Closed)) {
        frontend.close()
      } else {
        throw new IllegalStateException("closeFrontendSocket cannot be called in this state")
      }
    }

    def close: F[Boolean] = {
      ct.delay(_status.compareAndSet(Status.Open, Status.Closed)).flatMap {
        case true =>
          closeStreams >>
            ct.delay {
              Try {
                frontend.close()
                backend.close()
              } match {
                case scala.util.Failure(e) => e.printStackTrace()
                case _ => ()
              }
            } >> fiber.cancel >> ct.pure(true)
        case false => ct.pure(false)
      }
    }
  }

}
