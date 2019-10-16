package io.parapet.messaging

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, Fiber}
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.parapet.core.Peer.PeerInfo
import io.parapet.core.{Connection, InStream, OutStream, StreamSource}
import io.parapet.messaging.ZmqConnection._
import io.parapet.protobuf.protocol.{CmdType, Command, NewStream, OpenedStream, PeerInfo => PBPeerInfo}
import org.zeromq.ZMQ.Socket
import org.zeromq.{SocketType, ZContext, ZMQ, ZMQException, ZMsg}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

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


  def ready: F[Unit] = {
    ct.handleError(_ready.complete(()))(_ => ())
  }

  def createOrGetSource(protocolId: String): F[Either[ZmqStreamSource[F], ZmqStreamSource[F]]] = {
    // optimistic check
    if (streamSources.containsKey(protocolId)) {
      ct.pure(Right(streamSources.get(protocolId)))
    } else {
      for {
        ready <- Deferred[F, Unit]
        sourceEither <- ct.delay {
          val newSource = new ZmqStreamSource[F](new InStreamSource[F](protocolId, zmqContext, "*"), ready)
          val source = streamSources.putIfAbsent(protocolId, newSource)
          if (source == null) {
            Left(newSource)
          } else {
            // todo: clear newSource if needed
            println("clear newSource")
            newSource.inStreamSource.frontend.close()
            Right(source)
          }
        }
      } yield sourceEither
    }
  }

  private def sendNewStreamMsg(protocolId: String, sreamAddr: String): F[OpenedStream] = {
    ct.delay {
      val newStreamMsg = NewStream(
        peerInfo = PBPeerInfo(thisAddr),
        protocolId = protocolId,
        address = sreamAddr
      )
      val cmd = Command(
        cmdType = CmdType.NEW_STREAM,
        data = Option(newStreamMsg.toByteString)
      )
      socket.send(cmd.toByteArray, 0)

      println(s"ZmqConnection::sent NewStream[peer=$thisAddr, router=$sreamAddr]")
    } >> ct.delay(OpenedStream.parseFrom(Command.parseFrom(socket.recv()).data.get.toByteArray))
  }

  private def sendStreamOpened(protocolId: String, streamAddr: String): F[Unit] = {
    ct.delay {
      val openedStream = OpenedStream(ok = true, address = Option(streamAddr), peerAddr = thisAddr, protocolId = protocolId)
      val cmd = Command(
        cmdType = CmdType.OPENED_STREAM,
        data = Option(openedStream.toByteString)
      ).toByteArray

      socket.send(cmd, 0)
    }
  }

  override def newSteam(protocolId: String): F[StreamSource[F]] = {
    for {
      _ <- _ready.get
      sourceEither <- createOrGetSource(protocolId)
      source <-
        sourceEither match {
          case Left(source) =>
            for {
              _ <- ct.delay(println("==========sendNewStreamMsg"))
              openedStream <- sendNewStreamMsg(protocolId, source.inStreamSource.frontend.getLastEndpoint)
              _ <- completeStreamSource(protocolId, source, openedStream.address.get)
              _ <- sendStreamOpened(protocolId, source.inStreamSource.frontend.getLastEndpoint)
            } yield source
          case Right(source) =>
            ct.delay(println(s"ZmqConnection::Peer[$thisAddr] stream was created in peer recv loop")) >>
              ct.pure(source)
        }
    } yield source
  }

  def completeStreamSource(protocolId: String, streamSource: ZmqStreamSource[F], streamAddr: String): F[Unit] = {
    streamSource.outStreamSource(new OutStreamSource[F](protocolId, streamAddr, zmqContext)).flatMap {
      case true => streamSource.open >> streamSource.ready >> ct.delay(println("completeStreamSource"))
      case false => ct.delay(println("impossible"))
    }
  }

  override def close: F[Unit] = ct.unit // todo
}

object ZmqConnection {

  // todo
  object Status extends Enumeration {
    type Status = Value
    val Connecting, Open, Closed = Value
  }


  class ZmqStreamSource[F[_] : Concurrent](
                                            val inStreamSource: InStreamSource[F],
                                            _ready: Deferred[F, Unit] // change to Either[String, Unit]
                                          ) extends StreamSource[F] {
    private val ct: Concurrent[F] = implicitly[Concurrent[F]]

    private var _outStreamSource: AtomicReference[OutStreamSource[F]] = new AtomicReference[OutStreamSource[F]]()

    def ready: F[Unit] = ct.handleError(_ready.complete(()))(_ => ())

    def outStreamSource(os: OutStreamSource[F]): F[Boolean] = {
      ct.delay(_outStreamSource.compareAndSet(null, os))
    }

    def outStreamSource: OutStreamSource[F] = {
      _outStreamSource.get
    }

    override def in: F[InStream[F]] = {
      for {
        _ <- _ready.get
        stream <- inStreamSource.stream
        _ <- ct.delay(println(s"InStream[${inStreamSource.frontend.getLastEndpoint}]"))
      } yield stream
    }

    override def out: F[OutStream[F]] = {
      for {
        _ <- _ready.get
        source <- ct.pure(_outStreamSource.get())
        stream <- source.stream
        _ <- ct.delay(println(s"OutStream[${source.backend.getLastEndpoint}]"))
      } yield stream
    }

    override def close: F[Unit] = {
      // todo: close streams
      ct.delay(println("close stream source"))
    }

    override def open: F[Unit] = {
      inStreamSource.open >> _outStreamSource.get().open
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

    val sub = zmqCtx.createSocket(SocketType.SUB)
    sub.connect(addr)
    sub.subscribe("")

    override def read: F[Array[Byte]] = {
      val msg = ZMsg.recvMsg(sub)
      msg.pop() // client id
      ct.delay(msg.pop.getData)
    }
  }

  class InStreamSource[F[_] : Concurrent](
                                           protocolId: String,
                                           zmqCtx: ZContext,
                                           frontendHost: String) {

    val address = s"inproc://$protocolId-in"
    val captureAddr = s"inproc://$protocolId-capture"

    private val ct: Concurrent[F] = implicitly[Concurrent[F]]
    private var fiber: Fiber[F, Unit] = _
    private var backend: Socket = _
    val frontend = createInSocket

    lazy val capture = {
      val s =  zmqCtx.createSocket(SocketType.DEALER)
      s.bind(captureAddr)
      s
    }

    private val opened = new AtomicBoolean()

    def readFromCapture(): F[Unit] = {
      // @tailrec
      def read(s: Socket): F[Unit] = {
        val msg = ZMsg.recvMsg(s)
        msg.pop()
        val data = msg.popString()
        ct.delay(println("Capture: " + data)) >> read(s)
      }

      ct.start(ct.delay {
        val s = zmqCtx.createSocket(SocketType.DEALER)
        s.connect(captureAddr)
        s
      }.flatMap(s => read(s))).void

    }

    def open: F[Boolean] = {
      ct.delay(opened.compareAndSet(false, true)).flatMap {
        case true => for {
          _ <- ct.delay {
            backend = zmqCtx.createSocket(SocketType.PUB)
            backend.bind(address)
          }
        //  _  <- readFromCapture()
          f <- ct.start(ct.delay {
            ZMQ.proxy(frontend, backend, null)
            ()
          })
          _ <- ct.delay(fiber = f)
        } yield true
        case false => ct.pure(false)
      }
    }

    def stream: F[InStream[F]] = {
      ct.delay(new ZMQInStream[F](address, zmqCtx))
    }

    private def createInSocket: Socket = {
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

    // check fiber  !=  null
    def close: F[Unit] = {
      ct.delay(println("close stream source"))
      //      ct.delay(opened.compareAndSet(true, false)).flatMap {
      //        case true => ct.delay(frontend.close()) >> fiber.cancel
      //        case false => ct.unit
      //      }
    }
  }

}
