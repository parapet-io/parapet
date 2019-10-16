package io.parapet.messaging

import cats.effect.Concurrent
import cats.effect.concurrent.Deferred
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import io.parapet.core.Peer.PeerInfo
import io.parapet.core.processes.PeerProcess
import io.parapet.core.{Connection, Lock, Peer, Process, StreamSource}
import io.parapet.messaging.ZmqConnection.{OutStreamSource, ZmqStreamSource}
import io.parapet.protobuf.protocol.{PeerInfo => PBPeerInfo, _}
import org.zeromq.{SocketType, ZContext, ZFrame, ZMsg}

import scala.collection.JavaConverters._
import scala.util.Try

class ZmqPeer[F[_] : Concurrent](val info: PeerInfo, lock: Lock[F]) extends Peer[F] {
  self =>

  private val ct: Concurrent[F] = implicitly[Concurrent[F]]

  private lazy val zmqContext = new ZContext(1)
  private lazy val socket = zmqContext.createSocket(SocketType.ROUTER)
  private val connectedPeers = new java.util.concurrent.ConcurrentHashMap[String, ZmqConnection[F]]()

  private val selfAddr = Utils.getAddress(info)

  def processConnectCmd(clientId: ZFrame, connnectMsg: Connect): F[Unit] = {
    val peerAddr = connnectMsg.peerInfo.addr
    for {
      ready <- Deferred[F, Unit]
      newConn <- ct.delay {
        println(s"Peer[$selfAddr] received 'Connect' from [$peerAddr], id=${clientId.toString}")
        // todo validate request (encryption, etc.)
        val peerSocket = zmqContext.createSocket(SocketType.DEALER)
        peerSocket.setIdentity(Utils.getAddress(info).getBytes)
        new ZmqConnection[F](info, zmqContext, peerSocket, ready)
      }
      conn <- ct.delay(connectedPeers.putIfAbsent(peerAddr, newConn))
      _ <- if (conn == null) {
        ct.delay(s"Сonnection with $peerAddr was created from Connect cmd") >>
          ct.delay(newConn.socket.connect(peerAddr)) >> newConn.ready
      } else {
        ct.delay(s"Сonnection with $peerAddr was created by client") >> ct.delay(newConn.socket.close())
      }
      _ <- ct.delay {
        val response = new ZMsg()
        response.add(clientId)
        response.add(Command(cmdType = CmdType.CONNECTED, data = Option(Connected(ok = true).toByteString)).toByteArray)
        response.send(socket, true)
        println(s"$selfAddr sent 'Connected' response to $peerAddr, clientId = ${clientId.toString}")
      }

    } yield ()

  }

  def processNewStreamCmd(clientId: ZFrame, newStream: NewStream): F[Unit] = {
    val protocolId = newStream.protocolId
    val peerAddr = newStream.peerInfo.addr
    val peerStreamAddr = newStream.address

    println(s"Peer[$selfAddr] recv loop: received NewStream command from peer=$peerAddr, stream addr: $peerStreamAddr")

    val conn = connectedPeers.get(peerAddr)
    require(conn != null)

    for {
      sourceEither <- conn.createOrGetSource(protocolId)
      _ <-
        sourceEither match {
          case Left(source) =>
            completeStreamSource(protocolId, source, peerStreamAddr) >>
              sendStreamOpened(protocolId, clientId, conn, source.inStreamSource.frontend.getLastEndpoint)
          case Right(source) =>
              ct.delay(println(s"Peer[$selfAddr] stream source was created from connection")) >>
              sendStreamOpened(protocolId, clientId, conn, source.inStreamSource.frontend.getLastEndpoint)
        }
    } yield ()
  }

  def completeStreamSource(protocolId: String, streamSource: ZmqStreamSource[F], streamAddr: String): F[Unit] = {
    streamSource.outStreamSource(new OutStreamSource[F](protocolId, streamAddr, zmqContext)).flatMap {
      case true => streamSource.open >> ct.delay(println("steam has been opened")) >> log(streamSource)
      case false => ct.delay(println("impossible"))
    }
  }

  def log(streamSource: ZmqStreamSource[F]): F[Unit] = {
    ct.delay {
      println(s"Peer[addr=$selfAddr]::stream source[in=${streamSource.inStreamSource.frontend.getLastEndpoint}, " +
        s"out=${streamSource.outStreamSource.backend.getLastEndpoint}]")
    }
  }

  def sendStreamOpened(protocolId:String, clientId: ZFrame, connection: ZmqConnection[F], streamAddr: String): F[Unit] = {

    ct.delay(println(s"Peer[$selfAddr] recv loop: created stream source on '$streamAddr' with $clientId ${connection.socket.getLastEndpoint}")) >>
    ct.delay {
      val openedStream = OpenedStream(ok = true, address = Option(streamAddr), peerAddr = selfAddr, protocolId =protocolId )
      val msg = new ZMsg()
      msg.add(clientId)
      val cmd = Command(
        cmdType = CmdType.OPENED_STREAM,
        data = Option(openedStream.toByteString)
      ).toByteArray

      msg.add(cmd)
      println(s"OPENED_STREAM sent, id = ${clientId.toString}")
      msg.send(socket, true)

    }
  }

  def recvCmd: F[(ZFrame, Command)] = {
    ct.delay {
      val zMsg = ZMsg.recvMsg(socket, 0)
      val clientId = zMsg.pop() // todo check if we need to store clientId for further actions
      val msgBuf = zMsg.pop().getData
      val cmd = Try(Command.parseFrom(msgBuf)) match {
        case scala.util.Success(c) => c
        case scala.util.Failure(e) =>
          println(clientId)
          println(e.getMessage)
          println(Connected.parseFrom(msgBuf))
          throw e
      }
      //zMsg.destroy()
      (clientId, cmd)
    }
  }

  private def processOpenedStream(openedStream: OpenedStream): F[Unit] = {
    val connection = connectedPeers.get(openedStream.peerAddr)
    val streamSource = connection.streamSources.get(openedStream.protocolId)
    streamSource.ready
  }

  private def rcvCommand: F[Unit] = {
    def step: F[Unit] = {
      recvCmd.flatMap {
        case (clientId, cmd) => cmd.cmdType match {
          case CmdType.CONNECT => processConnectCmd(clientId, Connect.parseFrom(cmd.data.get.toByteArray))
          case CmdType.NEW_STREAM => processNewStreamCmd(clientId, NewStream.parseFrom(cmd.data.get.toByteArray))
//          case CmdType.CONNECTED => ct.delay(println(s"Peer[$selfAddr] received CmdType.CONNECTED"))
         case CmdType.OPENED_STREAM => ct.delay(println("CmdType.OPENED_STREAM")) >>
             processOpenedStream(OpenedStream.parseFrom(cmd.data.get.toByteArray))

        }
      } >> step
    }

    step
  }

  def run: F[Unit] = {
    ct.delay {
      //socket.setIdentity(Utils.getAddress(info).getBytes)
      socket.bind(selfAddr)
      println(s"peer[addr=$selfAddr has been started")
    } >> rcvCommand
  }

  def sendConnectionReq(zmqConnection: ZmqConnection[F]): F[ZmqConnection[F]] = {
    ct.delay {
      //  println(s"peer[$selfAddr] opened socket[$addr0, $connStatus, id = ${new String(peerSocket.getIdentity)}]")
      val cmd = Command(
        cmdType = CmdType.CONNECT,
        data = Option(Connect(peerInfo = PBPeerInfo(selfAddr)).toByteString)
      ).toByteArray

      zmqConnection.socket.send(cmd)
      val res = zmqConnection.socket.recv()
      Connected.parseFrom(Command.parseFrom(res).data.get.toByteArray)
    } >> zmqConnection.ready.map(_ => zmqConnection)

  }

  override def connect(addr0: String): F[Connection[F]] = {
    // optimistic check
    if (connectedPeers.containsKey(addr0)) {
      ct.pure(connectedPeers.get(addr0))
    } else {
      for {
        ready <- Deferred[F, Unit]
        newConn <- ct.delay {
          val peerSocket = zmqContext.createSocket(SocketType.DEALER)
          peerSocket.setIdentity(selfAddr.getBytes)
          new ZmqConnection[F](info, zmqContext, peerSocket, ready)
        }
        conn <- ct.delay(connectedPeers.putIfAbsent(addr0, newConn))
        conn0 <- if (conn == null) {
          ct.delay(newConn.socket.connect(addr0)) >> sendConnectionReq(newConn)
        } else {
          ct.delay {
            newConn.socket.close()
            conn
          }
        }

      } yield conn0
    }

  }

  override def stop: F[Unit] = {
    connectedPeers.values.asScala.toList.map(_.close).sequence >>
      ct.delay(zmqContext.close()) // todo handle exceptions
  }

  override lazy val process: Process[F] = new PeerProcess[F](self)
}

object ZmqPeer {
  def apply[F[_] : Concurrent](peerInfo: PeerInfo): F[Peer[F]] = {
    val ct: Concurrent[F] = implicitly[Concurrent[F]]
    for {
      lock <- Lock[F]
    } yield new ZmqPeer(peerInfo, lock)
  }
}
