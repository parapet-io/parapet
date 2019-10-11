package io.parapet.messaging

import cats.effect.Concurrent
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import io.parapet.core.Peer.PeerInfo
import io.parapet.core.processes.PeerProcess
import io.parapet.core.{Connection, Lock, Peer, Process}
import io.parapet.protobuf.protocol.{PeerInfo => PBPeerInfo, _}
import org.zeromq.{SocketType, ZContext, ZMsg}

import scala.collection.JavaConverters._

class ZmqPeer[F[_] : Concurrent](val info: PeerInfo, lock: Lock[F]) extends Peer[F] {
  self =>

  private val ct: Concurrent[F] = implicitly[Concurrent[F]]

  private lazy val zmqContext = new ZContext(1)
  private lazy val socket = zmqContext.createSocket(SocketType.ROUTER)
  private val connectedPeers = new java.util.concurrent.ConcurrentHashMap[String, ZmqConnection[F]]()

  private val selfAddr = Utils.getAddress(info)

  private def rcvCommand: F[Unit] = {
    def step: F[Unit] = {
      ct.delay {
        val zMsg = ZMsg.recvMsg(socket, 0)
        val clientId = zMsg.pop() // todo check if we need to store clientId for further actions
        val msgBuf = zMsg.pop().getData
        zMsg.destroy()
        val cmd = Command.parseFrom(msgBuf)
        cmd.cmdType match {
          case CmdType.CONNECT =>
            val connect = Connect.parseFrom(cmd.data.get.toByteArray)
            val peerAddr = connect.peerInfo.addr
            println(s"Peer[$selfAddr] received 'Connect' from [$peerAddr]")
            // todo validate request (encryption, etc.)
            val peerSocket = zmqContext.createSocket(SocketType.DEALER)
            peerSocket.connect(peerAddr)
            val conn = new ZmqConnection[F](info, zmqContext, peerSocket)

            if (connectedPeers.putIfAbsent(peerAddr, conn) != null) {
              // concurrent invocation of `connect`
              // close socket
              println(s"peer[$selfAddr] connection with $peerAddr already exists. close socket")
              peerSocket.close()
            }

            println(s"$selfAddr sends 'Connected' to $peerAddr")
            val response = new ZMsg()
            response.add(clientId)
            response.add(Connected(ok = true).toByteArray)
            response.send(socket, true)
          case CmdType.NEW_STREAM =>
            val newStream = NewStream.parseFrom(cmd.data.get.toByteArray)
            val protocolId = newStream.protocolId
            val peerAddr = newStream.peerInfo.addr
            println(s"Peer[$selfAddr] received NewStream command from $peerAddr [protocolId=$protocolId]")

            var openedStream = OpenedStream(ok = true)
            if (!connectedPeers.contains(peerAddr)) {
              openedStream = openedStream.withOk(false).withMsg(s"$peerAddr isn't connected")
            } else if (!info.protocols.contains(protocolId)) {
              openedStream = openedStream.withOk(false).withMsg(s"protocolId=$protocolId isn't supported by $selfAddr")
            }

            if (openedStream.ok) {
              println(s"$selfAddr opened stream[peer=$peerAddr, " +
                s"protocolId=$protocolId, in=${newStream.inAddr}, out=${newStream.outAddr}]")
              val inSocket = zmqContext.createSocket(SocketType.ROUTER)
              inSocket.bind(newStream.outAddr)
              val outSocket = zmqContext.createSocket(SocketType.DEALER)
              outSocket.connect(newStream.inAddr)
              val stream = new ZmqStream[F](selfAddr, inSocket, outSocket)
              connectedPeers.get(peerAddr).add(protocolId, stream)
            }

            val peerSocket = Option(connectedPeers.get(peerAddr)).map(_.socket).getOrElse({
              val peerSocket = zmqContext.createSocket(SocketType.DEALER)
              peerSocket.connect(peerAddr)
              peerSocket
            })

            val response = new ZMsg()
            response.add(clientId)
            response.add(openedStream.toByteArray)
            response.send(socket, true)

            if (!connectedPeers.contains(peerAddr)) {
              peerSocket.close()
            }
          // todo validate request and send a response
        }

      } >> step
    }

    step
  }

  def run: F[Unit] = {
    ct.delay {
      socket.bind(selfAddr)
      println(s"peer[addr=$selfAddr] has been started")
    } >> rcvCommand
  }

  override def connect(addr0: String): F[Connection[F]] = {

    ct.delay {
      // optimistic check
      if (connectedPeers.containsKey(addr0)) {
        connectedPeers.get(addr0)
      } else {
        val socket = zmqContext.createSocket(SocketType.DEALER)
        val newConn = new ZmqConnection[F](info, zmqContext, socket)
        val conn = connectedPeers.putIfAbsent(addr0, newConn)
        if (conn == null) {

          val connStatus = socket.connect(addr0)
          println(s"peer[$selfAddr] opened socket[$addr0, $connStatus]")

          val cmd = Command(
            cmdType = CmdType.CONNECT,
            data = Option(Connect(peerInfo = PBPeerInfo(selfAddr)).toByteString)
          ).toByteArray

          socket.send(cmd)

          val res = socket.recv()
          val connected = Connected.parseFrom(res)
          if (connected.ok) {
            println(s"peer[$selfAddr] has successfully connected to $addr0")
            conn
          } else {
            println(s"peer[$selfAddr] connection request was rejected by $addr0")
            throw new RuntimeException(connected.msg.getOrElse(""))
          }
        } else {
          socket.close() // remove socket form ZMQ context
          conn
        }

      }
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
