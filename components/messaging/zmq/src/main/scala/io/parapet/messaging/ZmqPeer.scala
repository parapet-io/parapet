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

import scala.collection.mutable


class ZmqPeer[F[_] : Concurrent](val info: PeerInfo, lock: Lock[F]) extends Peer[F] {
  self =>

  private val ct: Concurrent[F] = implicitly[Concurrent[F]]

  private lazy val zmqContext = new ZContext(1)
  private lazy val socket = zmqContext.createSocket(SocketType.ROUTER)
  private val connectedPeers = mutable.Map[String, ZmqConnection[F]]()

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
            println(s"Peer[$selfAddr] received connect command from [${connect.peerInfo.addr}]")
            // todo validate request (connection exists, encryption, etc.)
            val peerSocket = zmqContext.createSocket(SocketType.DEALER)
            peerSocket.connect(connect.peerInfo.addr)
            val connection = new ZmqConnection[F](info, zmqContext, peerSocket)
            println(s"$selfAddr adds conn to map")
            connectedPeers.put(connect.peerInfo.addr, connection)
            println(s"$selfAddr sends Connected response to ${connect.peerInfo.addr}")
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
              connectedPeers(peerAddr).add(protocolId, stream)
            }

            val peerSocket = connectedPeers.get(peerAddr).map(_.socket).getOrElse({
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

  // tmp solution
  // using java ConcurrentHashMap causes a dead lock: connectedPeers.put(connect.peerInfo.addr, connection)

  override def connect(addr0: String): F[Connection[F]] = {
    lock.withPermit {
      ct.delay {
        if (connectedPeers.contains(addr0)) {
          connectedPeers(addr0)
        } else {
          val socket = zmqContext.createSocket(SocketType.DEALER)
          val conStatus = socket.connect(addr0)
          // todo check conStatus before sending cmd
          println(s"peer[$selfAddr] socket[$addr0] connected = $conStatus")

          val cmd = Command(
            cmdType = CmdType.CONNECT,
            data = Option(Connect(peerInfo = PBPeerInfo(selfAddr)).toByteString)).toByteArray

          socket.send(cmd)

          val res = socket.recv()
          val connected = Connected.parseFrom(res)
          if (connected.ok) {
            println(s"peer[$selfAddr] has successfully connected to $addr0")
            val con = new ZmqConnection(info, zmqContext, socket)
            if(!connectedPeers.contains(addr0)) {
              connectedPeers += (addr0 -> con)
              con
            }else connectedPeers(addr0)
          } else {
            println(s"peer[$selfAddr] connection request was rejected by $addr0")
            throw new RuntimeException(connected.msg.getOrElse(""))
          }
        }
      }
    }

  }

  override def stop: F[Unit] = {
    connectedPeers.values.toList.map(_.close).sequence >>
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
