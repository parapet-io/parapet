package io.parapet.messaging

import java.net.Socket

import cats.effect.Concurrent
import cats.syntax.flatMap._
import io.parapet.core.Peer.PeerInfo
import io.parapet.core.{Connection, Peer}
import io.parapet.protobuf.protocol.{CmdType, Command, Connect, NewStream}
import org.zeromq.{SocketType, ZContext, ZMsg}

class ZmqPeer[F[_] : Concurrent](val addr: String) extends Peer[F] {

  private val ct: Concurrent[F] = implicitly[Concurrent[F]]

  private val zmqContext = new ZContext(1)
  private val socket = zmqContext.createSocket(SocketType.ROUTER)
  socket.bind(addr) // never copy paste !!!

  private def rcvCommand: F[Unit] = {
    def step: F[Unit] = {
      ct.delay {
        val zMsg = ZMsg.recvMsg(socket, 0)
        val clientId = zMsg.pop()


        var msgBuf = zMsg.pop().getData
        val cmd = Command.parseFrom(msgBuf)
        cmd.cmdType match {
          case CmdType.CONNECT =>
            val con = Connect.parseFrom(cmd.data.get.toByteArray)
            println(s"Peer[$addr] received connect command from [${con.peerInfo.addr}]")
            // todo validate request and send response
          case CmdType.NEW_STREAM =>
            val newStream = NewStream.parseFrom(cmd.data.get.toByteArray)
            println(s"Peer[$addr] received NewStream command [protocolId=${newStream.protocolId}]")
          // todo validate request and send response
        }
        // todo validate request

      } >> step
    }

    step
  }

  def run: F[Unit] = {
    ct.delay(println(s"peer[addr=$addr] started")) >> rcvCommand
  }

  override def connect(addr0: String): F[Connection[F]] = {
    ZmqConnection[F](zmqContext, addr0, PeerInfo(addr))
  }
}

object ZmqPeer {
  def apply[F[_] : Concurrent](addr: String): F[Peer[F]] = {
    val ct: Concurrent[F] = implicitly[Concurrent[F]]
    ct.delay(new ZmqPeer(addr))
  }
}
