package io.parapet.messaging

import cats.effect.Concurrent
import io.parapet.core.Peer.PeerInfo
import io.parapet.core.{Connection, Stream}
import io.parapet.protobuf.protocol.{CmdType, Command, Connect, PeerInfo => PBPeerInfo}
import org.zeromq.ZMQ.Socket
import org.zeromq.{SocketType, ZContext}

class ZmqConnection[F[_] : Concurrent](socket: Socket) extends Connection[F] {
  private val ct: Concurrent[F] = implicitly[Concurrent[F]]

  override def newSteam(protocolId: String): F[Stream[F]] = {
    ZmqStream(protocolId, socket)
  }
}

object ZmqConnection {
  def apply[F[_] : Concurrent](ctx: ZContext,
                               addr: String,
                               peerInfo0: PeerInfo): F[Connection[F]] = {

    val ct: Concurrent[F] = implicitly[Concurrent[F]]

    ct.delay {
      val socket = ctx.createSocket(SocketType.DEALER)
      val conStatus = socket.connect(addr)
      println(s"$addr connected = $conStatus")

      val cmd = Command(cmdType = CmdType.CONNECT, data =
        Option(Connect(peerInfo = PBPeerInfo(peerInfo0.addr)).toByteString)).toByteArray
      println(socket.send(cmd))

      // todo wait for response: socket.recv(0)
      // validate response
      new ZmqConnection(socket)
    }

  }
}
