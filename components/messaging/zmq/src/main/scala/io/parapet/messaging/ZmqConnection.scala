package io.parapet.messaging

import cats.effect.Concurrent
import io.parapet.core.Peer.PeerInfo
import io.parapet.core.{Connection, Stream}
import io.parapet.protobuf.protocol.{CmdType, Command, NewStream, OpenedStream, PeerInfo => PBPeerInfo}
import org.zeromq.ZMQ.Socket
import org.zeromq.{SocketType, ZContext}

import scala.collection.mutable

class ZmqConnection[F[_] : Concurrent](
                                        peerInfo: PeerInfo,
                                        zmqContext: ZContext,
                                        val socket: Socket) extends Connection[F] {
  private val ct: Concurrent[F] = implicitly[Concurrent[F]]

  private val thisAddr = Utils.getAddress(peerInfo)

  // protocol id -> stream
  // todo id -> [(streamId,stream)]
  // using ConcurrentHashMap is a temp solution to avoid race conditions
  private val openStreams = mutable.Map[String, Stream[F]]()

  override def newSteam(protocolId: String): F[Stream[F]] = {
    ct.delay {
      if (openStreams.contains(protocolId)) {
        openStreams(protocolId)
      } else {
        println(s"$thisAddr tries to create stream with ${socket.getLastEndpoint}")
        val inPort = peerInfo.ports.take
        val outPort = peerInfo.ports.take

        val inAddr = Utils.getAddress(peerInfo.protocol, peerInfo.host, inPort)
        val outAddr = Utils.getAddress(peerInfo.protocol, peerInfo.host, outPort)


        val newStream = NewStream(
          peerInfo = PBPeerInfo(thisAddr),
          protocolId = protocolId,
          inAddr = inAddr,
          outAddr = outAddr,
        )
        val cmd = Command(
          cmdType = CmdType.NEW_STREAM,
          data = Option(newStream.toByteString)
        )
        socket.send(cmd.toByteArray)

        val openedStream = OpenedStream.parseFrom(socket.recv())
        if (openedStream.ok) {
          val peerAddr = socket.getLastEndpoint
          println(s"${thisAddr} has opened stream with peer[addr=$peerAddr, protocolId=$protocolId]")

          val inSocket = zmqContext.createSocket(SocketType.ROUTER)
          inSocket.bind(inAddr) // to receive messages, connect on the other end

          val outSocket = zmqContext.createSocket(SocketType.DEALER)
          outSocket.connect(outAddr) // to send messages, bind on the other end
          val stream = new ZmqStream[F](thisAddr, inSocket, outSocket)
          openStreams += (protocolId -> stream)
          stream
        } else {
          peerInfo.ports.release(inPort)
          peerInfo.ports.release(outPort)
          throw new RuntimeException(openedStream.msg.getOrElse(""))
        }
      }


    }


  }

  override def close: F[Unit] = ct.unit // todo

  // todo refactor
  override def add(protocolId: String, stream: Stream[F]): Boolean = {
    if (openStreams.contains(protocolId)) {
      println(s"stream with $protocolId exists!!!")
      false
    } else {
      openStreams += (protocolId -> stream)
      true
    }
  }
}

object ZmqConnection {

  // todo
  object Status extends Enumeration {
    type Status = Value
    val Connecting, Open, Closed = Value
  }

}
