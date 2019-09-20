package io.parapet.messaging

import java.net.ServerSocket

import cats.effect.Concurrent
import io.parapet.core.Stream
import io.parapet.protobuf.protocol.{CmdType, Command, NewStream}
import org.zeromq.ZMQ.Socket
import org.zeromq.{SocketType, ZContext}

class ZmqStream[F[_] : Concurrent](pub: Socket, recvSocket: Socket) extends Stream[F] {


  private val ct: Concurrent[F] = implicitly[Concurrent[F]]


  override def write(data: Array[Byte]): F[Unit] = ct.delay(println("write data"))

  override def read: F[Array[Byte]] = ct.delay {
    println("read data")
    Array.empty
  }
}

// PUB 5555 <-> SUB
// SUB <-> PUB 6666

//

object ZmqStream {

  def apply[F[_] : Concurrent](protocolId0: String,
                               ctx: ZContext,
                               socket: Socket): F[Stream[F]] = {
    val ct: Concurrent[F] = implicitly[Concurrent[F]]
    ct.delay {
      // we need two sockets PUB/ROUTER
      // write data to PUB
      // recv from ROUTER
      val pub = ctx.createSocket(SocketType.PUB)
      // idea: use a single pub socket per peer and use selector
      val pubPort = new ServerSocket(0).getLocalPort
      pub.bind(s"tcp://*:$pubPort")
      val recvSocket = ctx.createSocket(SocketType.ROUTER)
      val recvSocketPort = new ServerSocket(0).getLocalPort
      recvSocket.bind(s"tcp://*:$recvSocketPort")
      // todo send PUB socket addr
      // todo recv peer's PUB socket addr
      val newStream = NewStream(protocolId = protocolId0)
      val cmd = Command(cmdType = CmdType.NEW_STREAM,
        data = Option(newStream.toByteString)).toByteArray
      socket.send(cmd, 0)
      new ZmqStream[F](pub, recvSocket)
    }
  }
}


