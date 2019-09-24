package io.parapet.messaging

import java.net.ServerSocket

import cats.effect.Concurrent
import io.parapet.core.Stream
import io.parapet.protobuf.protocol.{CmdType, Command, NewStream}
import org.zeromq.ZMQ.Socket
import org.zeromq.{SocketType, ZContext}


class ZmqStream[F[_] : Concurrent](addr:String, inSocket: Socket, outSocket: Socket) extends Stream[F] {

  private val ct: Concurrent[F] = implicitly[Concurrent[F]]

  override def write(data: Array[Byte]): F[Unit] = ct.delay {
    println(s"peer[$addr] writes data to ${outSocket.getLastEndpoint}")
    outSocket.send(data, 0)
  }

  override def read: F[Array[Byte]] = ct.delay {
    println(s"peer[$addr] reads data from ${inSocket.getLastEndpoint}")
    inSocket.recv()
  }
}


