package io.parapet.components.network

import io.parapet.components.network.ZmqTcpClient._
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.{Encoder, Process}
import io.parapet.implicits._
import org.zeromq.{SocketType, ZMQ}

import scala.language.higherKinds

class ZmqTcpClient[F[_]](host: String, port: Int,
                         encoder: Encoder) extends Process[F] {

  import effectDsl._
  import flowDsl._

  private lazy val zmqContext = ZMQ.context(1)
  private lazy val socket = zmqContext.socket(SocketType.REQ)

  override val handle: Receive = {
    case Start => eval(socket.connect(s"tcp://$host:$port"))
    case Stop => eval {
      socket.close()
      zmqContext.close()
    }
    case e =>
      eval {
        val data = appendZeroByte(encoder.write(e))
        socket.send(data, 0)
      } ++
        reply(sender => use(socket.recv(0))(response => encoder.read(response) ~> sender))

  }
}


object ZmqTcpClient {

  def apply[F[_]](host: String, port: Int, encoder: Encoder): ZmqTcpClient[F] =
    new ZmqTcpClient(host, port, encoder)

//  case class Req(data: Array[Byte]) extends Event
//
//  case class Rep(data: Array[Byte]) extends Event

  private def appendZeroByte(data: Array[Byte]): Array[Byte] = {
    val tmp = new Array[Byte](data.length + 1)
    data.copyToArray(tmp)
    tmp(tmp.length - 1) = 0
    tmp
  }

}