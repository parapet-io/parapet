package io.parapet.components.network

import io.parapet.core.{Event, Process}
import io.parapet.implicits._
import org.zeromq.{SocketType, ZMQ}

import scala.language.higherKinds
import io.parapet.components.network.ZmqTcpClient._
import io.parapet.core.Dsl.{Dsl, Effects, FlowOps}
import io.parapet.core.Event.{Start, Stop}

class ZmqTcpClient[F[_]](host: String, port: Int)(implicit FD: FlowOps[F, Dsl[F, ?]], ED: Effects[F, Dsl[F, ?]]) extends Process[F] {

  import FD._
  import ED._

  private lazy val zmqContext = ZMQ.context(1)
  private lazy val socket = zmqContext.socket(SocketType.REQ)

  override val handle: Receive = {
    case Start => eval(socket.connect(s"tcp://$host:$port"))
    case Req(buf) =>
      eval(socket.send(buf, 0)) ++
        reply(sender => use(socket.recv(0))(response => Rep(response) ~> sender))
    case Stop => eval {
      socket.close()
      zmqContext.close()
    }
  }
}


object ZmqTcpClient {

  def apply[F[_]](host: String, port: Int)(implicit FD: FlowOps[F, Dsl[F, ?]], ED: Effects[F, Dsl[F, ?]]): ZmqTcpClient[F] = new ZmqTcpClient(host, port)

  case class Req(data: Array[Byte]) extends Event

  case class Rep(data: Array[Byte]) extends Event


}