package io.parapet.components.network

import io.parapet.components.network.ZmqTcpServer._
import io.parapet.core.Dsl.{Dsl, Effects, FlowOps}
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.implicits._
import org.zeromq.{SocketType, ZMQ}

class ZmqTcpServer[F[_]](port: Int, sink: ProcessRef)
                        (implicit FD: FlowOps[F, Dsl[F, ?]], ED: Effects[F, Dsl[F, ?]]) extends Process[F] {

  import FD._
  import ED._

  private lazy val zmqContext = ZMQ.context(1)
  private lazy val socket = zmqContext.socket(SocketType.REP)


  override val handle: Receive = {
    case Start => eval(println("server...")) ++
      eval(socket.bind(s"tcp://*:$port")) ++ eval(println("server has started")) ++ Await ~> self
    case Await => eval(println("server waiting for messages")) ++
      use(socket.recv(0))(data => eval(println("server received message")) ++ Req(data) ~> sink)
    case Rep(data) => eval(socket.send(data, 0)) ++ Await ~> self
    case Stop =>
      eval {
        socket.close()
        zmqContext.close()
      }
  }
}

object ZmqTcpServer {

  def apply[F[_]](port: Int, sink: ProcessRef)(implicit FD: FlowOps[F, Dsl[F, ?]], ED: Effects[F, Dsl[F, ?]]): ZmqTcpServer[F] = new ZmqTcpServer(port, sink)

  case class Req(data: Array[Byte]) extends Event

  case class Rep(data: Array[Byte]) extends Event

  private object Await extends Event

}
