package io.parapet.components.network

import io.parapet.components.network.ZmqTcpServer._
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.{Encoder, Event, Process, ProcessRef}
import io.parapet.implicits._
import org.zeromq.{SocketType, ZMQ}

class ZmqTcpServer[F[_]](port: Int, sink: ProcessRef, encoder: Encoder) extends Process[F] {

  import effectDsl._
  import flowDsl._

  private lazy val zmqContext = ZMQ.context(1)
  private lazy val socket = zmqContext.socket(SocketType.REP)


  override val handle: Receive = {
    case Start => eval(println("server...")) ++
      eval(socket.bind(s"tcp://*:$port")) ++ eval(println("server has started")) ++ Await ~> selfRef
    case Stop =>
      eval {
        socket.close()
        zmqContext.close()
      }
    case Await => eval(println("server waiting for messages")) ++
      use(socket.recv(0)) { data =>
        val event = encoder.read(data)
        eval(println(s"tcp-server received event: " + event)) ++ event ~> sink
      }
    case e => eval {
      val data = appendZeroByte(encoder.write(e))
      socket.send(data, 0)
    } ++ Await ~> selfRef

  }
}

object ZmqTcpServer {

  def apply[F[_]](port: Int, sink: ProcessRef, encoder: Encoder): ZmqTcpServer[F] =
    new ZmqTcpServer(port, sink, encoder)

  private object Await extends Event

  private def appendZeroByte(data: Array[Byte]): Array[Byte] = {
    val tmp = new Array[Byte](data.length + 1)
    data.copyToArray(tmp)
    tmp(tmp.length - 1) = 0
    tmp
  }

}
