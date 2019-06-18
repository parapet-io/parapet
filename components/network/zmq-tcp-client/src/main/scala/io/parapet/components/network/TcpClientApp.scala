package io.parapet.components.network

import cats.effect.IO
import io.parapet.components.network.ZmqTcpClient
import io.parapet.components.network.ZmqTcpClient._
import io.parapet.CatsApp
import io.parapet.core.Event.Start
import io.parapet.core.{Process, ProcessRef}
import io.parapet.instances.DslInstances.catsInstances.effect._
import io.parapet.instances.DslInstances.catsInstances.flow._
import io.parapet.implicits._

object TcpClientApp extends CatsApp {
  val port = 5555

  class MessageSender(tcpClient: ProcessRef) extends Process[IO] {
    override val handle: Receive = {
      case Start => createRequest(5).map(_ ~> zmqTcpClient).foldLeft(empty)(_ ++ _)
      case Rep(data) => eval(println("Received response: " + new String(data)))
    }

    def createRequest(n: Int): Seq[Req] = {
      (1 to n).map { i =>
        val data = s"Message[$i] ".getBytes()
        data(data.length - 1) = 0 //Sets the last byte to 0
        Req(data)
      }
    }
  }

  val zmqTcpClient = ZmqTcpClient[IO]("localhost", port)
  val messageSender = new MessageSender(zmqTcpClient.self)
  override val processes: Array[Process[IO]] = Array(messageSender, zmqTcpClient)

}
