package io.parapet.cluster

import cats.effect.{Concurrent, IO}
import io.parapet.cluster.api.ClusterApi._
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.Start
import io.parapet.core.{Channel, Process, ProcessRef}
import io.parapet.core.processes.RouletteLeaderElection.{Broadcast, Rep, Req}
import org.zeromq.{SocketType, ZContext}
import org.zeromq.ZMQ.Socket
import zmq.ZMQ

import java.util

class ClusterProcess(leaderElection: ProcessRef)(implicit ctxShit : Concurrent[IO]) extends Process[IO] {

  import dsl._

  private val ch = new Channel[IO](ref)

  private val zmqCtx = new ZContext()
  private val clients = new util.HashMap[String, Socket]()

  override def handle: Receive = {
    case Start => register(ref, ch)
    case Req(clientId, data) =>
      encoder.read(data) match {
        case join: Join =>
          blocking {
            ch.send(Broadcast(data), leaderElection, {
              case scala.util.Success(_) =>
                processJoin(join) ++
                  eval {
                    getSocket(clientId, join.address).send(encoder.write(Result(ResultCodes.OK, "node has been added to the group")))
                  }
              case scala.util.Failure(err) => Rep(clientId, encoder.write(Result(ResultCodes.ERROR, Option(err.getMessage).getOrElse("")))) ~> leaderElection
            })
          }
      }
  }

  private def getSocket(clientId: String, addr: String): Socket = {
    clients.computeIfAbsent(clientId, _ => {
      try {
        val socket = zmqCtx.createSocket(SocketType.DEALER)
        socket.connect("tcp://" + addr)
        socket
      } catch {
        case e: Exception => e.printStackTrace()
          throw e
      }

    })
    clients.get(clientId)
  }

  private def processJoin(join: Join): DslF[IO, Unit] = {
    eval(println(s"client[id: ${join.nodeId}, addr: ${join.address}] joined group: ${join.group}"))
  }

}

object ClusterProcess {}
