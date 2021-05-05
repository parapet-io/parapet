package io.parapet.cluster

import cats.effect.{Concurrent, IO}
import com.typesafe.scalalogging.Logger
import io.parapet.cluster.api.ClusterApi._
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.Start
import io.parapet.core.processes.RouletteLeaderElection.{encoder => _, _}
import io.parapet.core.{Channel, Process, ProcessRef}
import org.zeromq.ZMQ.Socket
import org.zeromq.{SocketType, ZContext}

import java.util

class ClusterProcess(leaderElection: ProcessRef)(implicit ctxShit : Concurrent[IO]) extends Process[IO] {

  import dsl._

  private val ch = new Channel[IO](ref)

  private val zmqCtx = new ZContext()
  private val clients = new util.HashMap[String, Socket]()
  private val logger = Logger[ClusterProcess]
  private val leEncoder = io.parapet.core.processes.RouletteLeaderElection.encoder // todo do we need this ?

  override def handle: Receive = {
    case Start => register(ref, ch)
    case Req(clientId, data) =>
      encoder.read(data) match {
        case join: Join =>
          ifLeader {
            eval(logger.debug("send broadcast to leader election process")) ++ broadcast(clientId, join)
          } {
            eval(logger.debug("process join and send response to the leader")) ++
              processJoin(join) ++
              Rep(clientId, encoder.write(JoinResult(join.nodeId, JoinResultCodes.OK))) ~> leaderElection
          }
        case joinRes: JoinResult => eval(logger.debug(s"cluster received $joinRes"))
      }
  }

  // sends join to all nodes in the cluster and waits for responses
  private def broadcast(clientId: String, join: Join): DslF[IO, Unit] = {
    // Note: clientId != join.nodeId when Join sent by a leader election process
    val data = encoder.write(join)
    blocking {
      ch.send(Broadcast(data), leaderElection, {
        case scala.util.Success(BroadcastResult(majorityCount)) =>
          // todo wait for ack from majority of nodes. use timeout
          eval(logger.debug(s"wait for $majorityCount to process $join")) ++
            processJoin(join) ++
            eval {
              getSocket(clientId, join.address)
                .send(encoder.write(Result(ResultCodes.OK, "node has been added to the group")))
            }
        case scala.util.Failure(err) =>
          eval(getSocket(clientId, join.address)
            .send(encoder.write(Result(ResultCodes.ERROR, Option(err.getMessage).getOrElse("")))))
      })
    }
  }

  private def ifLeader(isLeader: => DslF[IO, Unit])(isNotLeader: => DslF[IO, Unit]): DslF[IO, Unit] = {
    ch.send(IsLeader, leaderElection, {
      case scala.util.Success(IsLeaderRep(leader)) =>
        if (leader) eval(logger.debug("I'm a leader")) ++ isLeader
        else isNotLeader
    })
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
