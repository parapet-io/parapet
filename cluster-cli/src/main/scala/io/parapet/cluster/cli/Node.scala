package io.parapet.cluster.cli
import io.parapet.core.processes.RouletteLeaderElection.WHO_TAG
import io.parapet.core.processes.RouletteLeaderElection.REQ_TAG
import org.zeromq.{SocketType, ZContext, ZMQ, ZMsg}

import java.nio.ByteBuffer
import java.nio.channels.Selector
import scala.annotation.tailrec
import scala.util.Try
import io.parapet.cluster.api.ClusterApi._

class Node(host: String, port: Int, id: String, server: Array[String]) extends Interface {

  private val zmqCtx = new ZContext()
  private val addr = s"$host:$port"

  // this node server
  private val netServer = zmqCtx.createSocket(SocketType.ROUTER)
  netServer.bind(s"tcp://*:$port")

  // creates connections to all servers in the cluster
  private val clusterNodes =
    server.map { addr =>
      val socket = zmqCtx.createSocket(SocketType.DEALER)
      socket.setIdentity(id.getBytes())
      socket.connect(s"tcp://$addr")
      (addr, socket)
    }.toMap

  private var _leader: Option[String] = Option.empty

  override def connect(): Unit = {
    _leader = Option(getLeader)
    println(s"node[id: $id] joined the cluster. leader: ${_leader}")
  }

  override def join(group: String): Try[Unit] =
    Try {
      _leader match {
        case Some(leaderAddr) =>
          val join = Join(nodeId = id, address = addr, group = group)
          val data = encoder.write(join)
          val msg = ByteBuffer.allocate(4 + data.length)
          msg.putInt(REQ_TAG)
          msg.put(data)
          msg.rewind()
          clusterNodes(leaderAddr).send(msg.array())

          // receive a response
          val responseMsg = ZMsg.recvMsg(netServer)
          responseMsg.popString() // identity
          val resp = responseMsg.pop().getData
          encoder.read(resp) match {
            case res: Result =>
              println(s"client received a response from leader: $res")
          }
        case None => throw new RuntimeException("leader is not defined")
      }
    }

  override def leave(group: String): Try[Unit] = ???

  override def whisper(node: String, data: Array[Byte]): Try[Unit] = ???

  override def broadcast(group: String, data: Array[Byte]): Try[Unit] = ???

  override def leader: Option[String] = _leader

  override def getNodes: Seq[String] = ???

  // attempts to acquire a leader until it's available
  private def getLeader: String = {

    val pollItems = clusterNodes.values.map(socket => new ZMQ.PollItem(socket, ZMQ.Poller.POLLIN)).toArray

    val msg = new Array[Byte](4)
    val buf = ByteBuffer.allocate(msg.length)
    buf.putInt(WHO_TAG)
    buf.rewind()
    buf.get(msg)

    @tailrec
    def step(attempts: Int): String = {
      clusterNodes.values.foreach(socket => socket.send(msg, ZMQ.NOBLOCK))
      val selector = Selector.open()
      ZMQ.poll(selector, pollItems, 5000L)
      selector.close()
      val events = pollItems.filter(_.isReadable())
      if (events.length > 0) {
        events.map(item => item.getSocket.recvStr()).find(_.nonEmpty) match {
          case Some(leader) => leader
          case None =>
            println(s"no leader available. attempts made: $attempts")
            Thread.sleep(5000L)
            step(attempts + 1)
        }
      } else {
        println(s"no nodes responded within a timeout. attempts made: $attempts")
        step(attempts + 1)
      }
    }
    step(1)

  }

  override def close(): Unit = {
    clusterNodes.values.foreach(socket => socket.close())
    zmqCtx.close()
  }
}
