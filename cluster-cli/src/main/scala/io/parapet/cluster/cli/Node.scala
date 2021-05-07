package io.parapet.cluster.cli
import com.typesafe.scalalogging.Logger
import io.parapet.cluster.api.ClusterApi._
import io.parapet.core.processes.RouletteLeaderElection
import io.parapet.core.processes.RouletteLeaderElection.{REQ_TAG, WHO_REP_TAG, WHO_TAG, WhoRep}
import org.zeromq.ZMQ.Socket
import org.zeromq.{SocketType, ZContext, ZFrame, ZMQ, ZMsg}
import zmq.ZError

import java.nio.ByteBuffer
import java.nio.channels.Selector
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Try
import org.zeromq.ZMQ.Poller

import scala.util.control.Breaks.{break, breakable}

// TODO leader heartbeat
class Node(id: String, host: String, port: Int, servers: Array[String]) extends Interface {

  private val logger = Logger[Node]
  private val zmqCtx = new ZContext()
  private val addr = s"$host:$port"
  private var _msgHandler: MessageHandler = _

  // this node server

  // connections to all servers in the cluster
  private val _servers = mutable.Map.empty[String, Socket]
  private val _nodes = new ConcurrentHashMap[String, Socket]()

  private var _leader: Option[String] = Option.empty
  private val server = new Server(port)

  def msgHandler(handler: MessageHandler): Unit = _msgHandler = handler

  override def connect(): Unit = {
    _servers ++= servers.map { address =>
      val socket = zmqCtx.createSocket(SocketType.DEALER)
      socket.setIdentity(id.getBytes())
      socket.connect(s"tcp://$address")
      (address, socket)
    }.toMap
    _leader = Option(getLeader)
    logger.info(s"node[id: $id] connected to the cluster. leader: ${_leader}")
    server.start()
  }

  override def join(group: String): Try[Unit] =
    Try {
      _leader match {
        case Some(leaderAddress) =>
          val join = Join(nodeId = id, address = addr, group = group)
          val data = encoder.write(join)
          val msg = ByteBuffer.allocate(4 + data.length)
          msg.putInt(REQ_TAG)
          msg.put(data)
          msg.rewind()
          _servers(leaderAddress).send(msg.array())

          breakable {
            while (true) {
              // receive a response
              val responseMsg = ZMsg.recvMsg(_servers(leaderAddress))
              // responseMsg.popString() // identity
              val buf = ByteBuffer.wrap(responseMsg.pop().getData)
              val code = buf.getInt(0)
              if (code == WHO_REP_TAG) {
                logger.debug("ignore late WHO messages")
              } else {
                encoder.read(buf.array()) match {
                  case res: Result => logger.debug(s"client received a response from leader: $res")
                    break
                }
              }
            }
          }

        case None => throw new RuntimeException("no leader")
      }
    }

  override def leave(group: String): Try[Unit] =
    Try(throw new UnsupportedOperationException("leave is not supported yet"))


  override def send(req: Req): Try[Unit] = {
    Try {
      val socket = _nodes.computeIfAbsent(req.nodeId, _ => {
        logger.debug(s"node[id=${req.nodeId}] is not registered. requesting node info")
        val leader = _leader.getOrElse(throw new IllegalStateException("no leader"))
        val getNodeInfo = GetNodeInfo(id, req.nodeId)
        val data = encoder.write(getNodeInfo)
        val buf = ByteBuffer.allocate(4 + data.length)
        buf.putInt(REQ_TAG)
        buf.put(data)
        _servers(leader).send(buf.array())
        val responseMsg = ZMsg.recvMsg(_servers(leader))
        encoder.read(responseMsg.pop().getData) match {
          case ni@NodeInfo(address, code) =>
            logger.debug(s"received node info. node info=$ni")
            // todo check code
            val socket = zmqCtx.createSocket(SocketType.DEALER)
            socket.setIdentity(id.getBytes())
            socket.connect(s"tcp://$address")
            logger.debug(s"connection with ${ni.address} has been established")
            socket
        }
      })
      socket.send(req.data)
      logger.debug(s"req ${new String(req.data)} to ${req.nodeId} has been sent")
    }
  }

  override def send(rep: Rep): Try[Unit] = server.send(rep).map(_ => ())

  override def broadcast(group: String, data: Array[Byte]): Try[Unit] =
    Try(throw new UnsupportedOperationException("broadcast is not supported yet"))

  override def leader: Option[String] = _leader

  override def getNodes: Try[Seq[String]] =
    Try(throw new UnsupportedOperationException("getNodes is not supported yet"))

  override def close(): Unit =
    if (!zmqCtx.isClosed) {
      _servers.values.foreach(socket =>
        try socket.close()
        catch {
          case err: Exception => logger.error("failed to close the socket", err)
        },
      )

      try zmqCtx.close()
      catch {
        case err: Exception => logger.error("error occurred while shutting down ZMQ context", err)
      }
    }

  // attempts to acquire a leader until it's available
  private def getLeader: String = {

    val pollItems = _servers.values.map(socket => new ZMQ.PollItem(socket, ZMQ.Poller.POLLIN)).toArray

    val msg = new Array[Byte](4)
    val buf = ByteBuffer.allocate(msg.length)
    buf.putInt(WHO_TAG)
    buf.rewind()
    buf.get(msg)

    @tailrec
    def step(attempts: Int): String = {
      _servers.values.foreach(socket => socket.send(msg, ZMQ.NOBLOCK))
      val selector = Selector.open()
      ZMQ.poll(selector, pollItems, 5000L)
      selector.close()
      val events = pollItems.filter(_.isReadable())
      if (events.length > 0) {
        events.map(item => {
          val data = item.getSocket.recv()
          val buf = ByteBuffer.allocate(4 + data.length)
          buf.putInt(0)
          buf.put(data)
          val rep = RouletteLeaderElection.encoder.read(buf.array()).asInstanceOf[WhoRep]
          logger.debug(s"node ${rep.address} is leader: ${rep.leader}")
          rep
        }).find(_.leader) match {
          case Some(leader) => leader.address
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

  class Server(port: Int) {
    private val logger = Logger[Server]
    private val zmqCtx = new ZContext()
    private val appControl = new ThreadLocal[Socket]() {
      override def initialValue(): Socket = {
        val socket = zmqCtx.createSocket(SocketType.PAIR)
        socket.bind(CONTROL_ADDRESS)
        socket
      }
    }

    // Socket to talk to application
    private val threadPool = Executors.newSingleThreadExecutor()
    private val workersPool = Executors.newFixedThreadPool(4) // todo configurable

    private val CONTROL_ADDRESS = "inproc://control"
    private val CONTROL_POLLIN = 0
    private val ROUTER_POLLIN = 1

    // commands
    private val CONTROL_SEND = 0
    private val CONTROL_SEND_BYTES = Array(0x0.toByte, 0x0.toByte, 0x0.toByte, 0x0.toByte)
    private val CONTROL_TERM = 1
    private val CONTROL_TERM_BYTES = Array(0x0.toByte, 0x0.toByte, 0x0.toByte, 0x1.toByte)

    def start(): Unit = {
      threadPool.submit(new Loop())
    }

    def send(rep: Rep): Try[Boolean] =
      Try {
        val msg = new ZMsg
        msg.add(CONTROL_SEND_BYTES)
        msg.addString(rep.nodeId)
        msg.add(rep.data)
        msg.send(appControl.get())
      }

    def stop(): Unit =
      try {
        val msg = new ZMsg
        msg.add(CONTROL_TERM_BYTES)
        msg.send(appControl.get())
        threadPool.shutdown() // what for the loop task to complete
        threadPool.awaitTermination(5, TimeUnit.MINUTES)
        zmqCtx.close()
        workersPool.shutdownNow()
      } catch {
        case err: Exception => logger.error("error occurred while stopping the server", err)
      }

    class Loop extends Runnable {
      private val control = zmqCtx.createSocket(SocketType.PAIR)
      private val server = zmqCtx.createSocket(SocketType.ROUTER)
      private val poller = zmqCtx.createPoller(2)

      private def init(): Unit = {
        server.bind(s"tcp://*:$port")
        control.connect(CONTROL_ADDRESS)
        poller.register(control, Poller.POLLIN)
        poller.register(server, Poller.POLLIN)
      }

      override def run(): Unit = {
        init()
        breakable {
          while (!Thread.currentThread().isInterrupted && !zmqCtx.isClosed)
            try {
              poller.poll() // waiting for messages
              if (poller.pollin(CONTROL_POLLIN)) {
                val msg = ZMsg.recvMsg(control)
                val cmd = ByteBuffer.wrap(msg.pop().getData).getInt
                cmd match {
                  case CONTROL_SEND =>
                    logger.debug(s"send a message to ?")
                    msg.send(server)
                  case CONTROL_TERM => break
                  case unknown => logger.warn(s"unknown control message=$unknown")
                }
              }
              if (poller.pollin(ROUTER_POLLIN)) {
                val reqMsg = ZMsg.recvMsg(server)
                val clientId = reqMsg.popString() // identity
                val data = reqMsg.pop().getData
                workersPool.submit(new Runnable {
                  override def run(): Unit = _msgHandler.handle(Req(clientId, data))
                })
              }

            } catch {
              case err: org.zeromq.ZMQException if err.getErrorCode == ZError.ETERM =>
                logger.info("zmq context has been terminated")
              case err: Exception => logger.error("error occurred while processing message", err)
            }
        }
      }


    }

  }

}
