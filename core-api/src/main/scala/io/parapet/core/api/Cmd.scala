package io.parapet.core.api

import com.sksamuel.avro4s.{AvroDoc, AvroInputStream, AvroOutputStream, AvroSchema}
import io.parapet.{Event, ProcessRef}
import org.apache.avro.Schema

import java.io.ByteArrayOutputStream

sealed trait Cmd extends Event {

  def toByteArray: Array[Byte] = {
    val os = new ByteArrayOutputStream()
    val aos = AvroOutputStream.binary[Cmd].to(os).build()
    aos.write(this)
    aos.flush()
    aos.close()
    os.toByteArray
  }

}

object Cmd {
  private val schema: Schema = AvroSchema[Cmd]

  object netClient {
    sealed trait Api extends Cmd
    case class Send(data: Array[Byte], reply: Option[ProcessRef] = None) extends Api
    case class Rep(data: Option[Array[Byte]]) extends Api
  }

  object netServer {
    sealed trait Api extends Cmd
    case class Send(clientId: String, data: Array[Byte]) extends Api
    case class Message(clientId: String, data: Array[Byte]) extends Api
  }

  object coordinator {

    sealed trait Api extends Cmd
    case class Propose(id: String, num: Double) extends Api
    case object Start extends Api
    case class Elected(id: String) extends Api

    sealed trait AckCode

    object AckCode {
      @AvroDoc("success")
      case object Ok extends AckCode

      @AvroDoc("some process has been already selected as coordinator")
      case object Elected extends AckCode

      @AvroDoc("the process has already voted")
      case object Voted extends AckCode

      @AvroDoc("the current process generated a higher number than proposer")
      case object High extends AckCode
    }

    case class Ack(id: String, num: Double, code: AckCode) extends Api
  }

  object leaderElection {
    sealed trait Api extends Cmd
    @AvroDoc("two types of ack codes: success and failure")
    sealed trait AckCode

    object AckCode {
      @AvroDoc("success")
      case object Ok extends AckCode
      @AvroDoc("failure: the current process is coordinator")
      case object Coordinator extends AckCode
      @AvroDoc("failure: indicates that this process has already voted in the current round")
      case object Voted extends AckCode
      @AvroDoc("failure: leader has been already elected")
      case object Elected extends AckCode
      @AvroDoc("failure: the current process has a higher number than sender")
      case object High extends AckCode
    }

    // internal api
    case class Propose(address: String, num: Double) extends Api
    case class Ack(address: String, num: Double, code: AckCode) extends Api
    @AvroDoc("coordinator sends announce to a node that will become a new leader")
    case class Announce(address: String) extends Api
    case class Heartbeat(address: String, leader: Option[String] = None) extends Api

    // client facing api
    case class Who(clientId: String) extends Api
    case class WhoRep(address: String, leader: Boolean) extends Api
    case class LeaderUpdate(id: String, address: String) extends Api
    case class Req(clientId: String, data: Array[Byte]) extends Api
    case class Rep(clientId: String, data: Array[Byte]) extends Api
  }

  object cluster {
    sealed trait Api extends Cmd
    sealed trait Code

    object Code {
      case object Ok extends Code
      case object NotFound extends Code
      case object Error extends Code
      case object Joined extends Code
      case object StateUpdate extends Code
    }

    case class Join(nodeId: String, address: String, group: String) extends Api
    case class JoinResult(nodeId: String, code: Code) extends Api
    case class GetNodeInfo(senderId: String, id: String) extends Api
    case class NodeInfo(id: String, address: String, code: Code) extends Api
    case class Ack(msg: String, code: Code) extends Api

    case class Node(id: String, protocol: String, address: String, groups: Set[String]) extends Api
    case class State(version: Long, nodes: List[Node]) extends Api
    case object GetState extends Api

  }

  object clusterNode {
    sealed trait Api extends Cmd
    case class Req(nodeId: String, data: Array[Byte]) extends Api
  }

  def apply(bytes: Array[Byte]): Cmd = {
    val ais = AvroInputStream.binary[Cmd].from(bytes).build(schema)
    ais.iterator.toSet.head
  }

}
