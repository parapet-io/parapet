package io.parapet.core.api

import com.sksamuel.avro4s.{AvroDoc, AvroInputStream, AvroOutputStream, AvroSchema}
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
  object leaderElection {
    sealed trait Api extends Cmd
    @AvroDoc("two types of ack codes: success and failure")
    sealed trait AckCode

    object AckCode {
      @AvroDoc("success")
      case object Ok extends AckCode
      @AvroDoc("failure: the current process is  coordinator")
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
    //  case class Broadcast(data: Array[Byte]) extends Api
    //  case class BroadcastResult(majorityCount: Int) extends Api

    // client facing api
    case class Who(clientId: String) extends Api
    case class WhoRep(address: String, leader: Boolean) extends Api
    case class LeaderUpdate(address: String) extends Api
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
    }

    case class Join(nodeId: String, address: String, group: String) extends Api
    case class JoinResult(nodeId: String, code: Code) extends Api
    case class GetNodeInfo(senderId: String, id: String) extends Api
    case class NodeInfo(id: String, address: String, code: Code) extends Api
    case class Ack(msg: String, code: Code) extends Api
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