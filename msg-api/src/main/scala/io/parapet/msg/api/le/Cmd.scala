package io.parapet.msg.api.le
import com.sksamuel.avro4s.{AvroDoc, AvroInputStream, AvroOutputStream, AvroSchema}
import io.parapet.msg.api.le.Cmd._
import org.apache.avro.Schema

import java.io.ByteArrayOutputStream

case class Cmd(tag: CmdTag, cmd: Api) {
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

  val Schema: Schema = AvroSchema[Cmd]

  sealed trait CmdTag
  case object ProposeTag extends CmdTag
  case object AckTag extends CmdTag
  case object AnnounceTag extends CmdTag
  case object HeartbeatTag extends CmdTag
  case object WhoTag extends CmdTag
  case object WhoRepTag extends CmdTag
  case object ReqTag extends CmdTag
  case object RepTag extends CmdTag
  case object BroadcastTag extends CmdTag
  case object BroadcastResultTag extends CmdTag

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

  sealed trait Api
  // internal api
  case class Propose(address: String, num: Double) extends Api
  case class Ack(address: String, num: Double, code: AckCode) extends Api
  @AvroDoc("coordinator sends announce to a node that will become a new leader")
  case class Announce(address: String) extends Api
  case class Heartbeat(address: String, leader: Option[String] = None) extends Api
  case class Who(clientId: String) extends Api
  case class WhoRep(address: String, leader: Boolean) extends Api
  case class Broadcast(data: Array[Byte]) extends Api
  case class BroadcastResult(majorityCount: Int) extends Api

  // client facing api
  case class Req(clientId: String, data: Array[Byte]) extends Api
  case class Rep(clientId: String, data: Array[Byte]) extends Api

  def apply(api: Api): Cmd =
    api match {
      case _: Propose => Cmd(ProposeTag, api)
      case _: Ack => Cmd(AckTag, api)
      case _: Announce => Cmd(AnnounceTag, api)
      case _: Heartbeat => Cmd(HeartbeatTag, api)
      case _: Who => Cmd(WhoTag, api)
      case _: WhoRep => Cmd(WhoRepTag, api)
      case _: Broadcast => Cmd(BroadcastTag, api)
      case _: BroadcastResult => Cmd(BroadcastResultTag, api)
      case _: Req => Cmd(ReqTag, api)
      case _: Rep => Cmd(RepTag, api)

    }

  def apply(bytes: Array[Byte]): Cmd = {
    val ais = AvroInputStream.binary[Cmd].from(bytes).build(Schema)
    ais.iterator.toSet.head
  }
}
