package io.parapet.net.transport.zmq

import com.google.protobuf.CodedInputStream
import com.google.protobuf.ByteString
import io.parapet.net.transport.{Message, TransportError}
import io.parapet.net.transport.wire.{Header, Message as WireMessage}

import scala.util.control.NonFatal

private[zmq] object ZmqWireFraming:
  def encode(message: Message): Array[Byte] =
    val header = Header(correlationId = message.correlationId)
    WireMessage(header = Some(header), payload = ByteString.copyFrom(message.payload)).toByteArray

  def decode(frame: Array[Byte], operation: String): Either[TransportError, Message] =
    parseMessage(frame, operation).flatMap { wireMessage =>
      wireMessage.header.map(_.correlationId).filter(_.nonEmpty) match
        case Some(correlationId) =>
          Right(Message(correlationId, wireMessage.payload.toByteArray))
        case None =>
          Left(TransportError.ProtocolViolation(s"ZMQ $operation message is missing correlation id"))
    }

  private def parseMessage(frame: Array[Byte], operation: String): Either[TransportError, WireMessage] =
    try Right(WireMessage.parseFrom(CodedInputStream.newInstance(frame)))
    catch
      case NonFatal(error) =>
        Left(TransportError.ProtocolViolation(s"invalid ZMQ $operation wire header: ${error.getMessage}"))
