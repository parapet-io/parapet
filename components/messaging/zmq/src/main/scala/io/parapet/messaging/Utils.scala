package io.parapet.messaging

import io.parapet.messaging.api.ErrorCodes
import io.parapet.messaging.api.MessagingApi.{Failure, Response}
import io.parapet.core.Dsl.{Dsl, DslF, FlowOps}
import io.parapet.core.Encoder.EncodingException
import io.parapet.core.Peer.PeerInfo
import io.parapet.core.ProcessRef
import org.zeromq.{ZContext, ZMQException}

import scala.concurrent.duration._
import scala.util.Try

object Utils {

  def getAddress(peerInfo: PeerInfo): String =
    getAddress(peerInfo.protocol, peerInfo.host, peerInfo.port)

  def getAddress(protocol: String, host: String, port: Int): String = s"$protocol://$host:$port"

  def appendZeroByte(data: Array[Byte]): Array[Byte] = {
    val tmp = new Array[Byte](data.length + 1)
    data.copyToArray(tmp)
    tmp(tmp.length - 1) = 0
    tmp
  }

  def close[F[_]](zmqContext: ZContext):
  DslF[F, Unit] = {
    val dsl = implicitly[FlowOps[F, Dsl[F, ?]]]
    import dsl._
    // sometimes 'zmqContext.close()' waits forever
    // even though neither this process nor a child closes a socket explicitly
    // todo: this issues has to be investigated further
    race(eval(Try(zmqContext.close())), delay(10.seconds))
  }

  def tryEval[F[_], A](t: => Try[A],
                       onSuccess: A => DslF[F, Unit],
                       onFailure: Throwable => DslF[F, Unit]): DslF[F, Unit] = {
    t match {
      case scala.util.Success(a) => onSuccess(a)
      case scala.util.Failure(err) => onFailure(err)
    }
  }

  def failure(error: Throwable): Response = {
    error match {
      case e: ZMQException => Failure(e.getMessage, ErrorCodes.TransferError)
      case e: EncodingException => Failure(e.getMessage, ErrorCodes.EncodingError)
      case e => Failure(e.getMessage, ErrorCodes.UnknownError)
    }
  }

  def sendFailure[F[_]](ref: ProcessRef, error: Throwable): DslF[F, Unit] = {
    val dsl = implicitly[FlowOps[F, Dsl[F, ?]]]
    dsl.send(failure(error), ref)
  }
}