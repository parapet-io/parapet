package io.parapet.core.processes

import cats.effect.Concurrent
import com.typesafe.scalalogging.Logger
import io.parapet.core.Event.{DeadLetter, Envelope}
import io.parapet.syntax.logger._
import io.parapet.core.ProcessRef.DeadLetterRef
import io.parapet.core.{Process, ProcessRef}
import org.slf4j.LoggerFactory

trait DeadLetterProcess[F[_]] extends Process[F] {
  override val name: String = DeadLetterRef.ref
  override final val selfRef: ProcessRef = DeadLetterRef
}

object DeadLetterProcess {

  class DeadLetterLoggingProcess[F[_]] extends DeadLetterProcess[F] {
    import effectDsl._
    private val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))
    override val name: String = DeadLetterRef.ref + "-logging"
    override val handle: Receive = {
      case DeadLetter(Envelope(sender, event, receiver), error) =>
        val mdcFields: MDCFields = Map(
          "processRef" -> selfRef,
          "processName" -> name,
          "sender" -> sender,
          "receiver" -> receiver,
         // "eventId" -> event.id, todo add WithId
          "event" ->  event)

        eval {
          logger.mdc(mdcFields) { _ =>
            logger.error(s"event cannot be processed", error)
          }
        }
    }

  }

  def logging[F[_] : Concurrent]: DeadLetterProcess[F] = new DeadLetterLoggingProcess()

}