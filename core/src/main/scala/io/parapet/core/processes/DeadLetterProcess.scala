package io.parapet.core.processes

import cats.effect.Concurrent
import com.typesafe.scalalogging.Logger
import io.parapet.{Envelope, ProcessRef}
import io.parapet.ProcessRef.DeadLetterRef
import io.parapet.core.Events.DeadLetter
import io.parapet.core.Process
import io.parapet.syntax.logger._
import org.slf4j.LoggerFactory

trait DeadLetterProcess[F[_]] extends Process[F] {
  override val name: String = DeadLetterRef.value
  override final val ref: ProcessRef = DeadLetterRef
}

object DeadLetterProcess {

  class DeadLetterLoggingProcess[F[_]] extends DeadLetterProcess[F] {
    import dsl._
    private val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))
    override val name: String = DeadLetterRef.value + "-logging"
    override val handle: Receive = { case DeadLetter(Envelope(sender, event, receiver), error) =>
      val mdcFields: MDCFields = Map(
        "processRef" -> ref,
        "processName" -> name,
        "sender" -> sender,
        "receiver" -> receiver,
        // "eventId" -> event.id, todo add WithId
        "event" -> event,
      )

      eval {
        logger.mdc(mdcFields) { _ =>
          logger.error(s"event cannot be processed", error)
        }
      }
    }

  }

  def logging[F[_]: Concurrent]: DeadLetterProcess[F] = new DeadLetterLoggingProcess()

}
