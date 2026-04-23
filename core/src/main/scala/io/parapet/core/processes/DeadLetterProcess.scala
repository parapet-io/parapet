package io.parapet.core.processes

import com.typesafe.scalalogging.Logger
import io.parapet.ProcessRef.DeadLetterRef
import io.parapet.core.Events.DeadLetter
import io.parapet.core.Process
import io.parapet.effect.Effect
import io.parapet.syntax.logger.*
import io.parapet.{Envelope, ProcessRef}
import org.slf4j.LoggerFactory

trait DeadLetterProcess[F[_]] extends Process[F]:
  override val name: String = DeadLetterRef.value
  override final val ref: ProcessRef = DeadLetterRef

object DeadLetterProcess:
  final class DeadLetterLoggingProcess[F[_]] extends DeadLetterProcess[F]:
    import dsl.*

    private val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))
    override val name: String = s"${DeadLetterRef.value}-logging"

    override val handle: Receive = {
      case DeadLetter(Envelope(sender, event, receiver), error) =>
        val mdcFields: MDCFields = Map(
          "processRef" -> ref,
          "processName" -> name,
          "sender" -> sender,
          "receiver" -> receiver,
          "event" -> event
        )

        eval {
          val canonicalEventName = Option(event).map(_.getClass).getOrElse("null")
          logger.mdc(mdcFields) { _ =>
            logger.error(s"event $canonicalEventName cannot be processed", error)
          }
        }
    }

  def logging[F[_]](using Effect[F]): DeadLetterProcess[F] =
    new DeadLetterLoggingProcess[F]
