package io.parapet.core.processes

import com.typesafe.scalalogging.Logger
import io.parapet.ProcessRef.DeadLetterRef
import io.parapet.core.Events.DeadLetter
import io.parapet.core.Process
import io.parapet.effect.Effect
import io.parapet.syntax.logger.*
import io.parapet.{Envelope, ProcessRef}
import org.slf4j.LoggerFactory

/** Marker trait for the singleton process that consumes [[io.parapet.core.Events.DeadLetter]] messages.
  *
  * Subclasses are pinned to [[io.parapet.ProcessRef.DeadLetterRef]] so the runtime can route undeliverable envelopes
  * without registry lookups.
  *
  * Override [[io.parapet.ParApp.deadLetter]] to substitute a custom implementation (metrics, alerting, persistence,
  * etc.).
  */
trait DeadLetterProcess[F[_]] extends Process[F, DeadLetter, Nothing]:
  override val name: String                      = DeadLetterRef.value
  final override val ref: ProcessRef[DeadLetter] = DeadLetterRef

/** Built-in [[DeadLetterProcess]] implementations. */
object DeadLetterProcess:
  /** Default dead-letter handler: logs each undeliverable envelope at `error`, with sender/receiver/event details
    * surfaced as SLF4J MDC fields for structured log search.
    */
  final class DeadLetterLoggingProcess[F[_]] extends DeadLetterProcess[F]:
    import dsl.*

    private val logger        = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))
    override val name: String = s"${DeadLetterRef.value}-logging"

    override val handle: Receive = { case DeadLetter(Envelope(sender, event, receiver, _), error) =>
      val mdcFields: MDCFields = Map(
        "processRef"  -> ref,
        "processName" -> name,
        "sender"      -> sender,
        "receiver"    -> receiver,
        "event"       -> event
      )

      eval {
        val canonicalEventName = Option(event).map(_.getClass).getOrElse("null")
        logger.mdc(mdcFields) { _ =>
          logger.error(s"event $canonicalEventName cannot be processed", error)
        }
      }
    }

  /** Returns the default logging implementation. */
  def logging[F[_]](using Effect[F]): DeadLetterProcess[F] =
    new DeadLetterLoggingProcess[F]
