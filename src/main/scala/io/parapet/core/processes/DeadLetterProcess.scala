package io.parapet.core.processes

import java.io.{PrintWriter, StringWriter}

import cats.effect.Concurrent
import com.typesafe.scalalogging.Logger
import io.parapet.core.Event.{DeadLetter, Failure}
import io.parapet.core.Logging.{MDCFields, _}
import io.parapet.core.ProcessRef.DeadLetterRef
import io.parapet.core.exceptions.{EventDeliveryException, EventRecoveryException}
import io.parapet.core.{Process, ProcessRef}
import org.slf4j.LoggerFactory

trait DeadLetterProcess[F[_]] extends Process[F] {
  override val name: String = DeadLetterRef.ref
  override final val ref: ProcessRef = DeadLetterRef
}

object DeadLetterProcess {

  class DeadLetterLoggingProcess[F[_]] extends DeadLetterProcess[F] {
    private val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))
    override val name: String = DeadLetterRef.ref + "-logging"
    override val handle: Receive = {
      case DeadLetter(Failure(pRef, event, error)) =>
        val errorMsg = error match {
          case e: EventDeliveryException => e.getCause.getMessage
          case e: EventRecoveryException => e.getCause.getMessage
        }

        val mdcFields: MDCFields = Map(
          "processId" -> ref,
          "processName" -> name,
          "failedProcessId" -> pRef,
          "eventId" -> event.id,
          "errorMsg" -> errorMsg,
          "stack_trace" -> getStackTrace(error))

        effectOps.eval {
          logger.mdc(mdcFields) { args =>
            logger.debug(s"$name: process[id=$pRef] failed to process event[id=${event.id}], error=${args("errorMsg")}")
          }
        }
    }


    private def getStackTrace(throwable: Throwable): String = {
      val sw = new StringWriter
      val pw = new PrintWriter(sw, true)
      throwable.printStackTrace(pw)
      sw.getBuffer.toString
    }
  }

  def logging[F[_] : Concurrent]: DeadLetterProcess[F] = new DeadLetterLoggingProcess()

}