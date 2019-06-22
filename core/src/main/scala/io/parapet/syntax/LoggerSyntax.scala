package io.parapet.syntax

import com.typesafe.scalalogging.Logger
import org.slf4j.MDC

trait LoggerSyntax {

  type MDCFields = Map[String, Any]

  implicit class LoggerOps(logger: Logger) {
    def mdc(fields: MDCFields)(log: MDCFields => Unit): Unit = {
      fields.foreach {
        case (key, value) => MDC.put(key, Option(value).fold("null")(_.toString))
      }
      log(fields)

      MDC.clear()
    }
  }

}
