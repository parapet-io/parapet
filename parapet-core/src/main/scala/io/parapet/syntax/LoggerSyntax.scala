package io.parapet.syntax

import com.typesafe.scalalogging.Logger
import org.slf4j.MDC

/** Adds an `mdc` extension to [[Logger]] for scoped MDC-enriched log calls.
  *
  * {{{
  * logger.mdc(Map("processRef" -> ref)) { _ =>
  *   logger.info("started")
  * }
  * }}}
  *
  * Fields are pushed into SLF4J [[MDC]] before `log` runs and the MDC is cleared afterwards.
  */
trait LoggerSyntax:
  /** Convenience alias for the MDC payload type. */
  type MDCFields = Map[String, Any]

  extension (logger: Logger)
    /** Runs `log` with `fields` populated in the MDC. Each value is `null`-safely stringified. The MDC is cleared after
      * `log` returns.
      */
    def mdc(fields: MDCFields)(log: MDCFields => Unit): Unit =
      fields.foreach { case (key, value) =>
        MDC.put(key, Option(value).fold("null")(_.toString))
      }
      log(fields)
      MDC.clear()
