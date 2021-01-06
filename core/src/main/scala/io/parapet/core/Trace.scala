package io.parapet.core

import cats.{Eval, Monad}
import io.parapet.core.Trace._

/**
  * Helper class to store an execution trace in stack safe, lazy fashion
  *
  * @param delimiter delimiter used to concatenate strings
  * @param trace     thunk that produces a message
  */
case class Trace(delimiter: String, devMode: Boolean, private var trace: MsgThunk[String], private val _id: String) {

  def id: String = _id

  private lazy val _value: String = {
    val tmp = trace.value
    trace = Monad[MsgThunk].pure("") // to fix OOM
    tmp
  }

  // pure
  def append(f: => Any): Trace = {
    Trace(delimiter, devMode, Monad[MsgThunk].flatMap(trace)(s => {
      Eval.later(s + delimiter + f)
    }).memoize, _id)
  }

  /**
    * Evaluates the message thunk and memorizes the result of computation.
    *
    * @return trace message
    */
  def value: String = {
    if (devMode) _value
    else "Tracing is disabled"
  }

  override def toString: String = value
}

object Trace {

  type MsgThunk[+A] = Eval[A]

  val None: Trace = Trace(devMode = false)

  def apply(devMode: Boolean): Trace = {
    Trace("\n", devMode)
  }

  def apply(delimiter: String, devMode: Boolean): Trace = {
    val id = System.nanoTime().toString
    Trace(delimiter, devMode, Eval.later(header(id)), id)
  }

  def header(id: String): String = {
    val builder = new StringBuilder()
    builder.append("\n").append("=" * 50).append("\n")
    builder.append("TRACE ").append(id).append("\n")
    builder.append("=" * 50).append("\n")
    builder.toString()

  }

}
