package io.parapet.core

import cats.{Eval, Monad}
import io.parapet.core.Trace._

/**
  * Helper class to store an execution trace in stack safe, lazy fashion
  *
  * @param delimiter delimiter used to concatenate strings
  * @param trace     thunk that produces a message
  */
case class Trace(delimiter: String, private val trace: MsgThunk[String], private val _id: String) {

  def id: String = _id

  // pure
  def append(f: => Any): Trace = {
    Trace(delimiter, Monad[MsgThunk].flatMap(trace)(s => {
      Eval.later(s + delimiter + f)
    }).memoize, _id)
  }


  /**
    * Evaluates the message thunk and memorizes the result of computation.
    *
    * @return trace message
    */
  def value: String = {
    if (DEBUG_MODE) trace.value
    else "Tracing is disabled"
  }

  override def toString: String = value
}

object Trace {
  private val DEBUG_MODE = false
  type MsgThunk[+A] = Eval[A]

  val Empty: Trace = Trace("\n")

  def apply(): Trace = apply("\n")

  def apply(delimiter: String): Trace = {
    val id = System.nanoTime().toString
    Trace(delimiter, Eval.later(header(id)), id)
  }

  def header(id: String): String = {
    val builder = new StringBuilder()
    builder.append("\n").append("=" * 50).append("\n")
    builder.append("TRACE ").append(id).append("\n")
    builder.append("=" * 50).append("\n")
    builder.toString()

  }

}
