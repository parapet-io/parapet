package io.parapet.core

import cats.data.NonEmptyList

trait ExecutionTrace {

  val values: List[String]

  /** Gets the most recently added value in this trace or `"disabled"` if [[ExecutionTrace.Dummy]] implementation is used.
    * @return the most recently in this trace or `"disabled"` if [[ExecutionTrace.Dummy]] implementation is used.
    */
  val last: String

  /** Creates a new trace and appends the given value.
    * @param value value to append to this trace
    * @return new trace
    */
  def add(value: String): ExecutionTrace

}

object ExecutionTrace {

  def apply(x: String): ExecutionTrace = new Impl(NonEmptyList(x, List.empty))

  class Impl(_values: NonEmptyList[String]) extends ExecutionTrace {

    override val values: List[String] = _values.toList

    override def add(value: String): ExecutionTrace = new Impl(_values :+ value)

    override val last: String = values.last

    override def toString: String = s"Trace(${values.mkString("->")})"
  }

  object Dummy extends ExecutionTrace { self =>
    override val values: List[String] = List.empty

    override def add(value: String): ExecutionTrace = self

    override val last: String = "disabled"

    override def toString: String = "disabled"
  }

}
