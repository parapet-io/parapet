package io.parapet.core

trait ExecutionTrace:
  val values: List[String]
  val last: String
  def add(value: String): ExecutionTrace

object ExecutionTrace:
  def apply(value: String): ExecutionTrace =
    Impl(List(value))

  final case class Impl(values: List[String]) extends ExecutionTrace:
    val last: String = values.last

    def add(value: String): ExecutionTrace =
      copy(values = values :+ value)

    override def toString: String =
      s"Trace(${values.mkString("->")})"

  object Dummy extends ExecutionTrace:
    val values: List[String] = Nil
    val last: String = "disabled"

    def add(value: String): ExecutionTrace =
      this

    override def toString: String =
      "disabled"
