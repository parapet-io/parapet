package io.parapet.core

/** A causal trace of envelope ids spanning a chain of related events.
  *
  * Each envelope routed by the [[Scheduler]] carries an [[ExecutionTrace]] that grows by
  * one entry per hop. Useful for after-the-fact reconstruction of "which incoming event
  * caused which downstream effects" when [[Parapet.ParConfig.tracingEnabled]] is on.
  */
trait ExecutionTrace:
  /** Ordered list of envelope ids visited so far. Oldest first. */
  val values: List[String]

  /** The most recent envelope id. */
  val last: String

  /** Returns a new trace with `value` appended; the original trace is unchanged. */
  def add(value: String): ExecutionTrace

/** Constructors for [[ExecutionTrace]] and the no-op [[Dummy]]. */
object ExecutionTrace:
  /** Builds a trace seeded with `value`. */
  def apply(value: String): ExecutionTrace =
    Impl(List(value))

  /** Default linked-list-backed implementation. */
  final case class Impl(values: List[String]) extends ExecutionTrace:
    val last: String = values.last

    def add(value: String): ExecutionTrace =
      copy(values = values :+ value)

    override def toString: String =
      s"Trace(${values.mkString("->")})"

  /** Trace placeholder used when tracing is disabled. Records nothing and returns itself
    * from [[add]] for zero-allocation behavior on the hot path.
    */
  object Dummy extends ExecutionTrace:
    val values: List[String] = Nil
    val last: String = "disabled"

    def add(value: String): ExecutionTrace =
      this

    override def toString: String =
      "disabled"
