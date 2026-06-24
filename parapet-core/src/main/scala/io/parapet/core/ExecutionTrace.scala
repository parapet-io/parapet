package io.parapet.core

/** A causal pointer carried by every envelope routed by the [[Scheduler]]: the id of the envelope currently being
  * handled in a chain of related events.
  *
  * It is *just that id* — an opaque `Long`, so threading it costs nothing and there is no per-hop allocation. That id
  * is all the runtime needs to stamp the `cause` of the next emitted envelope; the full causal chain is reconstructable
  * from the record/replay journal's `(id, cause)` edges. `0L` is the "no cause"/root sentinel (also used when tracing
  * is disabled).
  */
opaque type ExecutionTrace = Long

object ExecutionTrace:

  /** No causal context — a root, or tracing disabled. */
  val Dummy: ExecutionTrace = 0L

  /** A context positioned at envelope id `current`. */
  def apply(current: Long): ExecutionTrace = current

  extension (trace: ExecutionTrace)
    /** Id of the envelope currently being handled; becomes the `cause` of the next emitted envelope. */
    def current: Long = trace

    /** A context advanced to envelope id `id`. */
    def next(id: Long): ExecutionTrace = id
