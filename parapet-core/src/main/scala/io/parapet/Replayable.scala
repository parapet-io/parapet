package io.parapet

/** Opts a process into recording and replay of its business deliveries and supported effect outcomes.
  *
  * Delivery recovery requires an [[EventCodec]] for the process's complete input protocol. When effect journaling is
  * enabled, each journal-aware operation must provide a [[EventCodec]] for its recorded outcome. An operation without
  * the required codec fails before execution.
  */
trait Replayable
