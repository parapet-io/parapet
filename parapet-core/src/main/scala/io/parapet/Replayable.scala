package io.parapet

/** Opts a process into recording and replay of its business deliveries.
  *
  * When recovery is enabled, the process must provide an [[EventCodec]] for its complete input protocol.
  */
trait Replayable
