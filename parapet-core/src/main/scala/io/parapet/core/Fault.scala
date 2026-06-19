package io.parapet.core

import scala.concurrent.duration.FiniteDuration

/** The decision a [[FaultPolicy]] returns for a single intercepted operation. */
enum Fault:
  /** Run the operation normally - the no-fault outcome. */
  case Proceed

  /** Sleep for `duration`, then run the operation. Models latency / timeouts. */
  case Delay(duration: FiniteDuration)

  /** Skip the operation entirely. Meaningful only for outbound, `Unit`-returning ops (`Send`, `Forward`, `Delay`); for
    * value-producing ops [[FaultInjector]] falls back to [[Proceed]] since there is no value to synthesize. Use it to
    * simulate lost messages.
    */
  case Drop

  /** Abort the operation by raising `error` instead of running it. The error surfaces to the handler exactly as a real
    * one would, so it can be caught with `handleError`. Models a runtime failure.
    */
  case Fail(error: Throwable)
