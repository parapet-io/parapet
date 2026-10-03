package io.parapet.effect

import io.parapet.effect.ParIORuntime.CancellationSignal

/** Execution state shared by the cancellation scopes of one physical fiber. */
final private[effect] class FiberContext:
  private var runner: Thread | Null = null
  private var cancellationMasks     = List.empty[CancellationMaskToken]

  def registerRunner(thread: Thread): Unit = synchronized {
    if runner != null then throw new IllegalStateException("fiber already has a runner")
    runner = thread
  }

  def clearRunner(thread: Thread): Unit = synchronized {
    if runner != thread then throw new IllegalStateException("fiber runner is not registered")
    runner = null
  }

  def maskDepth: Int = synchronized {
    cancellationMasks.size
  }

  def tryEnterUncancellable(signal: CancellationSignal): FiberContext.UncancellableEntry = synchronized {
    if cancellationDueLocked(signal) then FiberContext.UncancellableEntry.CancelNow
    else
      val mask = CancellationMaskToken.create()
      cancellationMasks = mask :: cancellationMasks
      FiberContext.UncancellableEntry.Entered(mask)
  }

  def cancellationDue(signal: CancellationSignal): Boolean = synchronized {
    cancellationDueLocked(signal)
  }

  def interruptRunnerIfCancellationDue(signal: CancellationSignal): Unit = synchronized {
    if cancellationDueLocked(signal) then interruptRunnerLocked()
  }

  def exitMask(mask: CancellationMaskToken): Unit = synchronized {
    cancellationMasks match
      case active :: remaining if active eq mask =>
        cancellationMasks = remaining
      case _ =>
        throw new IllegalStateException("cannot exit a cancellation mask that is not innermost")
  }

  def tryRestoreCancellation(mask: CancellationMaskToken): FiberContext.RestoreCancellationDecision = synchronized {
    cancellationMasks match
      case active :: remaining if active eq mask =>
        cancellationMasks = remaining
        FiberContext.RestoreCancellationDecision.Opened
      case _ => FiberContext.RestoreCancellationDecision.Ignored
  }

  def reinstateMask(mask: CancellationMaskToken): Unit = synchronized {
    reinstateMaskLocked(mask)
  }

  def tryReinstateMask(
      signal: CancellationSignal,
      mask: CancellationMaskToken
  ): FiberContext.ReinstateMaskDecision = synchronized {
    if cancellationDueLocked(signal) then FiberContext.ReinstateMaskDecision.CancelNow
    else
      reinstateMaskLocked(mask)
      FiberContext.ReinstateMaskDecision.Reinstated
  }

  private def cancellationDueLocked(signal: CancellationSignal): Boolean =
    signal.isCancellationDueAt(cancellationMasks.size)

  private def reinstateMaskLocked(mask: CancellationMaskToken): Unit =
    if cancellationMasks.exists(_ eq mask) then
      throw new IllegalStateException("cancellation mask is already active")

    cancellationMasks = mask :: cancellationMasks

  private def interruptRunnerLocked(): Unit =
    runner match
      case current: Thread if current != Thread.currentThread() => current.interrupt()
      case _                                                    => ()

private[effect] object FiberContext:
  enum UncancellableEntry:
    case CancelNow
    case Entered(mask: CancellationMaskToken)

  enum RestoreCancellationDecision:
    case Opened
    case Ignored

  enum ReinstateMaskDecision:
    case CancelNow
    case Reinstated
