package io.parapet.effect

/** Cancellation and runner state for one effect fiber. */
final private[effect] class FiberContext:
  private var runner: Thread | Null = null
  private var cancellationRequested = false
  private var cancellationMasks     = List.empty[CancellationMaskToken]

  def registerRunner(thread: Thread): Unit = synchronized {
    if runner != null then throw new IllegalStateException("fiber already has a runner")
    runner = thread
  }

  def clearRunner(thread: Thread): Unit = synchronized {
    if runner != thread then throw new IllegalStateException("fiber runner is not registered")
    runner = null
  }

  def requestCancellation(): Boolean = synchronized {
    if cancellationRequested then false
    else
      cancellationRequested = true
      if cancellationDueLocked then interruptRunnerLocked()
      true
  }

  def tryEnterUncancellable(): FiberContext.UncancellableEntry = synchronized {
    if cancellationDueLocked then FiberContext.UncancellableEntry.CancelNow
    else
      val mask = CancellationMaskToken.create()
      cancellationMasks = mask :: cancellationMasks
      FiberContext.UncancellableEntry.Entered(mask)
  }

  def cancellationDue: Boolean = synchronized {
    cancellationDueLocked
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
        FiberContext.RestoreCancellationDecision.Restored
      case _ => FiberContext.RestoreCancellationDecision.Unchanged
  }

  def reinstateMask(mask: CancellationMaskToken): Unit = synchronized {
    reinstateMaskLocked(mask)
  }

  def tryReinstateMask(mask: CancellationMaskToken): FiberContext.ReinstateMaskDecision = synchronized {
    if cancellationDueLocked then FiberContext.ReinstateMaskDecision.CancelNow
    else
      reinstateMaskLocked(mask)
      FiberContext.ReinstateMaskDecision.Reinstated
  }

  private def cancellationDueLocked: Boolean =
    cancellationRequested && cancellationMasks.isEmpty

  private def reinstateMaskLocked(mask: CancellationMaskToken): Unit =
    if cancellationMasks.exists(_ eq mask) then throw new IllegalStateException("cancellation mask is already active")

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
    case Restored
    case Unchanged

  enum ReinstateMaskDecision:
    case CancelNow
    case Reinstated
