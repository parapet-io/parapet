package io.parapet.effect

import io.parapet.effect.ParIORuntime.CancellationSignal
import scala.collection.mutable.ListBuffer

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

  def interruptRunner(): Unit = synchronized {
    if cancellationMasks.isEmpty then interruptRunnerLocked()
  }

  def tryEnterUncancellable(signal: CancellationSignal): FiberContext.UncancellableEntry = synchronized {
    if signal.isRequested then FiberContext.UncancellableEntry.CancelNow
    else
      val mask = CancellationMaskToken.create()
      cancellationMasks = mask :: cancellationMasks
      FiberContext.UncancellableEntry.Entered(mask)
  }

  def exitMask(mask: CancellationMaskToken): Unit = synchronized {
    if cancellationMasks.isEmpty || !cancellationMasks.head.eq(mask) then
      throw IllegalArgumentException("invalid exist mask ")
    cancellationMasks = cancellationMasks.tail
  }

  def tryRestoreCancellation(mask: CancellationMaskToken): FiberContext.RestoreCancellationDecision = synchronized {
    cancellationMasks match {
      case active :: remaining if active eq mask =>
        cancellationMasks = remaining
        FiberContext.RestoreCancellationDecision.Opened
      case _ => FiberContext.RestoreCancellationDecision.Ignored
    }
  }

  def reinstateMask(mask: CancellationMaskToken): Unit = synchronized {
    if cancellationMasks.contains(mask) then throw new IllegalStateException("cancellation mask is already active")

    cancellationMasks = mask :: cancellationMasks
  }

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
