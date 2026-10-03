package io.parapet.effect

import io.parapet.runtime.SchedulerRuntime

import java.util.concurrent.{
  Callable,
  CancellationException,
  CompletableFuture,
  ExecutionException,
  ExecutorCompletionService,
  ExecutorService,
  Executors,
  Future,
  ScheduledExecutorService,
  ScheduledFuture,
  SynchronousQueue,
  ThreadFactory,
  ThreadPoolExecutor,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

/** Bounded pool configuration.
  */
final case class FixedPoolConfig(size: Int, threadNamePrefix: String):
  require(size > 0, s"pool size must be positive, got $size")
  require(threadNamePrefix.nonEmpty, "threadNamePrefix must be non-empty")

/** Elastic pool configuration.
  */
final case class ElasticPoolConfig(
    coreSize: Int,
    maxSize: Int,
    keepAlive: FiniteDuration,
    threadNamePrefix: String
):
  require(coreSize >= 0, s"coreSize must be non-negative, got $coreSize")
  require(maxSize > 0, s"maxSize must be positive, got $maxSize")
  require(maxSize >= coreSize, s"maxSize ($maxSize) must be >= coreSize ($coreSize)")
  require(keepAlive.toNanos >= 0L, s"keepAlive must be non-negative, got $keepAlive")
  require(threadNamePrefix.nonEmpty, "threadNamePrefix must be non-empty")

final case class TimerThreadPoolConfig(threads: Int, threadNamePrefix: String):
  require(threads > 0, s"threads must be positive, got $threads")
  require(threadNamePrefix.nonEmpty, "threadNamePrefix must be non-empty")

final case class ParIORuntimeConfig(
    scheduler: ElasticPoolConfig,
    parallel: FixedPoolConfig,
    async: FixedPoolConfig,
    blocking: ElasticPoolConfig,
    race: ElasticPoolConfig,
    timer: TimerThreadPoolConfig
)

object ParIORuntimeConfig:
  private val DefaultParallelism = math.max(2, Runtime.getRuntime.availableProcessors())

  /** Default runtime config. */
  val default: ParIORuntimeConfig =
    ParIORuntimeConfig(
      scheduler = ElasticPoolConfig(
        coreSize = DefaultParallelism,
        maxSize = Int.MaxValue,
        keepAlive = 60.seconds,
        threadNamePrefix = "parapet-scheduler"
      ),
      parallel = FixedPoolConfig(DefaultParallelism, "parapet-parallel"),
      async = FixedPoolConfig(DefaultParallelism, "parapet-async"),
      blocking = ElasticPoolConfig(
        coreSize = 0,
        maxSize = Int.MaxValue,
        keepAlive = 60.seconds,
        threadNamePrefix = "parapet-blocking"
      ),
      race = ElasticPoolConfig(
        coreSize = 0,
        maxSize = Int.MaxValue,
        keepAlive = 60.seconds,
        threadNamePrefix = "parapet-async-race"
      ),
      timer = TimerThreadPoolConfig(1, "parapet-timer")
    )

/** Helpers that construct the executors described by [[ParIORuntimeConfig]]. */
private[parapet] object Pools:

  /** Backed by a `ThreadPoolExecutor` with a `SynchronousQueue` and `allowCoreThreadTimeOut(true)`: submissions never
    * queue, threads spawn on demand up to `maxSize`, and idle threads (including core threads) terminate after
    * `keepAlive`. This shape is appropriate for any pool whose tasks may themselves block (sleeps, joins, nested
    * races).
    */
  def elastic(cfg: ElasticPoolConfig): ThreadPoolExecutor =
    val executor = new ThreadPoolExecutor(
      cfg.coreSize,
      cfg.maxSize,
      cfg.keepAlive.toNanos,
      TimeUnit.NANOSECONDS,
      new SynchronousQueue[Runnable](),
      namedThreadFactory(cfg.threadNamePrefix),
      new ThreadPoolExecutor.AbortPolicy()
    )
    executor.allowCoreThreadTimeOut(true)
    executor.prestartAllCoreThreads()
    executor

  /** Fixed-size executor backed by `Executors.newFixedThreadPool`.
    */
  def fixed(cfg: FixedPoolConfig): ExecutorService =
    Executors.newFixedThreadPool(cfg.size, namedThreadFactory(cfg.threadNamePrefix))

  /** Scheduled executor used for timer wake-ups. */
  def scheduled(cfg: TimerThreadPoolConfig): ScheduledExecutorService =
    Executors.newScheduledThreadPool(cfg.threads, namedThreadFactory(cfg.threadNamePrefix))

  private def namedThreadFactory(prefix: String): ThreadFactory =
    new ThreadFactory:
      private val index = new AtomicInteger(0)

      override def newThread(runnable: Runnable): Thread =
        val thread = new Thread(runnable)
        thread.setName(s"$prefix-${index.incrementAndGet()}")
        thread.setDaemon(true)
        thread

/** Runtime/interpreter for [[ParIO]].
  */
final class ParIORuntime(val config: ParIORuntimeConfig) extends AutoCloseable:
  import ParIO.*

  private enum RuntimeContext:
    case External, Scheduler, Parallel, Async, Blocking

  sealed private trait Frame
  final private case class BindFrame(run: Any => ParIO[Any])                   extends Frame
  final private case class RecoverFrame(run: Throwable => ParIO[Any])          extends Frame
  final private case class CancelFrame(run: ParIO[Any])                        extends Frame
  final private case class GuaranteeFrame(run: ParIO[Any])                     extends Frame
  final private case class ExitUncancellableFrame(mask: CancellationMaskToken) extends Frame
  final private case class ReinstateMaskFrame(mask: CancellationMaskToken)     extends Frame

  final private class FiberCancellationException extends CancellationException("fiber canceled")

  // `future` controls executor scheduling and carries the task's result or failure. `terminated` is an
  // outcome-independent barrier: it completes after a started task has fully unwound, or after cancellation proves
  // that the task will never start. Cancellation waits on this barrier so it cannot return while task finalizers are
  // still running.
  final private case class RunningTask[A](
      future: Future[A],
      fiberContext: FiberContext,
      startState: AtomicInteger,
      terminated: CompletableFuture[Unit]
  )

  private object TaskStartState:
    val Pending             = 0
    val Started             = 1
    val CanceledBeforeStart = 2

  private val runtimeContextLocal = new ThreadLocal[RuntimeContext]()

  // Pool selection rule of thumb.
  //   - `Pools.elastic` for work that may itself block (sleep, join, nested race, blocking I/O): threads grow on
  //     demand via a `SynchronousQueue`, so a submission never queues behind a parked task. Trade-off: thread count
  //     is unbounded under sustained burst.
  //   - `Pools.fixed` for short, CPU-bound, non-blocking work: hard cap on parallelism, excess submissions queue in
  //     an unbounded mailbox. Do not put blocking work here or queued tasks can starve.
  //   - `Pools.scheduled` is for timer wake-ups only.
  private val schedulerPool = Pools.elastic(config.scheduler)
  private val parallelPool  = Pools.fixed(config.parallel)
  private val asyncPool     = Pools.fixed(config.async)
  private val blockingPool  = Pools.elastic(config.blocking)
  private val racePool      = Pools.elastic(config.race)
  private val timer         = Pools.scheduled(config.timer)

  /** [[Effect]] instance backed by this runtime */
  given effect: Effect[ParIO] with
    def pure[A](value: A): ParIO[A] =
      ParIO.pure(value)

    extension [A](fa: ParIO[A])
      def flatMap[B](f: A => ParIO[B]): ParIO[B] =
        fa.flatMap(f)

      override def map[B](f: A => B): ParIO[B] =
        fa.map(f)

      def handleErrorWith(f: Throwable => ParIO[A]): ParIO[A] =
        fa.handleErrorWith(f)

    def delay[A](thunk: => A): ParIO[A] =
      ParIO.delay(thunk)

    def blocking[A](thunk: => A): ParIO[A] =
      ParIO.blocking(thunk)

    def suspend[A](thunk: => ParIO[A]): ParIO[A] =
      ParIO.suspend(thunk)

    def raiseError[A](error: Throwable): ParIO[A] =
      ParIO.raiseError(error)

    def canceled: ParIO[Unit] =
      ParIO.canceled

    def sleep(duration: FiniteDuration): ParIO[Unit] =
      ParIO.sleep(duration)

    def start[A](fa: ParIO[A]): ParIO[EffectFiber[ParIO, A]] =
      ParIO.delay(startFiberOn(asyncPool, RuntimeContext.Async, fa))

    def startBlocking[A](fa: ParIO[A]): ParIO[EffectFiber[ParIO, A]] =
      ParIO.delay(startFiberOn(blockingPool, RuntimeContext.Blocking, fa))

    def race[A, B](left: ParIO[A], right: ParIO[B]): ParIO[Either[A, B]] =
      ParIO.Race(left, right)

    def guarantee[A](fa: ParIO[A])(finalizer: ParIO[Unit]): ParIO[A] =
      ParIO.Guarantee(fa, finalizer)

    def onCancel[A](fa: ParIO[A])(finalizer: ParIO[Unit]): ParIO[A] =
      ParIO.OnCancel(fa, finalizer)

    def uncancellable[A](body: Poll[ParIO] => ParIO[A]): ParIO[A] =
      ParIO.Uncancellable(body)

  /** [[Parallel]] instance backed by this runtime */
  given parallel: Parallel[ParIO] with
    def par(effects: Seq[ParIO[Unit]]): ParIO[Unit] =
      ParIO.delay(runParallel(effects))

  private[parapet] given schedulerRuntime: SchedulerRuntime[ParIO] with
    def runSchedulerWorkers(workers: Seq[ParIO[Unit]]): ParIO[Unit] =
      ParIO.delay(runSchedulerWorkersOnPool(workers))

  /** Interpret `fa` synchronously on the current thread.
    */
  private[parapet] def unsafeRun[A](fa: ParIO[A]): A =
    Option(runtimeContextLocal.get()) match
      case Some(_) => unsafeRunLoop(fa, new FiberContext())
      case None    => withRuntimeContext(RuntimeContext.External)(unsafeRunLoop(fa, new FiberContext()))

  /** Stops the runtime's executors. */
  def shutdown(): Unit =
    timer.shutdownNow()
    racePool.shutdownNow()
    blockingPool.shutdownNow()
    asyncPool.shutdownNow()
    parallelPool.shutdownNow()
    schedulerPool.shutdownNow()

  override def close(): Unit =
    shutdown()

  private def unsafeRunLoop[A](io: ParIO[A], fiberContext: FiberContext): A =
    var current: ParIO[Any] = io.asInstanceOf[ParIO[Any]]
    var stack: List[Frame]  = Nil

    while true do
      if fiberContext.cancellationDue then throw runCancellationFinalizers(stack, fiberContext)

      try
        current match
          case Pure(value) =>
            stack match
              case Nil =>
                return value.asInstanceOf[A]
              case BindFrame(run) :: tail =>
                current = run(value)
                stack = tail
              case RecoverFrame(_) :: tail =>
                current = Pure(value)
                stack = tail
              case CancelFrame(_) :: tail =>
                current = Pure(value)
                stack = tail
              case GuaranteeFrame(finalizer) :: tail =>
                stack = tail
                unsafeRunLoop(finalizer, new FiberContext())
                current = Pure(value)
              case ReinstateMaskFrame(mask) :: tail =>
                fiberContext.tryReinstateMask(mask) match
                  case FiberContext.ReinstateMaskDecision.CancelNow =>
                    throw new FiberCancellationException
                  case FiberContext.ReinstateMaskDecision.Reinstated =>
                    stack = tail
              case ExitUncancellableFrame(mask) :: tail =>
                stack = tail
                fiberContext.exitMask(mask)

          case Delay(thunk) =>
            current = Pure(thunk())

          case Blocking(thunk) =>
            current = Pure(runBlocking(thunk()))

          case Suspend(thunk) =>
            current = thunk().asInstanceOf[ParIO[Any]]

          case Race(left, right) =>
            racePrograms(left, right) match
              case Outcome.Succeeded(value) => current = Pure(value)
              case Outcome.Failed(error)    => throw error
              case Outcome.Canceled()       => current = ParIO.Canceled

          case FlatMap(source, bind) =>
            current = source.asInstanceOf[ParIO[Any]]
            stack = BindFrame(bind.asInstanceOf[Any => ParIO[Any]]) :: stack

          case HandleError(source, handler) =>
            current = source.asInstanceOf[ParIO[Any]]
            stack = RecoverFrame(handler.asInstanceOf[Throwable => ParIO[Any]]) :: stack

          case Uncancellable(body) =>
            fiberContext.tryEnterUncancellable() match
              case FiberContext.UncancellableEntry.CancelNow =>
                throw new FiberCancellationException
              case FiberContext.UncancellableEntry.Entered(mask) =>
                val poll = new Poll[ParIO]:
                  override def apply[B](fa: ParIO[B]): ParIO[B] =
                    RestoreCancellation(fa, mask)

                stack = ExitUncancellableFrame(mask) :: stack
                current = body(poll).asInstanceOf[ParIO[Any]]

          case RestoreCancellation(fa, mask) =>
            fiberContext.tryRestoreCancellation(mask) match
              case FiberContext.RestoreCancellationDecision.Restored =>
                stack = ReinstateMaskFrame(mask) :: stack
              case FiberContext.RestoreCancellationDecision.Unchanged => ()

            current = fa.asInstanceOf[ParIO[Any]]

          case Canceled =>
            fiberContext.requestCancellation()
            current = Pure(())

          case Sleep(duration) =>
            sleepOnTimer(duration)
            current = Pure(())

          case OnCancel(source, finalizer) =>
            current = source.asInstanceOf[ParIO[Any]]
            stack = CancelFrame(finalizer.asInstanceOf[ParIO[Any]]) :: stack

          case Guarantee(source, finalizer) =>
            current = source.asInstanceOf[ParIO[Any]]
            stack = GuaranteeFrame(finalizer.asInstanceOf[ParIO[Any]]) :: stack
      catch
        case _: Throwable if fiberContext.cancellationDue =>
          throw runCancellationFinalizers(stack, fiberContext)
        case error: Throwable =>
          var frames  = stack
          var handled = false
          while !handled && frames.nonEmpty do
            frames match
              case RecoverFrame(run) :: tail =>
                current = Suspend(() => run(error))
                stack = tail
                handled = true
              case GuaranteeFrame(finalizer) :: tail =>
                val wasInterrupted = Thread.interrupted()
                try unsafeRunLoop(finalizer, new FiberContext())
                catch case finalizerError: Throwable => error.addSuppressed(finalizerError)
                finally if wasInterrupted then Thread.currentThread().interrupt()
                frames = tail
              case ReinstateMaskFrame(mask) :: tail =>
                fiberContext.reinstateMask(mask)
                frames = tail
              case ExitUncancellableFrame(mask) :: tail =>
                fiberContext.exitMask(mask)
                frames = tail
              case _ :: tail =>
                frames = tail
              case Nil =>
                ()

          if !handled then throw error

    throw new IllegalStateException("unreachable")

  private def runCancellationFinalizers(
      stack: List[Frame],
      fiberContext: FiberContext
  ): CancellationException =
    val cancellation = new FiberCancellationException
    var frames       = stack

    Thread.interrupted()

    while frames.nonEmpty do
      frames match
        case CancelFrame(finalizer) :: tail =>
          Thread.interrupted()
          try unsafeRunLoop(finalizer, new FiberContext())
          catch case error: Throwable => cancellation.addSuppressed(error)
          finally Thread.interrupted()
          frames = tail
        case GuaranteeFrame(finalizer) :: tail =>
          Thread.interrupted()
          try unsafeRunLoop(finalizer, new FiberContext())
          catch case error: Throwable => cancellation.addSuppressed(error)
          finally Thread.interrupted()
          frames = tail
        case ReinstateMaskFrame(mask) :: tail =>
          fiberContext.reinstateMask(mask)
          frames = tail
        case ExitUncancellableFrame(mask) :: tail =>
          fiberContext.exitMask(mask)
          frames = tail
        case _ :: tail =>
          frames = tail
        case Nil =>
          ()

    cancellation

  private def runCancelable[A](fiberContext: FiberContext)(thunk: => A): A =
    val runner = Thread.currentThread()
    fiberContext.registerRunner(runner)
    try thunk
    finally fiberContext.clearRunner(runner)

  private def startFiberOn[A](
      pool: ExecutorService,
      runtimeContext: RuntimeContext,
      fa: ParIO[A]
  ): EffectFiber[ParIO, A] =
    // `result` is completed by the runner after `unsafeRunLoop` terminates, so `join` observes the terminal outcome
    // and `cancel` waits for cancellation finalizers to finish.
    val result       = new CompletableFuture[Outcome[A]]()
    val startState   = new AtomicInteger(TaskStartState.Pending)
    val fiberContext = new FiberContext()
    val task         = pool.submit(new Callable[Unit]:
      override def call(): Unit =
        if startState.compareAndSet(TaskStartState.Pending, TaskStartState.Started) then
          withRuntimeContext(runtimeContext) {
            try
              result.complete(
                Outcome.Succeeded(runCancelable(fiberContext)(unsafeRunLoop(fa, fiberContext)))
              )
            catch
              case _: FiberCancellationException =>
                result.complete(Outcome.Canceled())
              case error: Throwable =>
                result.complete(Outcome.Failed(error))
          })

    new EffectFiber[ParIO, A]:
      def join: ParIO[Outcome[A]] =
        ParIO.blocking(await(result))

      def cancel: ParIO[Unit] =
        ParIO.blocking {
          fiberContext.requestCancellation()

          if startState.compareAndSet(TaskStartState.Pending, TaskStartState.CanceledBeforeStart) then
            task.cancel(false)
            result.complete(Outcome.Canceled())

          try result.get()
          catch
            case _: CancellationException    => ()
            case _: ExecutionException       => ()
            case error: InterruptedException =>
              Thread.currentThread().interrupt()
              throw error
          ()
        }

  private def racePrograms[A, B](left: ParIO[A], right: ParIO[B]): Outcome[Either[A, B]] =
    final case class RaceResult(tag: Int, outcome: Outcome[Any])

    def runBranch[X](tag: Int, program: ParIO[X], fiberContext: FiberContext): RaceResult =
      val outcome: Outcome[X] =
        try Outcome.Succeeded(runCancelable(fiberContext)(unsafeRunLoop(program, fiberContext)))
        catch
          case _: FiberCancellationException => Outcome.Canceled()
          case error: Throwable              => Outcome.Failed(error)
      RaceResult(tag, outcome)

    def toRaceOutcome(result: RaceResult): Outcome[Either[A, B]] =
      result.outcome match
        case Outcome.Succeeded(value) =>
          if result.tag == 0 then Outcome.Succeeded(Left(value.asInstanceOf[A]))
          else Outcome.Succeeded(Right(value.asInstanceOf[B]))
        case Outcome.Failed(error) => Outcome.Failed(error)
        case Outcome.Canceled()    => Outcome.Canceled()

    val completion = new ExecutorCompletionService[RaceResult](racePool)

    def submitBranch[X](tag: Int, program: ParIO[X]): RunningTask[RaceResult] =
      val fiberContext = new FiberContext()
      val startState   = new AtomicInteger(TaskStartState.Pending)
      val terminated   = new CompletableFuture[Unit]()
      val future       = completion.submit(new Callable[RaceResult]:
        override def call(): RaceResult =
          if startState.compareAndSet(TaskStartState.Pending, TaskStartState.Started) then
            try withRuntimeContext(RuntimeContext.Async)(runBranch(tag, program, fiberContext))
            finally terminated.complete(())
          else
            terminated.complete(())
            throw new CancellationException("task canceled before start"))
      RunningTask(future, fiberContext, startState, terminated)

    val leftTask  = submitBranch(0, left)
    val rightTask = submitBranch(1, right)

    try
      val first = completion.take().get()
      first.outcome match
        case Outcome.Canceled() =>
          toRaceOutcome(completion.take().get())
        case _ =>
          if first.tag == 0 then cancelAndAwait(rightTask)
          else cancelAndAwait(leftTask)
          toRaceOutcome(first)
    catch
      case error: ExecutionException =>
        cancelAndAwait(leftTask)
        cancelAndAwait(rightTask)
        throw Option(error.getCause).getOrElse(error)
      case error: CancellationException =>
        cancelAndAwait(leftTask)
        cancelAndAwait(rightTask)
        throw error
      case error: InterruptedException =>
        cancelAndAwait(leftTask)
        cancelAndAwait(rightTask)
        Thread.currentThread().interrupt()
        throw error

  private def runParallel(effects: Seq[ParIO[Unit]]): Unit =
    runAllOnPool(effects, parallelPool, RuntimeContext.Parallel)

  private def runSchedulerWorkersOnPool(workers: Seq[ParIO[Unit]]): Unit =
    runAllOnPool(workers, schedulerPool, RuntimeContext.Scheduler)

  private def runAllOnPool(
      effects: Seq[ParIO[Unit]],
      pool: ExecutorService,
      runtimeContext: RuntimeContext
  ): Unit =
    if effects.nonEmpty then
      val completion  = new ExecutorCompletionService[Unit](pool)
      val running     = scala.collection.mutable.ArrayBuffer.empty[RunningTask[Unit]]
      var completed   = false
      var interrupted = false

      try
        effects.foreach { effect =>
          val fiberContext = new FiberContext()
          val startState   = new AtomicInteger(TaskStartState.Pending)
          val terminated   = new CompletableFuture[Unit]()
          val future       = completion.submit(
            new Callable[Unit]:
              override def call(): Unit =
                if startState.compareAndSet(TaskStartState.Pending, TaskStartState.Started) then
                  try
                    withRuntimeContext(runtimeContext) {
                      runCancelable(fiberContext)(unsafeRunLoop(effect, fiberContext))
                    }
                  finally terminated.complete(())
                else terminated.complete(())
          )
          running += RunningTask(future, fiberContext, startState, terminated)
        }

        var remaining = running.size
        while remaining > 0 do
          completion.take().get()
          remaining -= 1

        completed = true
      catch
        case error: ExecutionException =>
          throw Option(error.getCause).getOrElse(error)
        case error: InterruptedException =>
          interrupted = true
          throw error
      finally
        if !completed then running.foreach(task => cancelAndAwait(task))
        if interrupted then Thread.currentThread().interrupt()

  private def cancelAndAwait[A](task: RunningTask[A]): Unit =
    if !task.future.isDone then
      task.fiberContext.requestCancellation()
      val canceledBeforeStart =
        task.startState.compareAndSet(TaskStartState.Pending, TaskStartState.CanceledBeforeStart)
      if canceledBeforeStart then
        task.future.cancel(false)
        task.terminated.complete(())
      else await(task.terminated)

  private def sleepOnTimer(duration: FiniteDuration): Unit =
    if duration.length > 0L then
      runBlocking {
        val signal                   = new CompletableFuture[Unit]()
        val task: ScheduledFuture[?] = timer.schedule(
          () => signal.complete(()),
          duration.toNanos,
          TimeUnit.NANOSECONDS
        )

        try await(signal)
        finally
          if !signal.isDone then task.cancel(true)
      }

  private def runBlocking[A](thunk: => A): A =
    Option(runtimeContextLocal.get()) match
      case Some(RuntimeContext.Blocking) => thunk
      case _                             =>
        val task = submitThunk(blockingPool, RuntimeContext.Blocking)(thunk)
        try await(task)
        catch
          case error: InterruptedException => // waiting thread is interrupted , cancel the actual blocking operation
            task.cancel(true)
            throw error

  private def submitThunk[A](pool: ExecutorService, runtimeContext: RuntimeContext)(thunk: => A): Future[A] =
    pool.submit(new Callable[A]:
      override def call(): A =
        withRuntimeContext(runtimeContext)(thunk))

  private def withRuntimeContext[A](runtimeContext: RuntimeContext)(thunk: => A): A =
    val previous = Option(runtimeContextLocal.get())
    runtimeContextLocal.set(runtimeContext)
    try thunk
    finally
      previous match
        case Some(value) => runtimeContextLocal.set(value)
        case None        => runtimeContextLocal.remove()

  private def await[A](future: Future[A]): A =
    try future.get()
    catch
      case error: ExecutionException =>
        throw Option(error.getCause).getOrElse(error)
      case error: InterruptedException =>
        Thread.currentThread().interrupt()
        throw error

object ParIORuntime:
  lazy val default: ParIORuntime =
    new ParIORuntime(ParIORuntimeConfig.default)
