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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
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
  import ParIORuntime.CancellationSignal

  private enum RuntimeContext:
    case External, Scheduler, Parallel, Async, Blocking

  sealed private trait Frame
  final private case class BindFrame(run: Any => ParIO[Any])          extends Frame
  final private case class RecoverFrame(run: Throwable => ParIO[Any]) extends Frame
  final private case class CancelFrame(run: ParIO[Any])               extends Frame
  final private case class GuaranteeFrame(run: ParIO[Any])            extends Frame

  final private case class RunningTask[A](
      future: Future[A],
      cancellationSignal: CancellationSignal,
      started: AtomicBoolean,
      terminated: CompletableFuture[Unit]
  )

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

    def outcome[A](fa: ParIO[A]): ParIO[Outcome[A]] =
      ParIO.OutcomeOf(fa)

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
      case Some(_) => unsafeRunLoop(fa, new CancellationSignal())
      case None    => withRuntimeContext(RuntimeContext.External)(unsafeRunLoop(fa, new CancellationSignal()))

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

  private def unsafeRunLoop[A](io: ParIO[A], cancellationSignal: CancellationSignal): A =
    var current: ParIO[Any] = io.asInstanceOf[ParIO[Any]]
    var stack: List[Frame]  = Nil

    while true do
      if cancellationSignal.isRequested then throw runCancellationFinalizers(stack)

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
                unsafeRunLoop(finalizer, new CancellationSignal())
                current = Pure(value)

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

          case OutcomeOf(source) =>
            val childSignal = cancellationSignal.child()
            try
              val value = unsafeRunLoop(source, childSignal)
              current = Pure(Outcome.Succeeded(value))
            catch
              // Cancellation of the enclosing computation must propagate.
              case error: Throwable if cancellationSignal.isRequested =>
                throw error

              // Cancellation requested by the source is materialized.
              case _: Throwable if childSignal.isRequested =>
                current = Pure(Outcome.Canceled())

              case error: Throwable =>
                current = Pure(Outcome.Failed(error))

          case FlatMap(source, bind) =>
            current = source.asInstanceOf[ParIO[Any]]
            stack = BindFrame(bind.asInstanceOf[Any => ParIO[Any]]) :: stack

          case HandleError(source, handler) =>
            current = source.asInstanceOf[ParIO[Any]]
            stack = RecoverFrame(handler.asInstanceOf[Throwable => ParIO[Any]]) :: stack

          case Canceled =>
            cancellationSignal.request()
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
        case _: Throwable if cancellationSignal.isRequested =>
          throw runCancellationFinalizers(stack)
        case error: Throwable =>
          var frames  = stack
          var handled = false
          while !handled && frames.nonEmpty do
            frames match
              case RecoverFrame(run) :: tail =>
                current = run(error)
                stack = tail
                handled = true
              case GuaranteeFrame(finalizer) :: tail =>
                val wasInterrupted = Thread.interrupted()
                try unsafeRunLoop(finalizer, new CancellationSignal())
                catch case finalizerError: Throwable => error.addSuppressed(finalizerError)
                finally if wasInterrupted then Thread.currentThread().interrupt()
                frames = tail
              case _ :: tail =>
                frames = tail
              case Nil =>
                ()

          if !handled then throw error

    throw new IllegalStateException("unreachable")

  private def runCancellationFinalizers(stack: List[Frame]): CancellationException =
    val cancellation = new CancellationException("fiber canceled")
    var frames       = stack

    while frames.nonEmpty do
      frames match
        case CancelFrame(finalizer) :: tail =>
          Thread.interrupted()
          try unsafeRunLoop(finalizer, new CancellationSignal())
          catch case error: Throwable => cancellation.addSuppressed(error)
          finally Thread.interrupted()
          frames = tail
        case GuaranteeFrame(finalizer) :: tail =>
          Thread.interrupted()
          try unsafeRunLoop(finalizer, new CancellationSignal())
          catch case error: Throwable => cancellation.addSuppressed(error)
          finally Thread.interrupted()
          frames = tail
        case _ :: tail =>
          frames = tail
        case Nil =>
          ()

    cancellation

  private def startFiberOn[A](
      pool: ExecutorService,
      runtimeContext: RuntimeContext,
      fa: ParIO[A]
  ): EffectFiber[ParIO, A] =
    // `task` is the executor cancellation handle and becomes canceled before a running computation has necessarily
    // finished unwinding. `result` is completed by the runner after `unsafeRunLoop` terminates, so `join` can observe
    // the fiber outcome and `cancel` can wait for cancellation finalizers to finish.
    val result             = new CompletableFuture[Outcome[A]]()
    val started            = new AtomicBoolean(false)
    val cancellationSignal = new CancellationSignal()
    val task               = pool.submit(new Callable[Unit]:
      override def call(): Unit =
        started.set(true)
        withRuntimeContext(runtimeContext) {
          try result.complete(Outcome.Succeeded(unsafeRunLoop(fa, cancellationSignal)))
          catch
            case _: Throwable if cancellationSignal.isRequested =>
              result.complete(Outcome.Canceled())
            case error: Throwable =>
              result.complete(Outcome.Failed(error))
        })

    new EffectFiber[ParIO, A]:
      def join: ParIO[Outcome[A]] =
        ParIO.blocking(await(result))

      def cancel: ParIO[Unit] =
        ParIO.blocking {
          if cancellationSignal.request() then
            task.cancel(true)

            if started.get() then
              try result.get()
              catch
                case _: CancellationException    => ()
                case _: ExecutionException       => ()
                case error: InterruptedException =>
                  Thread.currentThread().interrupt()
                  throw error
            else result.complete(Outcome.Canceled())
            ()
        }

  private def racePrograms[A, B](left: ParIO[A], right: ParIO[B]): Outcome[Either[A, B]] =
    final case class RaceResult(tag: Int, outcome: Outcome[Any])

    def runBranch[X](tag: Int, program: ParIO[X], signal: CancellationSignal): RaceResult =
      val outcome: Outcome[X] =
        try Outcome.Succeeded(unsafeRunLoop(program, signal))
        catch
          case _: Throwable if signal.isRequested => Outcome.Canceled()
          case error: Throwable                   => Outcome.Failed(error)
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
      val cancellationSignal = new CancellationSignal()
      val started            = new AtomicBoolean(false)
      val terminated         = new CompletableFuture[Unit]()
      val future             = completion.submit(new Callable[RaceResult]:
        override def call(): RaceResult =
          started.set(true)
          try withRuntimeContext(RuntimeContext.Async)(runBranch(tag, program, cancellationSignal))
          finally terminated.complete(()))
      RunningTask(future, cancellationSignal, started, terminated)

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
          val cancellationSignal = new CancellationSignal()
          val started            = new AtomicBoolean(false)
          val terminated         = new CompletableFuture[Unit]()
          val future             = completion.submit(
            new Callable[Unit]:
              override def call(): Unit =
                started.set(true)
                try withRuntimeContext(runtimeContext)(unsafeRunLoop(effect, cancellationSignal))
                finally terminated.complete(())
          )
          running += RunningTask(future, cancellationSignal, started, terminated)
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
      task.cancellationSignal.request()
      task.future.cancel(true)
      if task.started.get() then await(task.terminated)

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

  final private class CancellationSignal(parent: Option[CancellationSignal] = None):
    private val requested = new AtomicBoolean(false)

    def child(): CancellationSignal =
      new CancellationSignal(Some(this))

    def request(): Boolean =
      requested.compareAndSet(false, true)

    def isRequested: Boolean =
      requested.get() || parent.exists(_.isRequested)
