package io.parapet.effect

import io.parapet.core.{Parallel, SchedulerRuntime}

import java.util.concurrent.{
  Callable,
  CancellationException,
  CompletableFuture,
  ExecutionException,
  ExecutorCompletionService,
  ExecutorService,
  Executors,
  Future,
  LinkedBlockingQueue,
  ScheduledExecutorService,
  ScheduledFuture,
  SynchronousQueue,
  ThreadFactory,
  ThreadPoolExecutor,
  TimeUnit,
  TimeoutException
}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.concurrent.duration.*

/** Bounded executor sized to a fixed thread count.
  *
  * Submissions in excess of `size` queue in an unbounded `LinkedBlockingQueue` until a thread frees up. Threads are
  * pre-started so the first submissions don't pay creation cost.
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

  /** Default runtime:
    *   - elastic scheduler pool with core sized to available processors so every worker can actually start (the user
    *     may request more workers than available processors), threads time out after 60 s when idle
    *   - fixed parallel pool sized to available processors
    *   - fixed async pool sized to available processors
    *   - elastic blocking pool with zero core threads and an effectively unbounded maximum
    *   - elastic race pool with zero core threads and an effectively unbounded maximum
    *   - one daemon timer thread
    */
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

/** Internal helpers that construct the executors described by [[ParIORuntimeConfig]].
  *
  * Centralising the construction here means the "elastic" vs. "fixed" semantics are described once and every pool in
  * the runtime is built the same way. Callers do not need to know about `SynchronousQueue`, `allowCoreThreadTimeOut`,
  * or `prestartAllCoreThreads`.
  */
private[parapet] object Pools:

  /** Backed by a `ThreadPoolExecutor` with a `SynchronousQueue` and `allowCoreThreadTimeOut(true)`: submissions never
    * queue, threads spawn on demand up to `maxSize`, and idle threads (including core threads) terminate after
    * `keepAlive`. This shape is appropriate for any pool whose tasks may themselves block (sleeps, joins, nested races)
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

  /** Fixed-size executor: equivalent to `Executors.newFixedThreadPool(cfg.size, factory)`. Submissions in excess of
    * `cfg.size` queue in an unbounded `LinkedBlockingQueue`. All core threads are pre-started.
    */
  def fixed(cfg: FixedPoolConfig): ThreadPoolExecutor =
    val executor = new ThreadPoolExecutor(
      cfg.size,
      cfg.size,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](),
      namedThreadFactory(cfg.threadNamePrefix)
    )
    executor.prestartAllCoreThreads()
    executor

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
  final private case class BindFrame(run: Any => ParIO[Any])          extends Frame
  final private case class RecoverFrame(run: Throwable => ParIO[Any]) extends Frame

  private val runtimeContextLocal = new ThreadLocal[RuntimeContext]()

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

    def sleep(duration: FiniteDuration): ParIO[Unit] =
      ParIO.sleep(duration)

    def start[A](fa: ParIO[A]): ParIO[EffectFiber[ParIO, A]] =
      ParIO.delay(startFiberOn(asyncPool, RuntimeContext.Async, fa))

    def startBlocking[A](fa: ParIO[A]): ParIO[EffectFiber[ParIO, A]] =
      ParIO.delay(startFiberOn(blockingPool, RuntimeContext.Blocking, fa))

    def race[A, B](left: ParIO[A], right: ParIO[B]): ParIO[Either[A, B]] =
      ParIO.delay(racePrograms(left, right))

    def guarantee[A](fa: ParIO[A])(finalizer: ParIO[Unit]): ParIO[A] =
      def restoreInterrupt(wasInterrupted: Boolean): ParIO[Unit] =
        if wasInterrupted then ParIO.delay(Thread.currentThread().interrupt())
        else ParIO.unit

      def raiseOriginal[B](originalError: Throwable, wasInterrupted: Boolean): ParIO[B] =
        restoreInterrupt(wasInterrupted).flatMap(_ => ParIO.raiseError[B](originalError))

      fa
        .handleErrorWith { originalError =>
          ParIO.delay(Thread.interrupted()).flatMap { wasInterrupted =>
            finalizer
              .handleErrorWith { finalizerError =>
                originalError.addSuppressed(finalizerError)
                raiseOriginal[Unit](originalError, wasInterrupted)
              }
              .flatMap(_ => raiseOriginal[A](originalError, wasInterrupted))
          }
        }
        .flatMap(value => finalizer.flatMap(_ => ParIO.pure(value)))

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
      case Some(_) => unsafeRunLoop(fa)
      case None    => withRuntimeContext(RuntimeContext.External)(unsafeRunLoop(fa))

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

  private def unsafeRunLoop[A](io: ParIO[A]): A =
    var current: ParIO[Any] = io.asInstanceOf[ParIO[Any]]
    var stack: List[Frame]  = Nil

    while true do
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

          case Delay(thunk) =>
            current = Pure(thunk())

          case Blocking(thunk) =>
            current = Pure(runBlocking(thunk()))

          case Suspend(thunk) =>
            current = thunk().asInstanceOf[ParIO[Any]]

          case FlatMap(source, bind) =>
            current = source.asInstanceOf[ParIO[Any]]
            stack = BindFrame(bind.asInstanceOf[Any => ParIO[Any]]) :: stack

          case HandleError(source, handler) =>
            current = source.asInstanceOf[ParIO[Any]]
            stack = RecoverFrame(handler.asInstanceOf[Throwable => ParIO[Any]]) :: stack

          case Sleep(duration) =>
            sleepOnTimer(duration)
            current = Pure(())
      catch
        case error: Throwable =>
          var frames  = stack
          var handled = false
          while !handled && frames.nonEmpty do
            frames match
              case RecoverFrame(run) :: tail =>
                current = run(error)
                stack = tail
                handled = true
              case _ :: tail =>
                frames = tail
              case Nil =>
                ()

          if !handled then throw error

    throw new IllegalStateException("unreachable")

  private def startFiberOn[A](
      pool: ExecutorService,
      runtimeContext: RuntimeContext,
      fa: ParIO[A]
  ): EffectFiber[ParIO, A] =
    val result  = new CompletableFuture[A]()
    val started = new AtomicBoolean(false)
    val runner  = new AtomicReference[Thread]()
    val task    = pool.submit(new Callable[Unit]:
      override def call(): Unit =
        started.set(true)
        withRuntimeContext(runtimeContext) {
          val current = Thread.currentThread()
          runner.set(current)
          try result.complete(unsafeRunLoop(fa))
          catch case error: Throwable => result.completeExceptionally(error)
          finally runner.compareAndSet(current, null)
        })

    new EffectFiber[ParIO, A]:
      def join: ParIO[A] =
        ParIO.blocking(await(result))

      def cancel: ParIO[Unit] =
        ParIO.blocking {
          val cancellation = new CancellationException("fiber cancelled")
          task.cancel(true)

          if started.get() then
            Option(runner.get()).foreach(_.interrupt())
            try result.get(5, TimeUnit.SECONDS)
            catch
              case _: CancellationException => ()
              case _: ExecutionException    => ()
              case _: TimeoutException      => result.completeExceptionally(cancellation)
              case _: InterruptedException  =>
                result.completeExceptionally(cancellation)
                Thread.currentThread().interrupt()
          else result.completeExceptionally(cancellation)
          ()
        }

  private def racePrograms[A, B](left: ParIO[A], right: ParIO[B]): Either[A, B] =
    final case class RaceResult(tag: Int, value: Any)

    val completion = new ExecutorCompletionService[RaceResult](racePool)
    val leftFuture = completion.submit(new Callable[RaceResult]:
      override def call(): RaceResult =
        withRuntimeContext(RuntimeContext.Async)(RaceResult(0, unsafeRunLoop(left))))
    val rightFuture = completion.submit(new Callable[RaceResult]:
      override def call(): RaceResult =
        withRuntimeContext(RuntimeContext.Async)(RaceResult(1, unsafeRunLoop(right))))

    try
      val winner = completion.take().get()
      if winner.tag == 0 then rightFuture.cancel(true) else leftFuture.cancel(true)
      if winner.tag == 0 then Left(winner.value.asInstanceOf[A]) else Right(winner.value.asInstanceOf[B])
    catch
      case error: ExecutionException if error.getCause != null =>
        if !leftFuture.isDone then leftFuture.cancel(true)
        if !rightFuture.isDone then rightFuture.cancel(true)
        throw error.getCause
      case error: InterruptedException =>
        if !leftFuture.isDone then leftFuture.cancel(true)
        if !rightFuture.isDone then rightFuture.cancel(true)
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
      val completion = new ExecutorCompletionService[Unit](pool)
      val futures    = scala.collection.mutable.ArrayBuffer.empty[Future[Unit]]
      var completed  = false

      try
        effects.foreach(effect =>
          futures +=
            completion.submit(new Callable[Unit]:
              override def call(): Unit =
                withRuntimeContext(runtimeContext)(unsafeRunLoop(effect)))
        )

        var remaining = futures.size
        while remaining > 0 do
          completion.take().get()
          remaining -= 1

        completed = true
      catch
        case error: ExecutionException if error.getCause != null =>
          throw error.getCause
        case error: InterruptedException =>
          Thread.currentThread().interrupt()
          throw error
      finally if !completed then futures.foreach(future => if !future.isDone then future.cancel(true))

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
      case _                             => await(submitThunk(blockingPool, RuntimeContext.Blocking)(thunk))

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
      case error: ExecutionException if error.getCause != null =>
        throw error.getCause
      case error: InterruptedException =>
        Thread.currentThread().interrupt()
        throw error

object ParIORuntime:
  lazy val default: ParIORuntime =
    new ParIORuntime(ParIORuntimeConfig.default)
