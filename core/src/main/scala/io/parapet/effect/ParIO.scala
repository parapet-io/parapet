package io.parapet.effect

import java.util.concurrent.{
  Callable,
  CancellationException,
  CompletableFuture,
  ExecutionException,
  ExecutorCompletionService,
  ExecutorService,
  Executors,
  Future => JFuture,
  ThreadFactory,
  TimeUnit,
  TimeoutException
}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.FiniteDuration

/** Parapet's effect type.
  *
  * `ParIO` programs are values describing computations; nothing runs until [[ParIO.unsafeRunSync]] is called (typically
  * by [[io.parapet.ParIOApp]]).
  *
  * The implementation is a trampolined interpreter over an algebra of constructors ([[ParIO.Pure]], [[ParIO.Delay]],
  * [[ParIO.Blocking]], [[ParIO.Suspend]], [[ParIO.FlatMap]], [[ParIO.HandleError]], [[ParIO.Sleep]]) avoiding stack
  * growth even for deeply nested binds. Concurrency primitives (start/race/par) delegate to a cached thread pool of
  * daemon worker threads.
  */
sealed trait ParIO[+A]:
  /** Maps the result. */
  final def map[B](f: A => B): ParIO[B] =
    flatMap(a => ParIO.pure(f(a)))

  /** Sequences the next computation. */
  final def flatMap[B](f: A => ParIO[B]): ParIO[B] =
    ParIO.FlatMap(this, f)

  /** Recovers from an exception via `f`. */
  final def handleErrorWith[B >: A](f: Throwable => ParIO[B]): ParIO[B] =
    ParIO.HandleError(this, f)

  /** Synchronously runs the program on the calling thread, blocking until it completes (or rethrowing any uncaught
    * error).
    */
  final def unsafeRunSync(): A =
    ParIO.unsafeRun(this)

/** [[ParIO]] constructors, the trampolined interpreter, and the type-class instances required by the parapet runtime.
  */
object ParIO:
  /** Wraps an already-known value. */
  final case class Pure[A](value: A) extends ParIO[A]

  /** A pure but lazy computation. */
  final case class Delay[A](thunk: () => A) extends ParIO[A]

  /** A blocking computation that should run on the worker pool. */
  final case class Blocking[A](thunk: () => A) extends ParIO[A]

  /** Defers construction of a `ParIO`. */
  final case class Suspend[A](thunk: () => ParIO[A]) extends ParIO[A]

  /** Sequencing constructor used by [[ParIO.flatMap]]. */
  final case class FlatMap[A, B](source: ParIO[A], bind: A => ParIO[B]) extends ParIO[B]

  /** Error-recovery constructor used by [[ParIO.handleErrorWith]]. */
  final case class HandleError[A](source: ParIO[A], handler: Throwable => ParIO[A]) extends ParIO[A]

  /** Non-blocking-style sleep that uses `Thread.sleep` under the hood. */
  final case class Sleep(duration: FiniteDuration) extends ParIO[Unit]

  sealed private trait Frame
  final private case class BindFrame(run: Any => ParIO[Any])          extends Frame
  final private case class RecoverFrame(run: Throwable => ParIO[Any]) extends Frame

  final private val workerPool: ExecutorService =
    Executors.newCachedThreadPool(new ThreadFactory:
      private val index = new AtomicInteger(0)

      override def newThread(runnable: Runnable): Thread =
        val thread = new Thread(runnable)
        thread.setName(s"parapet-io-${index.incrementAndGet()}")
        thread.setDaemon(true)
        thread)

  /** Lifts a pure value. */
  def pure[A](value: A): ParIO[A] =
    Pure(value)

  /** The `pure(())` constant. */
  def unit: ParIO[Unit] =
    pure(())

  /** Defers a pure but lazy computation. */
  def delay[A](thunk: => A): ParIO[A] =
    Delay(() => thunk)

  /** Defers a blocking computation. */
  def blocking[A](thunk: => A): ParIO[A] =
    Blocking(() => thunk)

  /** Defers construction of a `ParIO`. */
  def suspend[A](thunk: => ParIO[A]): ParIO[A] =
    Suspend(() => thunk)

  /** Aborts with `error` when interpreted. */
  def raiseError[A](error: Throwable): ParIO[A] =
    Delay(() => throw error)

  /** Sleeps for `duration`. */
  def sleep(duration: FiniteDuration): ParIO[Unit] =
    Sleep(duration)

  private def submit[A](fa: ParIO[A]): JFuture[A] =
    workerPool.submit(new Callable[A]:
      override def call(): A =
        unsafeRun(fa))

  private def unwrap[A](future: JFuture[A]): A =
    try future.get()
    catch
      case error: ExecutionException if error.getCause != null =>
        throw error.getCause

  private def unsafeRun[A](io: ParIO[A]): A =
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
            current = Pure(thunk())

          case Suspend(thunk) =>
            current = thunk().asInstanceOf[ParIO[Any]]

          case FlatMap(source, bind) =>
            current = source.asInstanceOf[ParIO[Any]]
            stack = BindFrame(bind.asInstanceOf[Any => ParIO[Any]]) :: stack

          case HandleError(source, handler) =>
            current = source.asInstanceOf[ParIO[Any]]
            stack = RecoverFrame(handler.asInstanceOf[Throwable => ParIO[Any]]) :: stack

          case Sleep(duration) =>
            Thread.sleep(duration.toMillis)
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

          if !handled then throw error

    throw new IllegalStateException("unreachable")

  /** [[Effect]] type-class instance for [[ParIO]] - the bridge that lets the parapet runtime drive `ParIO` programs via
    * the same generic interpreter used for other effect types.
    */
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
      ParIO.delay {
        val result = new CompletableFuture[A]()
        val runner = new AtomicReference[Thread]()
        val task   = workerPool.submit(new Callable[Unit]:
          override def call(): Unit =
            runner.set(Thread.currentThread())
            try result.complete(unsafeRun(fa))
            catch case error: Throwable => result.completeExceptionally(error))

        new EffectFiber[ParIO, A]:
          def join: ParIO[A] =
            ParIO.blocking(unwrap(result))

          def cancel: ParIO[Unit] =
            ParIO.blocking {
              Option(runner.get()).foreach(_.interrupt())
              task.cancel(true)
              try result.get(5, TimeUnit.SECONDS)
              catch
                case _: CancellationException => ()
                case _: ExecutionException    => ()
                case _: TimeoutException      => ()
                case _: InterruptedException  => Thread.currentThread().interrupt()
              ()
            }
      }

    def race[A, B](left: ParIO[A], right: ParIO[B]): ParIO[Either[A, B]] =
      ParIO.blocking {
        final case class RaceResult(tag: Int, value: Any)

        val completion = new ExecutorCompletionService[RaceResult](workerPool)
        val leftFuture = completion.submit(new Callable[RaceResult]:
          override def call(): RaceResult =
            RaceResult(0, unsafeRun(left)))
        val rightFuture = completion.submit(new Callable[RaceResult]:
          override def call(): RaceResult =
            RaceResult(1, unsafeRun(right)))

        try
          val winner = completion.take().get()
          if winner.tag == 0 then rightFuture.cancel(true) else leftFuture.cancel(true)
          if winner.tag == 0 then Left(winner.value.asInstanceOf[A]) else Right(winner.value.asInstanceOf[B])
        catch
          case error: ExecutionException if error.getCause != null =>
            if !leftFuture.isDone then leftFuture.cancel(true)
            if !rightFuture.isDone then rightFuture.cancel(true)
            throw error.getCause
      }

    def guarantee[A](fa: ParIO[A])(finalizer: ParIO[Unit]): ParIO[A] =
      ParIO.delay {
        val result =
          try Right(fa.unsafeRunSync())
          catch case error: Throwable => Left(error)

        try finalizer.unsafeRunSync()
        catch
          case finalizerError: Throwable =>
            result match
              case Left(originalError) =>
                originalError.addSuppressed(finalizerError)
                throw originalError
              case Right(_) =>
                throw finalizerError

        result match
          case Left(error)  => throw error
          case Right(value) => value
      }

  /** [[io.parapet.core.Parallel]] instance for [[ParIO]] - backs the scheduler's `parallel.par` calls used to spin up
    * worker fibers and fan out child shutdowns.
    */
  given parallel: io.parapet.core.Parallel[ParIO] with
    def par(effects: Seq[ParIO[Unit]]): ParIO[Unit] =
      ParIO.blocking {
        val futures = effects.map(submit)
        try futures.foreach(unwrap)
        finally futures.foreach(_.cancel(true))
        ()
      }
