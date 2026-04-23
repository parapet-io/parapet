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

sealed trait ParIO[+A]:
  final def map[B](f: A => B): ParIO[B] =
    flatMap(a => ParIO.pure(f(a)))

  final def flatMap[B](f: A => ParIO[B]): ParIO[B] =
    ParIO.FlatMap(this, f)

  final def handleErrorWith[B >: A](f: Throwable => ParIO[B]): ParIO[B] =
    ParIO.HandleError(this, f)

  final def unsafeRunSync(): A =
    ParIO.unsafeRun(this)

object ParIO:
  final case class Pure[A](value: A) extends ParIO[A]
  final case class Delay[A](thunk: () => A) extends ParIO[A]
  final case class Blocking[A](thunk: () => A) extends ParIO[A]
  final case class Suspend[A](thunk: () => ParIO[A]) extends ParIO[A]
  final case class FlatMap[A, B](source: ParIO[A], bind: A => ParIO[B]) extends ParIO[B]
  final case class HandleError[A](source: ParIO[A], handler: Throwable => ParIO[A]) extends ParIO[A]
  final case class Sleep(duration: FiniteDuration) extends ParIO[Unit]

  private sealed trait Frame
  private final case class BindFrame(run: Any => ParIO[Any]) extends Frame
  private final case class RecoverFrame(run: Throwable => ParIO[Any]) extends Frame

  private final val workerPool: ExecutorService =
    Executors.newCachedThreadPool(new ThreadFactory:
      private val index = new AtomicInteger(0)

      override def newThread(runnable: Runnable): Thread =
        val thread = new Thread(runnable)
        thread.setName(s"parapet-io-${index.incrementAndGet()}")
        thread.setDaemon(true)
        thread
    )

  def pure[A](value: A): ParIO[A] =
    Pure(value)

  def unit: ParIO[Unit] =
    pure(())

  def delay[A](thunk: => A): ParIO[A] =
    Delay(() => thunk)

  def blocking[A](thunk: => A): ParIO[A] =
    Blocking(() => thunk)

  def suspend[A](thunk: => ParIO[A]): ParIO[A] =
    Suspend(() => thunk)

  def raiseError[A](error: Throwable): ParIO[A] =
    Delay(() => throw error)

  def sleep(duration: FiniteDuration): ParIO[Unit] =
    Sleep(duration)

  private def submit[A](fa: ParIO[A]): JFuture[A] =
    workerPool.submit(new Callable[A]:
      override def call(): A =
        unsafeRun(fa)
    )

  private def unwrap[A](future: JFuture[A]): A =
    try future.get()
    catch
      case error: ExecutionException if error.getCause != null =>
        throw error.getCause

  private def unsafeRun[A](io: ParIO[A]): A =
    var current: ParIO[Any] = io.asInstanceOf[ParIO[Any]]
    var stack: List[Frame] = Nil

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
          var frames = stack
          var handled = false
          while !handled && frames.nonEmpty do
            frames match
              case RecoverFrame(run) :: tail =>
                current = run(error)
                stack = tail
                handled = true
              case _ :: tail =>
                frames = tail

          if !handled then
            throw error

    throw new IllegalStateException("unreachable")

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
        val task = workerPool.submit(new Callable[Unit]:
          override def call(): Unit =
            runner.set(Thread.currentThread())
            try result.complete(unsafeRun(fa))
            catch case error: Throwable => result.completeExceptionally(error)
        )

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
            RaceResult(0, unsafeRun(left))
        )
        val rightFuture = completion.submit(new Callable[RaceResult]:
          override def call(): RaceResult =
            RaceResult(1, unsafeRun(right))
        )

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

  given parallel: io.parapet.core.Parallel[ParIO] with
    def par(effects: Seq[ParIO[Unit]]): ParIO[Unit] =
      ParIO.blocking {
        val futures = effects.map(submit)
        try futures.foreach(unwrap)
        finally futures.foreach(_.cancel(true))
        ()
      }
