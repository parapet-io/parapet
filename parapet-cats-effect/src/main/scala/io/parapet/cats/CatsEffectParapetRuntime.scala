package io.parapet.cats

import cats.effect.{FiberIO, IO, Outcome}
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import io.parapet.ParApp
import io.parapet.core.{Parallel, SchedulerRuntime}
import io.parapet.effect.{Effect, EffectFiber}

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CancellationException, ExecutorService, Executors, ThreadFactory}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/** Cats Effect backend. */
final class CatsEffectParapetRuntime private (
    schedulerExecutor: ExecutorService,
    blockingExecutor: ExecutorService
) extends AutoCloseable:
  private val schedulerExecutionContext = ExecutionContext.fromExecutorService(schedulerExecutor)
  private val blockingExecutionContext  = ExecutionContext.fromExecutorService(blockingExecutor)

  given effect: Effect[IO] with
    def pure[A](value: A): IO[A] =
      IO.pure(value)

    extension [A](fa: IO[A])
      def flatMap[B](f: A => IO[B]): IO[B] =
        fa.flatMap(f)

      override def map[B](f: A => B): IO[B] =
        fa.map(f)

      def handleErrorWith(f: Throwable => IO[A]): IO[A] =
        fa.handleErrorWith(f)

    def delay[A](thunk: => A): IO[A] =
      IO.delay(thunk)

    def blocking[A](thunk: => A): IO[A] =
      IO.interruptible(thunk)

    def suspend[A](thunk: => IO[A]): IO[A] =
      IO.defer(thunk)

    def raiseError[A](error: Throwable): IO[A] =
      IO.raiseError(error)

    def sleep(duration: FiniteDuration): IO[Unit] =
      IO.sleep(duration)

    def start[A](fa: IO[A]): IO[EffectFiber[IO, A]] =
      fa.start.map(wrapFiber)

    def startBlocking[A](fa: IO[A]): IO[EffectFiber[IO, A]] =
      fa.evalOn(blockingExecutionContext).start.map(wrapFiber)

    def race[A, B](left: IO[A], right: IO[B]): IO[Either[A, B]] =
      IO.race(left, right)

    def guarantee[A](fa: IO[A])(finalizer: IO[Unit]): IO[A] =
      fa.guarantee(finalizer)

  given parallel: Parallel[IO] with
    def par(effects: Seq[IO[Unit]]): IO[Unit] =
      effects.toList.parSequence_

  private[parapet] given schedulerRuntime: SchedulerRuntime[IO] with
    def runSchedulerWorkers(workers: Seq[IO[Unit]]): IO[Unit] =
      workers.toList.parTraverse_(worker => worker.evalOn(schedulerExecutionContext))

  override def close(): Unit =
    schedulerExecutor.shutdownNow()
    blockingExecutor.shutdownNow()

  private def wrapFiber[A](fiber: FiberIO[A]): EffectFiber[IO, A] =
    new EffectFiber[IO, A]:
      def join: IO[A] =
        fiber.join.flatMap {
          case Outcome.Succeeded(value) => value
          case Outcome.Errored(error)   => IO.raiseError(error)
          case Outcome.Canceled()       => IO.raiseError(new CancellationException("fiber cancelled"))
        }

      def cancel: IO[Unit] =
        fiber.cancel

object CatsEffectParapetRuntime:
  private val DefaultParallelism = math.max(2, Runtime.getRuntime.availableProcessors())

  def apply(
      schedulerThreads: Int = DefaultParallelism,
      blockingThreads: Int = DefaultParallelism
  ): CatsEffectParapetRuntime =
    new CatsEffectParapetRuntime(
      Executors.newFixedThreadPool(schedulerThreads, namedThreadFactory("parapet-cats-scheduler")),
      Executors.newFixedThreadPool(blockingThreads, namedThreadFactory("parapet-cats-blocking"))
    )

  lazy val default: CatsEffectParapetRuntime =
    apply()

  private def namedThreadFactory(prefix: String): ThreadFactory =
    new ThreadFactory:
      private val index = new AtomicInteger(0)

      override def newThread(runnable: Runnable): Thread =
        val thread = new Thread(runnable)
        thread.setName(s"$prefix-${index.incrementAndGet()}")
        thread.setDaemon(true)
        thread

/** App base for running Parapet with Cats Effect `IO`. */
trait CatsEffectParApp extends ParApp[IO]:
  protected def runtime: CatsEffectParapetRuntime =
    CatsEffectParapetRuntime.default

  protected def effectInstance: Effect[IO] =
    runtime.effect

  protected def parallelInstance: Parallel[IO] =
    runtime.parallel

  override private[parapet] def schedulerRuntimeInstance: SchedulerRuntime[IO] =
    runtime.schedulerRuntime

  def unsafeRun(program: IO[Unit]): Unit =
    program.unsafeRunSync()
