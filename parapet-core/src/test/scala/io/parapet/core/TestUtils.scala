package io.parapet.core

import io.parapet.core.Dsl.*
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.{Deliver, SubmissionResult, Task}
import io.parapet.core.processes.Noop
import io.parapet.effect.{Effect, EffectFiber, Monad}
import io.parapet.{Envelope, ProcessRef, Scope}

import java.util.concurrent.CancellationException
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

object TestUtils:
  type Id[A] = A

  given Monad[Id] with
    def pure[A](value: A): A = value

    extension [A](fa: A) def flatMap[B](f: A => B): B = f(fa)

  sealed trait TestIO[+A]:
    def unsafeRun(): A

    final def map[B](f: A => B): TestIO[B] =
      flatMap(value => TestIO.pure(f(value)))

    final def flatMap[B](f: A => TestIO[B]): TestIO[B] =
      TestIO.delay(f(unsafeRun()).unsafeRun())

    final def handleErrorWith[B >: A](f: Throwable => TestIO[B]): TestIO[B] =
      TestIO.delay {
        try unsafeRun()
        catch case error: Throwable => f(error).unsafeRun()
      }

  object TestIO:
    def pure[A](value: A): TestIO[A] =
      new TestIO[A]:
        def unsafeRun(): A = value

    def delay[A](thunk: => A): TestIO[A] =
      new TestIO[A]:
        def unsafeRun(): A = thunk

    def unit: TestIO[Unit] =
      pure(())

    def raiseError[A](error: Throwable): TestIO[A] =
      delay(throw error)

  given Effect[TestIO] with
    def pure[A](value: A): TestIO[A] =
      TestIO.pure(value)

    extension [A](fa: TestIO[A])
      def flatMap[B](f: A => TestIO[B]): TestIO[B] =
        fa.flatMap(f)

      override def map[B](f: A => B): TestIO[B] =
        fa.map(f)

      def handleErrorWith(f: Throwable => TestIO[A]): TestIO[A] =
        fa.handleErrorWith(f)

    def delay[A](thunk: => A): TestIO[A] =
      TestIO.delay(thunk)

    def blocking[A](thunk: => A): TestIO[A] =
      TestIO.delay(thunk)

    def suspend[A](thunk: => TestIO[A]): TestIO[A] =
      TestIO.delay(thunk.unsafeRun())

    def raiseError[A](error: Throwable): TestIO[A] =
      TestIO.raiseError(error)

    def sleep(duration: FiniteDuration): TestIO[Unit] =
      TestIO.delay(Thread.sleep(duration.toMillis))

    def start[A](fa: TestIO[A]): TestIO[EffectFiber[TestIO, A]] =
      TestIO.delay {
        val result = fa.handleErrorWith(TestIO.raiseError).unsafeRun()
        new EffectFiber[TestIO, A]:
          def join: TestIO[A]      = TestIO.pure(result)
          def cancel: TestIO[Unit] = TestIO.unit
      }

    def startBlocking[A](fa: TestIO[A]): TestIO[EffectFiber[TestIO, A]] =
      start(fa)

    def race[A, B](left: TestIO[A], right: TestIO[B]): TestIO[Either[A, B]] =
      TestIO.delay(Left(left.unsafeRun()))

    def guarantee[A](fa: TestIO[A])(finalizer: TestIO[Unit]): TestIO[A] =
      TestIO.delay {
        try
          val value = fa.unsafeRun()
          finalizer.unsafeRun()
          value
        catch
          case original: Throwable =>
            try finalizer.unsafeRun()
            catch case finalizerError: Throwable => original.addSuppressed(finalizerError)
            throw original
      }

  final class RuntimeFixture:
    val captured: ListBuffer[Envelope] = ListBuffer.empty

    private val scheduler: Scheduler[TestIO] = new Scheduler[TestIO]:
      def start: TestIO[Unit] = TestIO.unit

      def submit(task: Task[TestIO]): TestIO[SubmissionResult] = task match
        case Deliver(env, _) => TestIO.delay { captured += env; Scheduler.Ok }
        case _               => TestIO.pure(Scheduler.Ok)

    val context: Context[TestIO] =
      Context[TestIO](ParConfig.default, EventStore.stub[TestIO], EventTransformers.empty).unsafeRun()

    context.start(scheduler).unsafeRun()

    private val noop: Noop[TestIO] = new Noop[TestIO]
    context.register(ProcessRef.SystemRef, noop).unsafeRun()

    captured.clear() // drop any boot-time envelopes captured by the stub scheduler

    private val impl = DslInterpreter[TestIO](context)

    def runWithSender[A](
        sender: ProcessRef.Unknown,
        program: DslF[TestIO, A],
        scope: Scope = Scope.empty
    ): A =
      program
        .foldMap(
          impl.interpret(
            sender,
            context.getProcessState(noop.ref).get,
            ExecutionTrace.Dummy,
            scope
          )
        )
        .unsafeRun()

    def run[A](program: DslF[TestIO, A], scope: Scope = Scope.empty): A =
      runWithSender(ProcessRef.UndefinedRef, program, scope)
