package io.parapet.core

import io.parapet.core.Dsl.*
import io.parapet.effect.{Effect, EffectFiber, Monad}
import io.parapet.free.FunctionK
import io.parapet.{Event, ProcessRef}

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

  final case class Message(event: Event, target: ProcessRef)

  final class Execution(val trace: ListBuffer[Message] = ListBuffer.empty):
    override def toString: String =
      trace.toString()

  final class IdInterpreter(
      execution: Execution = new Execution(),
      mapper: Event => Event = identity,
      senderRef: ProcessRef = ProcessRef.UndefinedRef
  ) extends FunctionK[[x] =>> FlowOp[Id, x], Id]:

    def apply[A](fa: FlowOp[Id, A]): A =
      fa match
        case UnitFlow() =>
          ().asInstanceOf[A]

        case Pure(value) =>
          value

        case Send(event, _, receiver, receivers) =>
          val mapped = mapper(event())
          execution.trace.append(Message(mapped, receiver))
          receivers.foreach(target => execution.trace.append(Message(mapped, target)))
          ().asInstanceOf[A]

        case Forward(event, receivers) =>
          val mapped = mapper(event())
          receivers.foreach(target => execution.trace.append(Message(mapped, target)))
          ().asInstanceOf[A]

        case Par(flow) =>
          flow.asInstanceOf[DslF[Id, Unit]].foldMap(this)
          ().asInstanceOf[A]

        case Delay(_) =>
          ().asInstanceOf[A]

        case WithSender(run) =>
          run.asInstanceOf[ProcessRef => DslF[Id, A]](senderRef).foldMap(this)

        case Fork(flow) =>
          new Fiber.IdFiber(flow.asInstanceOf[DslF[Id, A]].foldMap(this)).asInstanceOf[A]

        case Register(_, _) =>
          ().asInstanceOf[A]

        case Race(first, _) =>
          Left(first.asInstanceOf[DslF[Id, Any]].foldMap(this)).asInstanceOf[A]

        case Suspend(thunk) =>
          thunk().asInstanceOf[A]

        case SuspendF(thunk) =>
          thunk().asInstanceOf[DslF[Id, A]].foldMap(this)

        case Eval(thunk) =>
          thunk().asInstanceOf[A]

        case Offload(body) =>
          body().asInstanceOf[DslF[Id, Any]].foldMap(this)
          ().asInstanceOf[A]

        case RaiseError(error) =>
          throw error

        case HandleError(body, onError) =>
          try body().asInstanceOf[DslF[Id, A]].foldMap(this)
          catch case error: Throwable => onError(error).asInstanceOf[DslF[Id, A]].foldMap(this)

        case Halt(_) =>
          ().asInstanceOf[A]

        case Guarantee(body, finalizer) =>
          try
            body().asInstanceOf[DslF[Id, Any]].foldMap(this)
            finalizer().asInstanceOf[DslF[Id, Unit]].foldMap(this)
            ().asInstanceOf[A]
          catch
            case error: Throwable =>
              finalizer().asInstanceOf[DslF[Id, Unit]].foldMap(this)
              throw error

        case Dsl.Lock(_) =>
          ().asInstanceOf[A]

        case Dsl.Unlock(_) =>
          ().asInstanceOf[A]
