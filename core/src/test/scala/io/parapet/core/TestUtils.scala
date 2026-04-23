package io.parapet.core

import io.parapet.core.Dsl.*
import io.parapet.effect.Monad
import io.parapet.free.FunctionK
import io.parapet.{Event, ProcessRef}

import scala.collection.mutable.ListBuffer

object TestUtils:
  type Id[A] = A

  given Monad[Id] with
    def pure[A](value: A): A = value

    extension [A](fa: A)
      def flatMap[B](f: A => B): B = f(fa)

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

        case Blocking(body) =>
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
