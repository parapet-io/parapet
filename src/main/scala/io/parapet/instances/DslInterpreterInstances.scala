package io.parapet.instances

import cats.data.State
import cats.effect.IO.{Delay, Suspend}
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.flatMap._
import cats.~>
import io.parapet.core.Dsl._
import io.parapet.core.Event.Envelope
import io.parapet.core.DslInterpreter._
import io.parapet.core.Parallel
import io.parapet.instances.parallel._
import cats.instances.list._
import cats.syntax.traverse._
import io.parapet.core.Queue.Enqueue
import io.parapet.core.Scheduler.{Deliver, Task, Terminate}

object DslInterpreterInstances {

  type TaskQueue[F[_]] = Enqueue[F, Task[F]]

  object dslInterpreterForCatsIO {
    def ioFlowInterpreter(taskQueue: TaskQueue[IO])
                         (implicit ctx: ContextShift[IO], timer: Timer[IO]): FlowOp[IO, ?] ~> Flow[IO, ?] =
      new (FlowOp[IO, ?] ~> Flow[IO, ?]) {

        override def apply[A](fa: FlowOp[IO, A]): Flow[IO, A] = {

          val parallel: Parallel[IO] = Parallel[IO]
          val interpreter: Interpreter[IO] = ioFlowInterpreter(taskQueue) or ioEffectInterpreter
          fa match {
            case Empty() => State[FlowState[IO], Unit] { s => (s, ()) }
            case Use(resource, f) => State[FlowState[IO], Unit] { s =>
              val res = IO.delay(resource()) >>= (r => interpret(f(r).asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty)).toList.sequence)
              (s.addOps(List(res)), ())
            }
            case Stop() => State[FlowState[IO], Unit] { s => (s.addOps(List(taskQueue.enqueue(Terminate()))), ()) }
            case Send(event, receivers) =>
              State[FlowState[IO], Unit] { s =>
                val ops = receivers.map(receiver => taskQueue.enqueue(Deliver(Envelope(s.selfRef, event, receiver))))
                (s.addOps(ops), ())
              }
            case Par(flows) =>
              State[FlowState[IO], Unit] { s =>
                val res = parallel.par(
                  flows.map(flow => interpret_(flow.asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty))))
                (s.addOps(List(res)), ())
              }
            case Delay(duration, Some(flow)) =>
              State[FlowState[IO], Unit] { s =>
                val delayIO = IO.sleep(duration)
                val res = interpret(flow.asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty)).map(op => delayIO >> op)
                (s.addOps(res), ())
              }
            case Delay(duration, None) =>
              State[FlowState[IO], Unit] { s => (s.addOps(List(IO.sleep(duration))), ()) }

            case Reply(f) =>
              State[FlowState[IO], Unit] { s =>
                (
                  s.addOps(interpret(f(s.senderRef).asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty))),
                  ()
                )
              }
          }
        }
      }

    def ioEffectInterpreter: Effect[IO, ?] ~> Flow[IO, ?] = new (Effect[IO, ?] ~> Flow[IO, ?]) {
      override def apply[A](fa: Effect[IO, A]): Flow[IO, A] = fa match {
        case Suspend(thunk) => State.modify[FlowState[IO]](s => s.addOps(List(thunk())))
        case Eval(thunk) => State.modify[FlowState[IO]](s => s.addOps(List(IO(thunk()))))
      }
    }
  }



}
