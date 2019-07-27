package io.parapet.instances

import cats.data.StateT
import cats.effect.{ContextShift, IO, Timer}
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.syntax.functor._
import cats.~>
import io.parapet.core.Dsl._
import io.parapet.core.DslInterpreter._
import io.parapet.core.Event.Envelope
import io.parapet.core.Queue.Enqueue
import io.parapet.core.Scheduler.{Deliver, Task}
import io.parapet.core.{Context, Parallel, Process, ProcessRef}
import io.parapet.instances.parallel._

object DslInterpreterInstances {

  private type TaskQueue[F[_]] = Enqueue[F, Task[F]]

  object dslInterpreterForCatsIO {
    def ioFlowInterpreter(context: Context[IO])
                         (implicit ctx: ContextShift[IO], timer: Timer[IO]): FlowOp[IO, ?] ~> Flow[IO, ?] =
      new (FlowOp[IO, ?] ~> Flow[IO, ?]) {

        def send(e: Envelope): IO[Unit] = {
          context.taskQueue.tryEnqueue(Deliver(e)).flatMap {
            case false => context.eventLog.write(e)
            case true => IO.unit
          }
        }

        override def apply[A](fa: FlowOp[IO, A]): Flow[IO, A] = {

          val parallel: Parallel[IO] = Parallel[IO]
          val interpreter: Interpreter[IO] = ioFlowInterpreter(context)

          fa match {

            case UnitFlow() => StateT.modify(s => s)

            case Send(event, receivers) =>
              StateT.modify[IO, FlowState[IO]] { s =>
                s.add(receivers.map(receiver => send(Envelope(s.selfRef, event, receiver))).toList.sequence)
              }

            case Forward(event, receivers) =>
              StateT.modify[IO, FlowState[IO]] { s =>
                s.add(receivers.map(receiver => send(Envelope(s.senderRef, event, receiver))).toList.sequence)
              }

            case par: Par[IO, Dsl[IO, ?]]@unchecked =>
              StateT.modify[IO, FlowState[IO]] { s =>
                s.add(interpret(par.flow, interpreter, s.copy(ops = List.empty)).flatMap(parallel.par))
              }

            case delayOp: Delay[IO, Dsl[IO, ?]]@unchecked =>
              StateT.modify[IO, FlowState[IO]] { s =>
                val delayIO = IO.sleep(delayOp.duration)
                delayOp.flow match {
                  case Some(flow) =>

                    val res = interpret(flow, interpreter, s.copy(ops = List.empty))
                      .map(ops => ops.map(op => delayIO >> op)).flatMap(_.toList.sequence)
                    s.add(res)
                  case None => s.add(delayIO)
                }
              }

            case reply: WithSender[IO, Dsl[IO, ?]]@unchecked =>
              StateT.modify[IO, FlowState[IO]] { s =>
                s.add(interpret_(reply.f(s.senderRef), interpreter, s.copy(ops = List.empty)))
              }

            case invoke: Invoke[IO, Dsl[IO, ?]]@unchecked =>
              StateT.modify[IO, FlowState[IO]] { s =>
                s.add(interpret_(invoke.body, interpreter, FlowState(invoke.caller, invoke.callee)))
              }

            case fork: Fork[IO, Dsl[IO, ?]]@unchecked =>
              StateT.modify[IO, FlowState[IO]] { s =>
                s.add(interpret_(fork.flow, interpreter, s.copy(ops = List.empty)).start)
              }

            case Register(parent, process: Process[IO]) =>
              StateT.modify[IO, FlowState[IO]] { s =>
                s.add(context.register(parent, process))
              }

            case Race(firstFlow, secondFlow) =>
              StateT.modify[IO, FlowState[IO]] { s =>
                val first = interpret_(firstFlow.asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty))
                val second = interpret_(secondFlow.asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty))
                s.add(IO.race(first, second))
              }

            case suspend: Suspend[IO, Dsl[IO, ?], A]@unchecked =>
              StateT.modify[IO, FlowState[IO]] { s =>
                s.add(IO.suspend(suspend.thunk()).flatMap { a =>
                  suspend.bind.map(f => (b: A) => interpret_(f(b), interpreter, s.copy(ops = List.empty)))
                    .getOrElse((_: A) => IO.unit)(a)
                })
              }

            case eval: Eval[IO, Dsl[IO, ?], A]@unchecked =>
              StateT.modify[IO, FlowState[IO]] { s =>
                val res = IO(eval.thunk()).flatMap { a =>
                  eval.bind.map(f => (b: A) => interpret_(f(b), interpreter, s.copy(ops = List.empty)))
                    .getOrElse((_: A) => IO.unit)(a)
                }
                s.add(IO(eval.thunk()).flatMap { a =>
                  eval.bind.map(f => (b: A) => interpret_(f(b), interpreter, s.copy(ops = List.empty)))
                    .getOrElse((_: A) => IO.unit)(a)
                })
              }
          }
        }
      }

  }

}