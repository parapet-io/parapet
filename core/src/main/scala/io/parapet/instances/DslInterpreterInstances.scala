package io.parapet.instances

import cats.data.State
import cats.effect.IO.{Delay, Suspend}
import cats.effect.concurrent.Deferred
import cats.effect.{ContextShift, IO, Timer}
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.~>
import io.parapet.core.Dsl._
import io.parapet.core.{Context, Parallel, Process, ProcessRef}
import io.parapet.core.DslInterpreter._
import io.parapet.core.Event.Envelope
import io.parapet.core.Queue.Enqueue
import io.parapet.core.Scheduler.{Deliver, Task}
import io.parapet.instances.parallel._

object DslInterpreterInstances {

  private type TaskQueue[F[_]] = Enqueue[F, Task[F]]

  object dslInterpreterForCatsIO {
    def ioFlowInterpreter(context: Context[IO])
                         (implicit ctx: ContextShift[IO], timer: Timer[IO]): FlowOp[IO, ?] ~> Flow[IO, ?] =
      new (FlowOp[IO, ?] ~> Flow[IO, ?]) {

        override def apply[A](fa: FlowOp[IO, A]): Flow[IO, A] = {

          val parallel: Parallel[IO] = Parallel[IO]
          val interpreter: Interpreter[IO] = ioFlowInterpreter(context) or ioEffectInterpreter
          fa match {
            case Empty() => State[FlowState[IO], Unit] { s => (s, ()) }
            case Use(resource, f) => State[FlowState[IO], Unit] { s =>
              val res = IO.delay(resource()) >>= (r => interpret(f(r).asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty)).toList.sequence)
              (s.addOps(List(res)), ())
            }
            case Send(event, receivers) =>
              State[FlowState[IO], Unit] { s =>
                val ops = receivers.map(receiver =>
                  context.taskQueue.tryEnqueue(Deliver(Envelope(s.selfRef, event, receiver))).flatMap(r =>
                    if(!r) IO.raiseError(new RuntimeException("queue is full")) else IO.unit)
                )
                (s.addOps(ops), ())
              }
            case Forward(event, receivers) =>
              State[FlowState[IO], Unit] { s =>
                val ops = receivers.map(receiver => context.taskQueue.enqueue(Deliver(Envelope(s.senderRef, event, receiver))))
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
            case Invoke(caller, body, callee) =>
              State[FlowState[IO], Unit] { s =>
                (
                  s.addOps(List(interpret_(body.asInstanceOf[DslF[IO, A]], interpreter, FlowState(caller, callee)))),
                  ()
                )
              }

            case Fork(flow) =>
              State[FlowState[IO], Unit] { s =>
                val res = interpret_(flow.asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty))

                (s.addOps(List(res.start)), ())
              }

            case Await(selector, onTimeout, timeout) =>
              State[FlowState[IO], Unit] { s =>

                val awaitHook = for {
                  awaitHook <- Deferred[IO, Unit]
                  token <- IO(context.eventDeliveryHooks.add(s.selfRef, selector, awaitHook))
                } yield (awaitHook, token)

                def race(token: String, d: Deferred[IO, Unit]): IO[Unit] = {
                  for {
                    r <- IO.race(d.get, IO.sleep(timeout))
                    _ <- r match {
                      case Left(_) => IO.unit // process received expected event. cancel delay
                      case Right(_) =>
                        context.eventDeliveryHooks.remove(s.selfRef, token) match {
                          case Some(hook) =>
                            interpret_(onTimeout.asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty))
                          case None => IO.unit // event was delivered after delay
                        }

                    }} yield ()
                }

                val p = awaitHook.flatMap {
                  case (d, token) => race(token, d).start
                }

                (s.addOps(List(p)), ())
              }
            case Register(parent, process:Process[IO]) =>
              State[FlowState[IO], Unit] { s =>
                (s.addOps(List(context.register(parent, process))), ())
              }
            case Race(firstFlow, secondFlow) =>
              State[FlowState[IO], Unit] { s =>
                val first = interpret_(firstFlow.asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty))
                val second = interpret_(secondFlow.asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty))
                (s.addOps(List(IO.race(first, second))), ())
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
