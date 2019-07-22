package io.parapet.instances

import cats.data.StateT
import cats.effect.concurrent.Deferred
import cats.effect.{ContextShift, IO, Timer}
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.~>
import io.parapet.core.Dsl._
import io.parapet.core.DslInterpreter._
import io.parapet.core.Event.Envelope
import io.parapet.core.Queue.Enqueue
import io.parapet.core.Scheduler.{Deliver, Task}
import io.parapet.core.{Context, Parallel, Process}
import io.parapet.instances.parallel._


// todo refactor: replace IO.pure((s.addOps(ops), ())) with StateT.modify

object DslInterpreterInstances {

  private type TaskQueue[F[_]] = Enqueue[F, Task[F]]

  object dslInterpreterForCatsIO {
    def ioFlowInterpreter(context: Context[IO])
                         (implicit ctx: ContextShift[IO], timer: Timer[IO]): FlowOp[IO, ?] ~> Flow[IO, ?] =
      new (FlowOp[IO, ?] ~> Flow[IO, ?]) {

        override def apply[A](fa: FlowOp[IO, A]): Flow[IO, A] = {

          val parallel: Parallel[IO] = Parallel[IO]
          val interpreter: Interpreter[IO] = ioFlowInterpreter(context) or ioEffectInterpreter(context)
          fa match {
            case Empty() => StateT[IO, FlowState[IO], Unit] { s =>IO.pure((s, ())) }
            case Use(resource, f) => StateT[IO, FlowState[IO], Unit] { s =>
              val res = IO.delay(resource()) >>= (r => interpret(f(r).asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty)).flatMap(_.toList.sequence))
              IO.pure((s.addOps(List(res)), ()))
            }
            case Send(event, receivers) =>
              StateT[IO, FlowState[IO], Unit] { s =>
                val ops = receivers.map(receiver =>
                  context.taskQueue.tryEnqueue(Deliver(Envelope(s.selfRef, event, receiver))).flatMap(r =>
                    if(!r) IO.raiseError(new RuntimeException("queue is full")) else IO.unit)
                )
                IO.pure((s.addOps(ops), ()))
              }
            case Forward(event, receivers) =>
              StateT[IO, FlowState[IO], Unit] { s =>
                val ops = receivers.map(receiver => context.taskQueue.enqueue(Deliver(Envelope(s.senderRef, event, receiver))))
                IO.pure((s.addOps(ops), ()))
              }
            case Par(flows) =>
              StateT[IO, FlowState[IO], Unit] { s =>
                val res = parallel.par(
                  flows.map(flow => interpret_(flow.asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty))))
                IO.pure((s.addOps(List(res)), ()))
              }
            case io.parapet.core.Dsl.Delay(duration, Some(flow)) =>
              StateT.modifyF[IO, FlowState[IO]]{s =>
                val delayIO = IO.sleep(duration)
               interpret(flow.asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty)).map(ops =>
                  ops.map(op => delayIO >> op)).map(res => s.addOps(res))

              }
            case io.parapet.core.Dsl.Delay(duration, None) =>
              StateT.modify[IO,FlowState[IO]](s => s.addOps(List(IO.sleep(duration))))

            case Reply(f) =>
              StateT.modifyF[IO, FlowState[IO]](s  =>
                interpret(f(s.senderRef).asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty))
                  .map(a => s.addOps(a)))


            case Invoke(caller, body, callee) =>
              StateT.modify[IO, FlowState[IO]] { s =>
                  s.addOps(List(interpret_(body.asInstanceOf[DslF[IO, A]], interpreter, FlowState(caller, callee))))
              }

            case Fork(flow) =>
              StateT.modify[IO, FlowState[IO]] { s =>
                val res = interpret_(flow.asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty))

                s.addOps(List(res.start))
              }

            case Await(selector, onTimeout, timeout) =>
              StateT.modify[IO, FlowState[IO]] { s =>

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

                s.addOps(List(p))
              }
            case Register(parent, process:Process[IO]) =>
              StateT[IO, FlowState[IO], Unit] { s =>
                IO.pure((s.addOps(List(context.register(parent, process))), ()))
              }
            case Race(firstFlow, secondFlow) =>
              StateT[IO, FlowState[IO], Unit] { s =>
                val first = interpret_(firstFlow.asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty))
                val second = interpret_(secondFlow.asInstanceOf[DslF[IO, A]], interpreter, s.copy(ops = List.empty))
                IO.pure((s.addOps(List(IO.race(first, second))), ()))
              }
          }
        }
      }

    def ioEffectInterpreter(context: Context[IO])
                           (implicit ctx: ContextShift[IO], timer: Timer[IO]): Effect[IO, ?] ~> Flow[IO, ?] = new (Effect[IO, ?] ~> Flow[IO, ?]) {
      override def apply[A](fa: Effect[IO, A]): Flow[IO, A] = {
        val interpreter: Interpreter[IO] = ioFlowInterpreter(context) or ioEffectInterpreter(context)
        fa match {

          case Suspend(thunk) => StateT.modify[IO, FlowState[IO]](s => s.addOps(List(thunk())))

          case suspend: SuspendF[IO, Dsl[IO, ?], A] => StateT.modify[IO, FlowState[IO]] { s =>
            val res = suspend.thunk().flatMap { a =>
              interpret_(suspend.f(a),
                interpreter, s.copy(ops = List.empty))
            }
            s.addOps(List(res))

          }
          case eval: Eval[IO, Dsl[IO, ?], A] => StateT.modify[IO, FlowState[IO]] { s =>
            val res = IO(eval.thunk()).flatMap { a =>
              eval.bind.map(f => (b: A) => interpret_(f(b), interpreter, s.copy(ops = List.empty)))
                .getOrElse((_: A) => IO.unit)(a)
            }
            s.addOps(List(res))
          }
        }

      }
    }
  }

}