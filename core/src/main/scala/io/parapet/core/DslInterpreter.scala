package io.parapet.core


import cats.data.StateT
import cats.effect.{Concurrent, Timer}
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import cats.{Monad, ~>}
import io.parapet.core.Dsl._
import io.parapet.core.Event.Envelope
import io.parapet.core.Scheduler.{Deliver, ProcessQueueIsFull}

object DslInterpreter {

  type Flow[F[_], A] = StateT[F, FlowState[F], A]
  type Interpreter[F[_]] = Dsl[F, ?] ~> Flow[F, ?]

  case class FlowState[F[_]](senderRef: ProcessRef, selfRef: ProcessRef, ops: Vector[F[_]] = Vector.empty) {
    def addAll(that: Seq[F[_]]): FlowState[F] = this.copy(ops = ops ++ that)

    def add(op: F[_]): FlowState[F] = this.copy(ops = ops :+ op)
  }

  private[parapet] def interpret[F[_] : Monad, A](program: DslF[F, A],
                                                  interpreter: Interpreter[F],
                                                  state: FlowState[F]): F[Seq[F[_]]] = {
    program.foldMap[Flow[F, ?]](interpreter).runS(state).map(_.ops)
  }

  private[parapet] def interpret_[F[_] : Monad, A](program: DslF[F, A],
                                                   interpreter: Interpreter[F],
                                                   state: FlowState[F]): F[Unit] = {
    interpret(program, interpreter, state)
      .flatMap(s => s.fold(Monad[F].unit)(_ >> _).void)
  }

  def apply[F[_] : Concurrent : Parallel : Timer](context: Context[F]):
  Interpreter[F] = new InterpreterImpl(context)

  class InterpreterImpl[F[_] : Concurrent : Parallel : Timer](context: Context[F])
    extends Interpreter[F] {
    self =>
    private val ct = implicitly[Concurrent[F]]
    private val timer = implicitly[Timer[F]]
    private val pa = implicitly[Parallel[F]]

    def send(e: Envelope): F[Unit] = {
      // todo move to context
      context.schedule(Deliver(e)).flatMap {
        case ProcessQueueIsFull => context.eventLog.write(e)
        case _ => ct.unit
      }
    }

    override def apply[A](fa: Dsl.Dsl[F, A]): Flow[F, A] = {
      fa match {
        case UnitFlow() => StateT.modify(s => s)

        case Send(event, receivers) =>
          StateT.modify[F, FlowState[F]] { s =>
            s.add(receivers.map(receiver => send(Envelope(s.selfRef, event, receiver))).toList.sequence)
          }

        case Forward(event, receivers) =>
          StateT.modify[F, FlowState[F]] { s =>
            s.add(receivers.map(receiver => send(Envelope(s.senderRef, event, receiver))).toList.sequence)
          }

        case par: Par[F, Dsl[F, ?]]@unchecked =>
          StateT.modify[F, FlowState[F]] { s =>
            s.add(interpret(par.flow, self, FlowState[F](s.senderRef, s.selfRef)).flatMap(pa.par))
          }

        case delayOp: Delay[F, Dsl[F, ?]]@unchecked =>
          StateT.modify[F, FlowState[F]] { s =>
            val delayIO = timer.sleep(delayOp.duration)
            delayOp.flow match {
              case Some(flow) =>
                // fixme:
                // Error:(90, 80) type mismatch;
                // found   : F[_] <:< F[_]
                // required: F[_] <:< F[Any]
                // .map(ops => ops.map(op => (delayIO >> op))).flatMap(_.toList.sequence)
                val res = interpret(flow, self, FlowState[F](s.senderRef, s.selfRef))
                  .map(ops => ops.map(op => (delayIO >> op).void)).flatMap(_.toList.sequence)
                s.add(res)
              case None => s.add(delayIO)
            }
          }

        case reply: WithSender[F, Dsl[F, ?]]@unchecked =>
          StateT.modify[F, FlowState[F]] { s =>
            s.add(interpret_(reply.f(s.senderRef), self, FlowState[F](s.senderRef, s.selfRef)))
          }

        case invoke: Invoke[F, Dsl[F, ?]]@unchecked =>
          StateT.modify[F, FlowState[F]] { s =>
            s.add(interpret_(invoke.body, self, FlowState[F](invoke.caller, invoke.callee)))
          }

        case fork: Fork[F, Dsl[F, ?]]@unchecked =>
          StateT.modify[F, FlowState[F]] { s =>
            s.add(ct.start(interpret_(fork.flow, self, FlowState[F](s.senderRef, s.selfRef))))
          }

        case Register(parent, process: Process[F]) =>
          StateT.modify[F, FlowState[F]] { s =>
            s.add(context.registerAndStart(parent, process))
          }

        case Race(firstFlow, secondFlow) =>
          StateT.modify[F, FlowState[F]] { s =>
            val first = interpret_(firstFlow.asInstanceOf[DslF[F, A]], self, FlowState[F](s.senderRef, s.selfRef))
            val second = interpret_(secondFlow.asInstanceOf[DslF[F, A]], self, FlowState[F](s.senderRef, s.selfRef))
            s.add(ct.race(first, second))
          }

        case suspend: Suspend[F, Dsl[F, ?], A]@unchecked =>
          StateT.modify[F, FlowState[F]] { s =>
            s.add(ct.suspend(suspend.thunk()).flatMap { a =>
              suspend.bind.map(f => (b: A) => interpret_(f(b), self, FlowState[F](s.senderRef, s.selfRef)))
                .getOrElse((_: A) => ct.unit)(a)
            })
          }

        case eval: Eval[F, Dsl[F, ?], A]@unchecked =>
          StateT.modify[F, FlowState[F]] { s =>
            s.add(ct.delay(eval.thunk()).flatMap { a =>
              eval.bind.map(f => (b: A) => interpret_(f(b), self, FlowState[F](s.senderRef, s.selfRef)))
                .getOrElse((_: A) => ct.unit)(a)
            })
          }
      }
    }
  }

}
