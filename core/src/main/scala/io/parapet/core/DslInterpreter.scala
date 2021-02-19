package io.parapet.core

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, Timer}
import cats.implicits._
import cats.~>
import io.parapet.core.Context.ProcessState
import io.parapet.core.Dsl.{
  Blocking,
  Delay,
  Dsl,
  Eval,
  FlowOp,
  Fork,
  Forward,
  Par,
  Race,
  Register,
  Send,
  Suspend,
  SuspendF,
  UnitFlow,
  WithSender,
}
import io.parapet.core.Event.Envelope
import io.parapet.core.Scheduler.{Deliver, ProcessQueueIsFull}

object DslInterpreter {

  // ugly but necessary for Channel
  private var _instance: Any = _
  private[parapet] def instance[F[_]](i : Interpreter[F]):Unit = _instance = i
  private[parapet] def instance[F[_]]: Interpreter[F] = {
    _instance.asInstanceOf[Interpreter[F]]
  }

  trait Interpreter[F[_]] {
    def interpret(sender: ProcessRef, target: ProcessRef): FlowOp[F, *] ~> F
    def interpret(sender: ProcessRef, target: ProcessRef, execTrace: ExecutionTrace): FlowOp[F, *] ~> F
    def interpret(sender: ProcessRef, ps: ProcessState[F], execTrace: ExecutionTrace): FlowOp[F, *] ~> F
  }

  def apply[F[_]: Concurrent: Timer](context: Context[F]): Interpreter[F] = new Impl(context)

  class Impl[F[_]: Concurrent: Timer](context: Context[F]) extends Interpreter[F] {
    private val ct = implicitly[Concurrent[F]]
    private val timer = implicitly[Timer[F]]

    def interpret(sender: ProcessRef, target: ProcessRef): FlowOp[F, *] ~> F = {
      interpret(sender, target, context.createTrace)
    }

    def interpret(sender: ProcessRef, target: ProcessRef, execTrace: ExecutionTrace): FlowOp[F, *] ~> F = {
      interpret(sender, context.getProcessState(target).get, execTrace)
    }

    def interpret(sender: ProcessRef, ps: ProcessState[F], execTrace: ExecutionTrace): FlowOp[F, *] ~> F =
      new (FlowOp[F, *] ~> F) {
        override def apply[A](fa: FlowOp[F, A]): F[A] =
          fa match {
            case UnitFlow() =>
              ct.unit
            //--------------------------------------------------------------
            case Send(event, receivers) =>
              receivers.map(receiver => send(ps.process.ref, event, receiver, execTrace)).toList.sequence_
            //--------------------------------------------------------------
            case reply: WithSender[F, Dsl[F, *], A] @unchecked =>
              reply.f(sender).foldMap[F](interpret(sender, ps, execTrace))
            //--------------------------------------------------------------
            case Forward(event, receivers) =>
              receivers.map(receiver => send(sender, event, receiver, execTrace)).toList.sequence_
            //--------------------------------------------------------------
            case par: Par[F, Dsl[F, *]] @unchecked =>
              par.flow.foldMap[F](interpret(sender, ps, execTrace))
            //--------------------------------------------------------------
            case fork: Fork[F, Dsl[F, *]] @unchecked =>
              ct.start(fork.flow.foldMap[F](interpret(sender, ps, execTrace))).void
            //--------------------------------------------------------------
            case delay: Delay[F] @unchecked =>
              timer.sleep(delay.duration)
            //--------------------------------------------------------------
            case eval: Eval[F, Dsl[F, *], A] @unchecked =>
              ct.delay(eval.thunk())
            //--------------------------------------------------------------
            case suspend: Suspend[F, Dsl[F, *], A] @unchecked =>
              ct.suspend(suspend.thunk())
            //--------------------------------------------------------------
            case suspend: SuspendF[F, Dsl[F, *], A] @unchecked =>
              ct.suspend(suspend.thunk().foldMap[F](interpret(sender, ps, execTrace)))
            //--------------------------------------------------------------
            case race: Race[F, Dsl[F, *], _, _] @unchecked =>
              val fa = race.first.foldMap[F](interpret(sender, ps, execTrace))
              val fb = race.second.foldMap[F](interpret(sender, ps, execTrace))
              ct.race(fa, fb)
            //--------------------------------------------------------------
            case blocking: Blocking[F, Dsl[F, *], A] @unchecked =>
              for {
                d <- Deferred[F, Unit]
                fiber <- ct.start(
                  blocking.body().foldMap[F](interpret(sender, ps, execTrace)).flatMap(_ => d.complete(())),
                )
                _ <- ps.blocking.add(fiber, d)
              } yield ()
            //--------------------------------------------------------------
            case Register(parent, process: Process[F]) =>
              context.registerAndStart(parent, process).void
            //--------------------------------------------------------------
          }
      }

    def send(sender: ProcessRef, event: () => Event, receiver: ProcessRef, execTrace: ExecutionTrace): F[Unit] =
      ct.suspend {
        val envelope = Envelope(sender, event(), receiver)
        context.addToEventLog(envelope) >>
          context.schedule(Deliver(envelope, execTrace.add(envelope.id))).flatMap {
            case ProcessQueueIsFull => context.eventStore.write(envelope)
            case _ => ct.unit
          }
      }
  }

}
