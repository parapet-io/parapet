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

  trait Interpreter[F[_]] {
    def interpret(sender: ProcessRef, ps: ProcessState[F]): FlowOp[F, *] ~> F
  }

  def apply[F[_]: Concurrent: Timer](context: Context[F]): Interpreter[F] = new Impl(context)

  class Impl[F[_]: Concurrent: Timer](context: Context[F]) extends Interpreter[F] {
    private val ct = implicitly[Concurrent[F]]
    private val timer = implicitly[Timer[F]]

    def interpret(sender: ProcessRef, ps: ProcessState[F]): FlowOp[F, *] ~> F =
      new (FlowOp[F, *] ~> F) {
        override def apply[A](fa: FlowOp[F, A]): F[A] =
          fa match {
            case UnitFlow() =>
              ct.unit
            //--------------------------------------------------------------
            case Send(event, receivers) =>
              receivers.map(receiver => send(Envelope(ps.process.ref, event(), receiver))).toList.sequence_
            //--------------------------------------------------------------
            case reply: WithSender[F, Dsl[F, *], A] @unchecked =>
              reply.f(sender).foldMap[F](interpret(sender, ps))
            //--------------------------------------------------------------
            case Forward(event, receivers) =>
              receivers.map(receiver => send(Envelope(sender, event(), receiver))).toList.sequence_
            //--------------------------------------------------------------
            case par: Par[F, Dsl[F, *]] @unchecked =>
              par.flow.foldMap[F](interpret(sender, ps))
            //--------------------------------------------------------------
            case fork: Fork[F, Dsl[F, *]] @unchecked =>
              ct.start(fork.flow.foldMap[F](interpret(sender, ps))).void
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
              ct.suspend(suspend.thunk().foldMap[F](interpret(sender, ps)))
            //--------------------------------------------------------------
            case race: Race[F, Dsl[F, *], _, _] @unchecked =>
              val fa = race.first.foldMap[F](interpret(sender, ps))
              val fb = race.second.foldMap[F](interpret(sender, ps))
              ct.race(fa, fb)
            //--------------------------------------------------------------
            case blocking: Blocking[F, Dsl[F, *], A] @unchecked =>
              for {
                d <- Deferred[F, Unit]
                fiber <- ct.start(blocking.body().foldMap[F](interpret(sender, ps)).flatMap(_ => d.complete(())))
                _ <- ps.blocking.add(fiber, d)
              } yield ()
            //--------------------------------------------------------------
            case Register(parent, process: Process[F]) =>
              context.registerAndStart(parent, process).void
            //--------------------------------------------------------------
          }
      }

    def send(e: Envelope): F[Unit] =
      context.record(e) >>
        // todo move to context
        context.schedule(Deliver(e)).flatMap {
          case ProcessQueueIsFull => context.eventLog.write(e)
          case _ => ct.unit
        }
  }

}
