package io.parapet.core

import io.parapet.core.Context.ProcessState
import io.parapet.core.Dsl.{Delay, FlowOp, Forward, Send}
import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.effect.Effect
import io.parapet.free.{FunctionK, ~>}
import io.parapet.{ProcessRef, Scope}

import scala.util.Random

/** An [[Interpreter]] decorator that injects the faults described by a [[FaultPolicy]] before delegating to `inner`.
  */
final class FaultInjector[F[_]](inner: Interpreter[F], policy: FaultPolicy[F], seed: Long)(using effect: Effect[F])
    extends Interpreter[F]:

  private type Op[A] = FlowOp[F, A]

  private val rng = new Random(seed)

  private def faulty(sender: ProcessRef.Unknown)(nt: Op ~> F): Op ~> F =
    new FunctionK[Op, F]:
      def apply[A](op: Op[A]): F[A] =
        op match
          case s: Send[F] @unchecked =>
            val event   = s.event() // force exactly once
            val rebuilt = Send[F](() => event, s.sender, s.receiver, s.receivers).asInstanceOf[Op[A]]
            val sent    = Sent(sender, s.receiver, event)
            act(decide(policy.onSend(sent, rng), policy.onOp(sender, s, rng)), rebuilt, nt)
          case other =>
            act(policy.onOp(sender, other, rng), other, nt)

  /** First decision wins; `fallback` is by-name so its RNG draw only happens when the first decision proceeds. */
  private def decide(first: Fault, fallback: => Fault): Fault =
    if first != Fault.Proceed then first else fallback

  private def act[A](fault: Fault, op: Op[A], nt: Op ~> F): F[A] =
    fault match
      case Fault.Proceed   => nt(op)
      case Fault.Delay(d)  => effect.sleep(d).flatMap(_ => nt(op))
      case Fault.Fail(err) => effect.raiseError(err)
      case Fault.Drop      =>
        op match
          case _: Send[F] @unchecked | _: Forward[F] @unchecked | _: Delay[F] @unchecked =>
            effect.pure(()).asInstanceOf[F[A]]
          case _ =>
            nt(op) // cannot synthesize a value for a value-producing op; run it
    end match

  def interpret(sender: ProcessRef.Unknown, target: ProcessRef.Unknown): Op ~> F =
    faulty(sender)(inner.interpret(sender, target))

  def interpret(sender: ProcessRef.Unknown, target: ProcessRef.Unknown, scope: Scope): Op ~> F =
    faulty(sender)(inner.interpret(sender, target, scope))

  def interpret(sender: ProcessRef.Unknown, processState: ProcessState[F], scope: Scope): Op ~> F =
    faulty(sender)(inner.interpret(sender, processState, scope))
