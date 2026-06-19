package io.parapet.core

import io.parapet.core.Dsl.FlowOp
import io.parapet.{Event, ProcessRef}

import scala.concurrent.duration.{Duration, FiniteDuration, NANOSECONDS}
import scala.util.Random

/** An outbound event handed to send rules. */
final case class Sent(sender: ProcessRef.Unknown, to: ProcessRef.Unknown, event: Event)

/** Describes faults to inject into a run.
  *
  * Two rule phases are available:
  *   - send rules target outbound events by receiver, probability, or, with [[Builder.onSend]], event value;
  *   - operation rules apply to any operation through [[Builder.failOps]] or [[Builder.onOp]].
  *
  * Within each phase, rules are evaluated in insertion order. The first rule that returns a decision other than
  * [[Fault.Proceed]] wins.
  */
trait FaultPolicy[F[_]]:

  /** Decides whether to inject a fault into an outbound send. */
  def onSend(sent: Sent, rng: Random): Fault =
    Fault.Proceed

  /** Decides whether to inject a fault into an operation.
    *
    * For sends, this is consulted after [[onSend]] returns [[Fault.Proceed]].
    */
  def onOp[A](sender: ProcessRef.Unknown, op: FlowOp[F, A], rng: Random): Fault =
    Fault.Proceed

object FaultPolicy:

  private type SendRule     = (Sent, Random) => Fault
  private type OpRule[F[_]] = (ProcessRef.Unknown, FlowOp[F, ?], Random) => Fault

  /** A policy that never injects anything. */
  def none[F[_]]: FaultPolicy[F] = new FaultPolicy[F] {}

  /** Starts building a policy. Chain combinators, then assign the result to `faultPolicy`. */
  def apply[F[_]](): Builder[F] = new Builder[F](Vector.empty, Vector.empty)

  /** Immutable fault policy assembled from ordered rules.
    *
    * Rules run in insertion order and evaluation stops at the first result other than [[Fault.Proceed]]; a rule that
    * returns `Proceed` lets later rules run. Two consequences are worth keeping in mind:
    *   - '''order matters''': `dropSends(...).delaySends(...)` is not the same as `delaySends(...).dropSends(...)`;
    *   - '''probabilities do not partition'''. `dropSends(0.5).delaySends(0.5, ...)` yields roughly 50% dropped, 25%
    *     delayed, 25% proceeding - the second rule only sees the ~50% the first one let through.
    */
  final class Builder[F[_]] private[FaultPolicy] (
      private val sendRules: Vector[SendRule],
      private val opRules: Vector[OpRule[F]]
  ) extends FaultPolicy[F]:

    override def onSend(sent: Sent, rng: Random): Fault =
      firstFault(sendRules.iterator.map(_(sent, rng)))

    override def onOp[A](sender: ProcessRef.Unknown, op: FlowOp[F, A], rng: Random): Fault =
      firstFault(opRules.iterator.map(_(sender, op, rng)))

    /** Drops a `prob` fraction of sends matching `when` (default: all). Filter by receiver (`_.to == ref`) or event
      * (`_.event.isInstanceOf[Heartbeat]`).
      */
    def dropSends(prob: Double, when: Sent => Boolean = _ => true): Builder[F] =
      requireProbability(prob)
      addSend((sent, rng) => if when(sent) && rng.nextDouble() < prob then Fault.Drop else Fault.Proceed)

    /** Delays a `prob` fraction of sends matching `when` (default: all) by a random duration within `between`. Filter
      * by receiver (`_.to == ref`) or event (`_.event.isInstanceOf[Heartbeat]`).
      */
    def delaySends(
        prob: Double,
        between: (FiniteDuration, FiniteDuration),
        when: Sent => Boolean = _ => true
    ): Builder[F] =
      requireProbability(prob)
      requireRange(between)
      addSend((sent, rng) =>
        if when(sent) && rng.nextDouble() < prob then Fault.Delay(randomBetween(between, rng)) else Fault.Proceed
      )

    /** Targets sends by pattern-matching the event value. Non-matching sends proceed.
      *
      * {{{
      * FaultPolicy[IO]()
      *   .onSend {
      *     case Sent(_, to, _: Heartbeat) if to == peerRef => Fault.Drop
      *     case Sent(_, _, _: Login)                       => Fault.Delay(200.millis)
      *   }
      * }}}
      */
    def onSend(pf: PartialFunction[Sent, Fault]): Builder[F] =
      addSend((sent, _) => pf.applyOrElse(sent, _ => Fault.Proceed))

    /** Fails a `prob` fraction of otherwise proceeding operations with a fresh `error`. */
    def failOps(prob: Double, error: => Throwable): Builder[F] =
      requireProbability(prob)
      addOp((_, _, rng) => if rng.nextDouble() < prob then Fault.Fail(error) else Fault.Proceed)

    /** General escape hatch for operation-level faults: decide directly from the sender, op, and `rng`. The [[FlowOp]]
      * is the raw core algebra node - reach for this only when probability/event targeting is not enough.
      */
    def onOp(rule: (ProcessRef.Unknown, FlowOp[F, ?], Random) => Fault): Builder[F] =
      addOp(rule)

    private def addSend(rule: SendRule): Builder[F] = new Builder(sendRules :+ rule, opRules)

    private def addOp(rule: OpRule[F]): Builder[F] = new Builder(sendRules, opRules :+ rule)

  private def requireProbability(prob: Double): Unit =
    require(prob >= 0.0 && prob <= 1.0 && !prob.isNaN, s"probability must be between 0.0 and 1.0, got $prob")

  private def requireRange(range: (FiniteDuration, FiniteDuration)): Unit =
    val (min, max) = range
    require(min >= Duration.Zero, s"minimum delay must be non-negative, got $min")
    require(max >= min, s"maximum delay $max must be >= minimum delay $min")

  private def firstFault(faults: Iterator[Fault]): Fault =
    faults.find(_ != Fault.Proceed).getOrElse(Fault.Proceed)

  /** Returns a duration in `[min, max)`, or exactly `min` when `min == max`. */
  private def randomBetween(range: (FiniteDuration, FiniteDuration), rng: Random): FiniteDuration =
    val (min, max) = range
    val lo         = min.toNanos
    val hi         = max.toNanos
    if hi == lo then min
    else FiniteDuration(lo + (rng.nextDouble() * (hi - lo)).toLong, NANOSECONDS)
