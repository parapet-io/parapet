package io.parapet.syntax

import io.parapet.core.Dsl.{Dsl, DslF, FlowOps}
import io.parapet.core.Process
import io.parapet.{Event, ProcessRef}

/** Provides the `~>` "send to" operator for ergonomic message dispatch inside DSL programs.
  *
  * {{{
  * MyEvent ~> targetRef       // send a single event
  * Seq(a, b, c) ~> target     // send a batch in order
  * }}}
  */
trait EventSyntax[F[_]]:
  extension [E <: Event](event: E)
    /** Sends `event` to `process`. */
    def ~>(process: ProcessRef[? >: E]): DslF[F, Unit] =
      EventSyntax.send(List(event), process)

    /** Sends `event` to `process.ref`. */
    def ~>(process: Process[F, ? >: E <: Event]): DslF[F, Unit] =
      EventSyntax.send(List(event), process.ref)

  extension [E <: Event](events: Seq[E])
    /** Sends each of `events`, in order, to `process`. */
    def ~>(process: ProcessRef[? >: E]): DslF[F, Unit] =
      EventSyntax.send(events, process)

    /** Sends each of `events`, in order, to `process.ref`. */
    def ~>(process: Process[F, ? >: E]): DslF[F, Unit] =
      EventSyntax.send(events, process.ref)

/** Implementation backing [[EventSyntax]]. */
object EventSyntax:
  /** Sequential helper: sends `events` to `processRef` one after another. */
  def send[F[_], E <: Event](events: Seq[E], processRef: ProcessRef[? >: E])(using
      dsl: FlowOps[F, [x] =>> Dsl[F, x]]
  ): DslF[F, Unit] =
    events.toList match
      case Nil          => dsl.unit
      case head :: tail =>
        tail.foldLeft(dsl.send(head, processRef)) { (acc, event) =>
          acc.flatMap(_ => dsl.send(event, processRef))
        }
