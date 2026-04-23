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
  extension (event: Event)
    /** Sends `event` to `process`. */
    def ~>(process: ProcessRef): DslF[F, Unit] =
      EventSyntax.send(List(event), process)

    /** Sends `event` to `process.ref`. */
    def ~>(process: Process[F]): DslF[F, Unit] =
      EventSyntax.send(List(event), process.ref)

  extension (events: Seq[Event])
    /** Sends each of `events`, in order, to `process`. */
    def ~>(process: ProcessRef): DslF[F, Unit] =
      EventSyntax.send(events, process)

    /** Sends each of `events`, in order, to `process.ref`. */
    def ~>(process: Process[F]): DslF[F, Unit] =
      EventSyntax.send(events, process.ref)

/** Implementation backing [[EventSyntax]]. */
object EventSyntax:
  /** Sequential helper: sends `events` to `processRef` one after another. */
  def send[F[_]](events: Seq[Event], processRef: ProcessRef)(using dsl: FlowOps[F, [x] =>> Dsl[F, x]]): DslF[F, Unit] =
    events.toList match
      case Nil => dsl.unit
      case head :: tail =>
        tail.foldLeft(dsl.send(head, processRef)) { (acc, event) =>
          acc.flatMap(_ => dsl.send(event, processRef))
        }
