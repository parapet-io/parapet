package io.parapet.syntax

import io.parapet.core.Dsl.{Dsl, DslF, FlowOps}
import io.parapet.core.Process
import io.parapet.{Event, ProcessRef}

trait EventSyntax[F[_]]:
  extension (event: Event)
    def ~>(process: ProcessRef): DslF[F, Unit] =
      EventSyntax.send(List(event), process)

    def ~>(process: Process[F]): DslF[F, Unit] =
      EventSyntax.send(List(event), process.ref)

  extension (events: Seq[Event])
    def ~>(process: ProcessRef): DslF[F, Unit] =
      EventSyntax.send(events, process)

    def ~>(process: Process[F]): DslF[F, Unit] =
      EventSyntax.send(events, process.ref)

object EventSyntax:
  def send[F[_]](events: Seq[Event], processRef: ProcessRef)(using dsl: FlowOps[F, [x] =>> Dsl[F, x]]): DslF[F, Unit] =
    events.toList match
      case Nil => dsl.unit
      case head :: tail =>
        tail.foldLeft(dsl.send(head, processRef)) { (acc, event) =>
          acc.flatMap(_ => dsl.send(event, processRef))
        }
