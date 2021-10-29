package io.parapet.syntax

import cats.free.Free
import cats.syntax.flatMap._
import io.parapet.core.Dsl.{Dsl, DslF, FlowOps}
import io.parapet.core.Process
import io.parapet.syntax.EventSyntax._
import io.parapet.{Event, ProcessRef}

trait EventSyntax[F[_]] {

  implicit class EventOps(e: Event) {
    def ~>(process: ProcessRef): Free[Dsl[F, *], Unit] = send(List(e), process)

    def ~>(process: Process[F]): DslF[F, Unit] = send(List(e), process.ref)
  }

  implicit class EventSeqOps(events: Seq[Event]) {
    def ~>(process: ProcessRef): Free[Dsl[F, *], Unit] = send(events, process)

    def ~>(process: Process[F]): DslF[F, Unit] = send(events, process.ref)
  }

}

object EventSyntax {
  def send[F[_]](events: Seq[Event], pRef: ProcessRef)(implicit dsl: FlowOps[F, Dsl[F, ?]]): Free[Dsl[F, ?], Unit] =
    events.map(e => dsl.send(e, pRef)).reduce(_ >> _)
}
