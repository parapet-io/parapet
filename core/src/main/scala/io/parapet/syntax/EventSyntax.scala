package io.parapet.syntax

import cats.free.Free
import cats.syntax.flatMap._
import io.parapet.core.Dsl.{Dsl, DslF, FlowOps}
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.syntax.EventSyntax._

trait EventSyntax {

  implicit class EventOps[F[_]](e: Event) {
    def ~>(process: ProcessRef): Free[Dsl[F, ?], Unit] = send(List(e), process)

    private[parapet] def ~>(process: Process[F]): DslF[F, Unit] = send(List(e), process.selfRef)
  }

  implicit class EventSeqOps[F[_]](events: Seq[Event]) {
    def ~>(process: ProcessRef): Free[Dsl[F, ?], Unit] = send(events, process)

    private[parapet] def ~>(process: Process[F]): DslF[F, Unit] = send(events, process.selfRef)
  }

}

object EventSyntax {
  def send[F[_]](events: Seq[Event], pRef: ProcessRef): Free[Dsl[F, ?], Unit] = {
    val flowDsl = implicitly[FlowOps[F, Dsl[F, ?]]]
    events.map(e => flowDsl.send(e, pRef)).foldLeft(flowDsl.empty)(_ >> _)
  }
}