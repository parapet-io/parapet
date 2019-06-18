package io.parapet.syntax

import cats.free.Free
import io.parapet.core.Dsl.{Dsl, DslF, FlowOps}
import io.parapet.core.{Event, Process, ProcessRef}

trait EventSyntax {

  implicit class EventOps[F[_]](e: Event) {
    def ~>(process: ProcessRef)(implicit FL: FlowOps[F, Dsl[F, ?]]): Free[Dsl[F, ?], Unit] = FL.send(e, process)

    private[parapet] def ~>(process: Process[F])(implicit FL: FlowOps[F, Dsl[F, ?]]): DslF[F, Unit] = FL.send(e, process.ref)
  }

}