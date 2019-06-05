package io.parapet.syntax

import io.parapet.core.Event
import io.parapet.core.Parapet.{Flow, FlowF, FlowOpOrEffect, Process, ProcessRef}

trait EventSyntax {

  implicit class EventOps[F[_]](e: Event) {
    def ~>(process: ProcessRef)(implicit FL: Flow[F, FlowOpOrEffect[F, ?]]): FlowF[F, Unit] = FL.send(e, process)

    private[parapet] def ~>(process: Process[F])(implicit FL: Flow[F, FlowOpOrEffect[F, ?]]): FlowF[F, Unit] = FL.send(e, process.ref)
  }

}