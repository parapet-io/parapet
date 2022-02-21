package io.parapet.spark

import io.parapet.core.Dsl.FlowOps
import io.parapet.core.Events
import io.parapet.spark.EventMapper._
import io.parapet.{Event, ProcessRef}

class EventMapper[F[_]](sink: ProcessRef, mapper: Mapper) extends io.parapet.core.Process[F] {

  override def handle: Receive = {
    case Events.Start | Events.Stop => dsl.unit
    case e => if (mapper.isDefinedAt(e)) mapper.apply(e) ~> sink else e ~> sink
  }
}

object EventMapper {
  type Mapper = PartialFunction[Event, Event]

  def apply[F[_] : FlowOps.Aux](sink: ProcessRef, mapper: Mapper): EventMapper[F] = new EventMapper[F](sink, mapper)
}
