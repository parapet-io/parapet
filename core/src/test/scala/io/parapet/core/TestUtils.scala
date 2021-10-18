package io.parapet.core

import cats.{Id, Monad, ~>}
import io.parapet.core.Dsl.{Delay, Dsl, Eval, FlowOp, Fork, Send, SuspendF, UnitFlow}
import io.parapet.{Event, ProcessRef}

import scala.collection.mutable.ListBuffer

object TestUtils {

  case class Message(e: Event, target: ProcessRef)

  class Execution(val trace: ListBuffer[Message] = ListBuffer.empty) {
    override def toString: String = trace.toString()

    def print(): Unit = {
      val margin = "=" * 20
      val title = " TRACE "
      println(trace.zipWithIndex.foldLeft(new StringBuilder("\n")
        .append(margin)
        .append(title)
        .append(margin)
        .append("\n"))
      ((b, p) => b.append(s"${p._2}: ${p._1}\n")).append(margin * 2 + "=" * title.length).toString())
    }
  }

  class IdInterpreter(val execution: Execution, mapper: Event => Event) extends (FlowOp[Id, *] ~> Id) {

    override def apply[A](fa: FlowOp[Id, A]): Id[A] = {
      fa match {
        case _: UnitFlow[Id]@unchecked => ()
        case eval: Eval[Id, Dsl[Id, *], A]@unchecked =>
          implicitly[Monad[Id]].pure(eval.thunk())
        case send: Send[Id]@unchecked =>
          val event = mapper(send.e())
          execution.trace.append(Message(event, send.receiver))
          send.receivers.foreach(p => {
            execution.trace.append(Message(event, p))
          })
        case fork: Fork[Id, Dsl[Id, *]] => fork.flow.foldMap(new IdInterpreter(execution, mapper))
        case _: Delay[Id] => ()
        case _: SuspendF[Id, Dsl[Id, *], A] => ().asInstanceOf[A] // s.thunk().foldMap(new IdInterpreter(execution))
      }
    }
  }

  object IdInterpreter {
    def apply(execution: Execution, mapper: Event => Event = e => e): IdInterpreter =
      new IdInterpreter(execution, mapper)
  }

}
