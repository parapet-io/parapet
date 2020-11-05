package io.parapet.core

import cats.{Id, Monad, ~>}
import io.parapet.core.Dsl.{Delay, Dsl, Eval, FlowOp, Fork, Send, UnitFlow}

import scala.collection.mutable.ListBuffer

object TestUtils {

  case class Message(e: Event, target: ProcessRef)

  class Execution(val trace: ListBuffer[Message] = ListBuffer.empty) {
    override def toString: String = trace.toString()

    def print(): Unit = {
      println(trace.zipWithIndex.foldLeft(new StringBuilder("Trace:\n"))((b, p) => b.append(s"${p._2}: ${p._1}\n")).toString())
    }
  }

  class IdInterpreter(val execution: Execution) extends (FlowOp[Id, *] ~> Id) {

    override def apply[A](fa: FlowOp[Id, A]): Id[A] = {
      fa match {
        case _: UnitFlow[Id]@unchecked => ()
        case eval: Eval[Id, Dsl[Id, ?], A]@unchecked =>
          implicitly[Monad[Id]].pure(eval.thunk())
        case send: Send[Id]@unchecked =>
          println("send")
          send.receivers.foreach(p => {
            execution.trace.append(Message(send.e(), p))
          })
        case fork: Fork[Id, Dsl[Id, ?]] => fork.flow.foldMap(new IdInterpreter(execution))
        case _: Delay[Id] => ()
      }
    }
  }

  object IdInterpreter {
    def apply(execution: Execution): IdInterpreter = new IdInterpreter(execution)
  }

}
