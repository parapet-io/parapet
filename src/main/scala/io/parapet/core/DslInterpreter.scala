package io.parapet.core


import cats.data.State
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.{Monad, ~>}
import io.parapet.core.Dsl._

object DslInterpreter {


  type Flow[F[_], A] = State[FlowState[F], A]
  type Interpreter[F[_]] = Dsl[F, ?] ~> Flow[F, ?]

  case class FlowState[F[_]](senderRef: ProcessRef, selfRef: ProcessRef, ops: Seq[F[_]] = Seq.empty) {
    def addOps(that: Seq[F[_]]): FlowState[F] = this.copy(ops = ops ++ that)
  }

  private[parapet] def interpret[F[_] : Monad, A](program: DslF[F, A],
                                               interpreter: Interpreter[F],
                                               state: FlowState[F]): Seq[F[_]] = {
    program.foldMap[Flow[F, ?]](interpreter).runS(state).value.ops
  }

  private[parapet] def interpret_[F[_] : Monad, A](program: DslF[F, A],
                                                interpreter: Interpreter[F],
                                                state: FlowState[F]): F[Unit] = {
    interpret(program, interpreter, state).fold(Monad[F].unit)(_ >> _).void
  }


}
