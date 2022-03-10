package io.parapet.core

import cats.effect.{CancelToken, Concurrent, ExitCase, Fiber}
import cats.{Id, Monad, ~>}
import io.parapet.core.Dsl.{Delay, Dsl, Eval, FlowOp, Fork, RaiseError, Send, SuspendF, UnitFlow}
import io.parapet.{Event, ProcessRef}
import cats.{Eval => CatsEval}

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
        case f: SuspendF[Id,Dsl[Id, *], A] => f.thunk().foldMap(this)
        case _: UnitFlow[Id]@unchecked => ()
        case eval: Eval[Id, Dsl[Id, *], A]@unchecked =>
          implicitly[Monad[Id]].pure(eval.thunk())
        case send: Send[Id]@unchecked =>
          val event = mapper(send.e())
          execution.trace.append(Message(event, send.receiver))
          send.receivers.foreach(p => {
            execution.trace.append(Message(event, p))
          })
        case fork: Fork[Id, Dsl[Id, *], A] =>
          val res = fork.flow.foldMap(new IdInterpreter(execution, mapper))
          new io.parapet.core.Fiber.IdFiber[A](res).asInstanceOf[A]
        case _: Delay[Id] => ()
        case _: SuspendF[Id, Dsl[Id, *], A] => ().asInstanceOf[A] // s.thunk().foldMap(new IdInterpreter(execution))
      }
    }
  }

  @deprecated("use EvalInterpreter from io.parapet.instances.interpreter")
  class EvalInterpreter(val execution: Execution, mapper: Event => Event) extends (FlowOp[CatsEval, *] ~> cats.Id) {

    override def apply[A](fa: FlowOp[CatsEval, A]): Id[A] = {
      fa match {
        case f: SuspendF[CatsEval,Dsl[CatsEval, *], A] => f.thunk().foldMap(this)
        case _: UnitFlow[CatsEval]@unchecked => ()
        case eval: Eval[CatsEval, Dsl[CatsEval, *], A]@unchecked =>
            implicitly[Monad[Id]].pure(eval.thunk())
        case send: Send[CatsEval]@unchecked =>
          val event = mapper(send.e())
          execution.trace.append(Message(event, send.receiver))
          send.receivers.foreach(p => {
            execution.trace.append(Message(event, p))
          })
        case fork: Fork[CatsEval, Dsl[CatsEval, *], A] =>
          val res = fork.flow.foldMap(new EvalInterpreter(execution, mapper))
          new io.parapet.core.Fiber.IdFiber[A](res).asInstanceOf[A]
        case _: Delay[CatsEval] => ()
        case _: SuspendF[CatsEval, Dsl[CatsEval, *], A] => ().asInstanceOf[A] // s.thunk().foldMap(new IdInterpreter(execution))
        case RaiseError(err) => throw err
      }
    }
  }


  class EvalFiber[A](a: A) extends cats.effect.Fiber[cats.Eval, A] {
    override def cancel: CancelToken[cats.Eval] = cats.Eval.Unit

    override def join: cats.Eval[A] = cats.Eval.now(a)
  }

  implicit object EvalConcurrent extends Concurrent[cats.Eval] {
    override def start[A](fa: cats.Eval[A]): cats.Eval[Fiber[cats.Eval, A]] = fa.map(new EvalFiber[A](_))

    def racePairLeft[A, B](fa: cats.Eval[A], fb: cats.Eval[B]):
    cats.Eval[Either[(A, Fiber[cats.Eval, B]), (Fiber[cats.Eval, A], B)]] =
      cats.Eval.now(Left((fa.value, new EvalFiber[B](fb.value))))

    override def racePair[A, B](fa: cats.Eval[A], fb: cats.Eval[B]):
    cats.Eval[Either[(A, Fiber[cats.Eval, B]), (Fiber[cats.Eval, A], B)]] = racePairLeft(fa, fb)

    override def async[A](k: (Either[Throwable, A] => Unit) => Unit): cats.Eval[A] = {
      var res: cats.Eval[A] = null
      k {
        case Left(err) => res = cats.Eval.later(throw err)
        case Right(value) => res = cats.Eval.now(value)
      }
      res
    }

    override def asyncF[A](k: (Either[Throwable, A] => Unit) => cats.Eval[Unit]): cats.Eval[A] = {
      var res: cats.Eval[A] = null
      k {
        case Left(err) => res = cats.Eval.later(throw err)
        case Right(value) => res = cats.Eval.now(value)
      }.value
      res
    }

    override def suspend[A](thunk: => cats.Eval[A]): cats.Eval[A] = thunk

    override def bracketCase[A, B](acquire: cats.Eval[A])
                                  (use: A => cats.Eval[B])
                                  (release: (A, ExitCase[Throwable]) => cats.Eval[Unit]): cats.Eval[B] =
      cats.Eval.defer {
        try {
          val a = acquire.value
          try {
            val b = use(a).value
            cats.Eval.now(b)
          } catch {
            case err: Throwable =>
              release(a, ExitCase.Error(err)).flatMap(_ => cats.Eval.later[B](throw err))
          }
        } catch {
          case err: Throwable => cats.Eval.later[B](throw err)
        }
      }
    override def raiseError[A](e: Throwable): cats.Eval[A] = cats.Eval.later(throw e)

    override def handleErrorWith[A](fa: cats.Eval[A])(f: Throwable => cats.Eval[A]): cats.Eval[A] = {
      try {
        cats.Eval.now(fa.value)
      } catch {
        case err: Throwable => f(err)
      }
    }

    override def flatMap[A, B](fa: cats.Eval[A])(f: A => cats.Eval[B]): cats.Eval[B] = fa.flatMap(f)

    override def tailRecM[A, B](a: A)(f: A => cats.Eval[Either[A, B]]): cats.Eval[B] =
      implicitly[Monad[cats.Eval]].tailRecM(a)(f)

    override def pure[A](x: A): cats.Eval[A] = cats.Eval.now(x)
  }

  object IdInterpreter {
    def apply(execution: Execution, mapper: Event => Event = e => e): IdInterpreter =
      new IdInterpreter(execution, mapper)
  }

  object EvalInterpreter {
    def apply(execution: Execution, mapper: Event => Event = e => e): EvalInterpreter =
      new EvalInterpreter(execution, mapper)
  }
}
