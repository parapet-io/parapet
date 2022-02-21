package io.parapet.instances

import cats.Monad
import cats.effect.{CancelToken, Concurrent, ExitCase, Fiber}

object concurrent {

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

}
