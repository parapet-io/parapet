package io.parapet.core

import cats.implicits._
import cats.{Functor, MonadError}
import scala.reflect.runtime.universe._

// links:
// [1] http://www.cs.nott.ac.uk/~pszgmh/alacarte.pdf
// [2] http://www.cs.ru.nl/~W.Swierstra/Publications/DataTypesALaCarte.pdf

object LaCarte {

  type ErrorOr[A] = Either[String, A]

  case class Coproduct[F[_], G[_], A](run: Either[F[A], G[A]])

  sealed trait Arith[A]
  case class Val[A](x: Int) extends Arith[A]
  case class Add[A](a: A, b: A) extends Arith[A]

  sealed trait Except[A]

  case class Throw[A](error: String) extends Except[A]

  case class Catch[A](e: A, h: A) extends Except[A]


  trait <:<[F[_], G[_]] {
    def inj[A](fa: F[A]): G[A]
  }

  trait Fix[F[_]]
  case class In[F[_]](exp: F[Fix[F]]) extends Fix[F]

  type ArithOrExcept[A] = Coproduct[Arith, Except, A]

  implicit object ArithFunctor extends Functor[Arith] {
    override def map[A, B](fa: Arith[A])(f: A => B): Arith[B] = {
      fa match {
        case Val(x) => Val(x)
        case Add(a, b) => Add(f(a), f(b))
      }
    }
  }

  implicit object ExceptFunctor extends Functor[Except] {
    override def map[A, B](fa: Except[A])(f: A => B): Except[B] = {
      fa match {
        case Throw(e) => Throw(e)
        case Catch(e, h) => Catch(f(e), f(h))
      }
    }
  }

  implicit def coproductFunctor[F[_] : Functor, G[_] : Functor]: Functor[Coproduct[F, G, ?]] = new Functor[Coproduct[F, G, ?]] {
    override def map[A, B](fa: Coproduct[F, G, A])(f: A => B): Coproduct[F, G, B] = {
      fa match {
        case Coproduct(Left(value)) => Coproduct(Either.left(implicitly[Functor[F]].map(value)(f)))
        case Coproduct(Right(value)) => Coproduct(Either.right(implicitly[Functor[G]].map(value)(f)))
      }
    }
  }

  trait Eval[F[_]] {
    def evAlg(f: F[ErrorOr[Int]])(implicit functor: Functor[F]): ErrorOr[Int]
  }

  implicit object ArithEval extends Eval[Arith] {
    override def evAlg(f: Arith[ErrorOr[Int]])(implicit functor: Functor[Arith]): ErrorOr[Int] = {
      f match {
        case Val(x) => MonadError[ErrorOr, String].pure(x)
        case Add(a, b) => for {
          a1 <- a
          b1 <- b
        } yield a1 + b1
      }
    }
  }


  implicit object ExceptEval extends Eval[Except] {
    override def evAlg(f: Except[ErrorOr[Int]])(implicit functor: Functor[Except]): ErrorOr[Int] = {
      f match {
        case Throw(error) => MonadError[ErrorOr, String].raiseError(error)
        case Catch(x, h) => x >> h
      }
    }
  }

  implicit def coproductEval[G[_] : Eval : Functor, F[_] : Eval : Functor]: Eval[Coproduct[F, G, ?]] = new Eval[Coproduct[F, G, ?]] {
    override def evAlg(f: Coproduct[F, G, ErrorOr[Int]])
                      (implicit functor: Functor[Coproduct[F, G, ?]]): ErrorOr[Int] = {
      val functorF = implicitly[Eval[F]]
      val functorG = implicitly[Eval[G]]
      f match {
        case Coproduct(Left(x)) => functorF.evAlg(x)
        case Coproduct(Right(y)) => functorG.evAlg(y)
      }

    }
  }

  def fold[F[_] : Functor, A](f: F[A] => A): Fix[F] => A = {
    case In(t) =>
      val g: F[Fix[F]] => F[A] = implicitly[Functor[F]].lift(fold(f))
      f(g(t))
  }

  def eval[F[_] : Eval : Functor](f: Fix[F]): ErrorOr[Int] = fold(implicitly[Eval[F]].evAlg).apply(f)
  def getType[T: TypeTag](obj: T): Type = typeOf[T]

  def main(args: Array[String]): Unit = {
    // val 1 ‘add‘ val 2
    val three: Fix[Arith] = In(Add(In(Val(1)), In(Val(2))))
    println(eval(three)) // Right(3)

    // val 42 ‘add‘ throw
    var error: Fix[ArithOrExcept] = In(Coproduct(Either.left(Add(In(Coproduct(Either.left(Val(42)))), In(Coproduct(Either.right(Throw("unexpected error"))))))))
    println(eval(error)) // Left(unexpected error)
  }

}
