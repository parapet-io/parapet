package io.parapet.core

import cats.Monad

import scala.annotation.tailrec

sealed trait Effect[A] {

  def get: A

  def isBlocking: Boolean

  @inline final def flatMap[B](f: A => Effect[B]): Effect[B] = f(this.get)

  @inline final def map[B](f: A => B): Effect[B] =
    if (isBlocking) Effect.blocking(f(this.get)) else Effect(f(this.get))
}

object Effect {

  def apply[A](a: A): Effect[A] = NonBlocking(a)

  def blocking[A](a: A): Effect[A] = Blocking(a)

  case class Blocking[A](value: A) extends Effect[A] {
    override def isBlocking: Boolean = true

    override def get: A = value
  }

  case class NonBlocking[A](value: A) extends Effect[A] {
    override def isBlocking: Boolean = false

    override def get: A = value
  }

  implicit val flatMapForEffect: Monad[Effect] = new Monad[Effect] {
    override def flatMap[A, B](fa: Effect[A])(f: A => Effect[B]): Effect[B] = fa.flatMap(f)


    @tailrec
    def tailRecM[A, B](a: A)(f: A => Effect[Either[A, B]]): Effect[B] = {
      val fa = f(a)
      fa.get match {
        case Left(a1) => tailRecM(a1)(f)
        case Right(b) => if (fa.isBlocking) Effect.blocking(b) else Effect(b)
      }
    }

    override def map[A, B](fa: Effect[A])(f: A => B): Effect[B] = fa.map(f)

    override def pure[A](x: A): Effect[A] = Effect(x)
  }
  

}