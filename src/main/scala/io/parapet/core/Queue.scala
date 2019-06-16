package io.parapet.core

import cats.Monad
import cats.effect.Concurrent
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.parapet.syntax.EffectOps
import io.parapet.syntax.boolean._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

trait Queue[F[_], A] {

  def size: F[Int]

  def isEmpty(implicit M:Monad[F]): F[Boolean] = size.map(v => v == 0)

  def enqueue(a: A): F[Unit]

  def enqueueAll(elements: Seq[A])(implicit M: Monad[F]): F[Unit] = elements.map(e => enqueue(e)).foldLeft(M.unit)(_ >> _)

  def tryEnqueue(a: A): F[Boolean]

  def tryEnqueue(a: A, duration: FiniteDuration)(implicit ct: Concurrent[F]): F[Boolean] = {
    EffectOps.retryWithTimeout(tryEnqueue(a).map(_.toOption(())), duration.fromNow).map(_.isDefined)
  }

  def dequeue: F[A]

  def tryDequeue: F[Option[A]]

  def tryDequeue(duration: FiniteDuration)(implicit ct: Concurrent[F]): F[Option[A]] = {
    EffectOps.retryWithTimeout(tryDequeue, duration.fromNow)
  }

  def dequeueThrough[F1[x] >: F[x] : Monad, B](f: A => F1[B]): F1[B] = {
    implicitly[Monad[F1]].flatMap(dequeue)(a => f(a))
  }

  // returns a tuple where Element 2 contains elements that match the given predicate
  def partition(p: A => Boolean)(implicit M: Monad[F]): F[(Seq[A], Seq[A])] = {
    def partition(left: Seq[A], right: Seq[A]): F[(Seq[A], Seq[A])] = {
      tryDequeue >>= {
        case Some(a) => if (p(a)) partition(left, right :+ a) else partition(left :+ a, right)
        case None => M.pure((left, right))
      }
    }

    partition(ListBuffer(), ListBuffer())
  }
}

object Queue {

  class FS2BasedQueue[F[_] : Concurrent, A](q: fs2.concurrent.InspectableQueue[F, A]) extends Queue[F, A] {

    val ct: Concurrent[F] = implicitly[Concurrent[F]]

    override def enqueue(a: A): F[Unit] = q.enqueue1(a)

    override def tryEnqueue(a: A): F[Boolean] = q.offer1(a)

    override def dequeue: F[A] = q.dequeue1

    override def tryDequeue: F[Option[A]] = q.tryDequeue1

    /**
      * Returns approximate (best-effort) size of the queue.
      *
      * @return queue size
      */
    override def size: F[Int] = q.getSize

  }

  def bounded[F[_] : Concurrent, A](capacity: Int): F[Queue[F, A]] = {
    for {
      q <- fs2.concurrent.InspectableQueue.bounded[F, A](capacity)
    } yield new FS2BasedQueue[F, A](q)
  }

  def unbounded[F[_] : Concurrent, A]: F[Queue[F, A]] = {
    for {
      q <- fs2.concurrent.InspectableQueue.unbounded[F, A]
    } yield new FS2BasedQueue[F, A](q)
  }

}