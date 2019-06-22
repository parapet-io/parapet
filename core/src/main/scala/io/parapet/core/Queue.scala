package io.parapet.core

import cats.Monad
import cats.effect.Concurrent
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.parapet.core.Queue.{Dequeue, Enqueue}

trait Queue[F[_], A] extends Enqueue[F, A] with Dequeue[F, A] {

  def size: F[Int]

  def isEmpty(implicit M: Monad[F]): F[Boolean] = size.map(v => v == 0)

}

object Queue {

  trait Enqueue[F[_], A] {
    def enqueue(a: A): F[Unit]

    def enqueueAll(elements: Seq[A])(implicit M: Monad[F]): F[Unit] = elements.map(e => enqueue(e)).foldLeft(M.unit)(_ >> _)

    def tryEnqueue(a: A): F[Boolean]

  }

  trait Dequeue[F[_], A] {
    def dequeue: F[A]

    def tryDequeue: F[Option[A]]

    def dequeueThrough[B](f: A => F[B])(implicit M: Monad[F]): F[B] = {
      implicitly[Monad[F]].flatMap(dequeue)(a => f(a))
    }
  }

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