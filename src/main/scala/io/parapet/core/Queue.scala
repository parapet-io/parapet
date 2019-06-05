package io.parapet.core

import java.util.concurrent.atomic.AtomicInteger

import cats.Monad
import cats.effect.Concurrent
import cats.syntax.flatMap._
import cats.syntax.functor._

import scala.collection.mutable.ListBuffer

trait Queue[F[_], A] {

  def size: Int

  def enqueue(a: A): F[Unit]

  def enqueueAll(elements: Seq[A])(implicit M: Monad[F]): F[Unit] = elements.map(e => enqueue(e)).foldLeft(M.unit)(_ >> _)

  def dequeue: F[A]

  def tryDequeue: F[Option[A]]

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

  class FS2BasedQueue[F[_]: Concurrent, A](q: fs2.concurrent.Queue[F, A]) extends Queue[F, A] {
    val sizeRef = new AtomicInteger()
    val ct: Concurrent[F] = implicitly[Concurrent[F]]

    override def enqueue(a: A): F[Unit] = {
      ct.delay(sizeRef.incrementAndGet()) >> q.enqueue1(a)
    }

    override def dequeue: F[A] = {
      q.dequeue1 >>= (a => ct.delay(sizeRef.decrementAndGet()).map(_ => a))
    }

    override def tryDequeue: F[Option[A]] = {
      q.tryDequeue1 >>= {
        case a: Some[A] => ct.delay(sizeRef.decrementAndGet()).map(_ => a)
        case _ => ct.pure(None)
      }
    }

    /**
      * Returns approximate (best-effort) size of the queue.
      *
      * @return queue size
      */
    override def size: Int = sizeRef.get()
  }

  def bounded[F[_] : Concurrent, A](capacity: Int): F[Queue[F, A]] = {
    for {
      q <- fs2.concurrent.Queue.bounded[F, A](capacity)
    } yield new FS2BasedQueue[F, A](q)
  }

  def unbounded[F[_] : Concurrent, A]: F[Queue[F, A]] = {
    for {
      q <- fs2.concurrent.Queue.unbounded[F, A]
    } yield new FS2BasedQueue[F, A](q)
  }
}