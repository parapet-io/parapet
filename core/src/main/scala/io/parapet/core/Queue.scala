package io.parapet.core

import cats.Monad
import cats.effect.{Concurrent, ContextShift}
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.parapet.core.Queue.{Dequeue, Enqueue}
import monix.catnap.ConcurrentQueue
import monix.execution.BufferCapacity.{Bounded, Unbounded}

trait Queue[F[_], A] extends Enqueue[F, A] with Dequeue[F, A] {

  def peek: F[A]

  def size: F[Int]

  def isEmpty(implicit M: Monad[F]): F[Boolean] = size.map(v => v == 0)

}

object Queue {

  sealed trait ChannelType
  object ChannelType {
    case object MPMC extends ChannelType
    case object MPSC extends ChannelType
    case object SPMC extends ChannelType
    case object SPSC extends ChannelType
  }

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

  class MonixBasedQueue[F[_] : Concurrent, A](q: ConcurrentQueue[F, A]) extends Queue[F, A] {
    val ct: Concurrent[F] = implicitly[Concurrent[F]]

    override def peek: F[A] = ct.raiseError(new UnsupportedOperationException("peek is not supported"))

    override def size: F[Int] = ct.raiseError(new UnsupportedOperationException("size is not supported"))

    override def dequeue: F[A] = q.poll

    override def tryDequeue: F[Option[A]] = q.tryPoll

    override def enqueue(a: A): F[Unit] = q.offer(a)

    override def tryEnqueue(a: A): F[Boolean] = q.tryOffer(a)
  }

  object MonixBasedQueue {

    def toMonix(ct: ChannelType): monix.execution.ChannelType = {
      ct match {
        case ChannelType.MPMC => monix.execution.ChannelType.MPMC
        case ChannelType.MPSC => monix.execution.ChannelType.MPSC
        case ChannelType.SPMC => monix.execution.ChannelType.SPMC
        case ChannelType.SPSC => monix.execution.ChannelType.SPSC
      }
    }

    def bounded[F[_] : Concurrent : ContextShift, A](capacity: Int, channelType: ChannelType): F[Queue[F, A]] = {
      for {
        q <- ConcurrentQueue[F].withConfig[A](
          capacity = Bounded(capacity),
          channelType = toMonix(channelType)
        )
      } yield new MonixBasedQueue[F, A](q)
    }

    def unbounded[F[_] : Concurrent : ContextShift, A](channelType: ChannelType): F[Queue[F, A]] = {
      for {
        q <- ConcurrentQueue[F].withConfig[A](
          capacity = Unbounded(),
          channelType = toMonix(channelType)
        )
      } yield new MonixBasedQueue[F, A](q)
    }
  }


  def bounded[F[_] : Concurrent : ContextShift, A](capacity: Int,
                                                   channelType: ChannelType = ChannelType.MPMC): F[Queue[F, A]] =
    MonixBasedQueue.bounded(capacity, channelType)

  def unbounded[F[_] : Concurrent : ContextShift, A](channelType: ChannelType = ChannelType.MPMC): F[Queue[F, A]] =
    MonixBasedQueue.unbounded(channelType)

}