package io.parapet.core

import io.parapet.effect.{Effect, Monad}

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, LinkedBlockingQueue}

trait Queue[F[_], A] extends Queue.Enqueue[F, A] with Queue.Dequeue[F, A]

object Queue:
  enum ChannelType:
    case MPMC, MPSC, SPMC, SPSC

  trait Enqueue[F[_], A]:
    def enqueue(value: A): F[Unit]
    def tryEnqueue(value: A): F[Boolean]

    def enqueueAll(values: Seq[A])(using monad: Monad[F]): F[Unit] =
      Monad.sequenceDiscard(values.map(enqueue))

  trait Dequeue[F[_], A]:
    def dequeue: F[A]
    def tryDequeue: F[Option[A]]

    def dequeueThrough[B](f: A => F[B])(using monad: Monad[F]): F[B] =
      dequeue.flatMap(f)

  final class JdkQueue[F[_], A](queue: BlockingQueue[A])(using effect: Effect[F]) extends Queue[F, A]:
    def enqueue(value: A): F[Unit] =
      effect.blocking {
        queue.put(value)
        ()
      }

    def tryEnqueue(value: A): F[Boolean] =
      effect.delay(queue.offer(value))

    def dequeue: F[A] =
      effect.blocking(queue.take())

    def tryDequeue: F[Option[A]] =
      effect.delay(Option(queue.poll()))

  def bounded[F[_], A](capacity: Int, channelType: ChannelType = ChannelType.MPMC)(using effect: Effect[F]): F[Queue[F, A]] =
    effect.delay(new JdkQueue[F, A](new ArrayBlockingQueue[A](capacity)))

  def unbounded[F[_], A](channelType: ChannelType = ChannelType.MPMC)(using effect: Effect[F]): F[Queue[F, A]] =
    effect.delay(new JdkQueue[F, A](new LinkedBlockingQueue[A]()))
