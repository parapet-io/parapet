package io.parapet.core

import io.parapet.effect.{Effect, Monad}

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, LinkedBlockingQueue, TimeUnit}
import scala.concurrent.duration.FiniteDuration

/** Asynchronous FIFO queue over the effect type `F`.
  *
  * Combines an [[Queue.Enqueue]] producer side with a [[Queue.Dequeue]] consumer side. Used for process mailboxes and
  * the scheduler's signal queue.
  */
trait Queue[F[_], A] extends Queue.Enqueue[F, A] with Queue.Dequeue[F, A]

/** [[Queue]] type-class members and built-in implementations. */
object Queue:
  /** Hint describing the expected concurrency pattern. Implementations may use this to pick a more efficient backing
    * structure (e.g., SPSC ring buffer for single-producer/ single-consumer mailboxes).
    *
    * The default JDK-backed [[JdkQueue]] ignores this hint.
    */
  enum ChannelType:
    case MPMC, MPSC, SPMC, SPSC

  /** Producer-side interface. */
  trait Enqueue[F[_], A]:
    /** Suspends until `value` can be added. */
    def enqueue(value: A): F[Unit]

    /** Non-blocking attempt to add `value`; returns `false` when the queue is full. */
    def tryEnqueue(value: A): F[Boolean]

    /** Convenience: enqueues each element of `values` in order, awaiting each. */
    def enqueueAll(values: Seq[A])(using monad: Monad[F]): F[Unit] =
      Monad.sequenceDiscard(values.map(enqueue))

  /** Consumer-side interface. */
  trait Dequeue[F[_], A]:
    /** Suspends until an element is available. */
    def dequeue: F[A]

    /** Non-blocking attempt to take an element; returns `None` when empty. */
    def tryDequeue: F[Option[A]]

    /** Blocking attempt to take an element with an upper bound on the wait. Returns `None` if `timeout` elapses with no
      * element available.
      */
    def tryDequeue(timeout: FiniteDuration): F[Option[A]]

    /** Dequeues one element and pipes it through `f`. */
    def dequeueThrough[B](f: A => F[B])(using monad: Monad[F]): F[B] =
      dequeue.flatMap(f)

  /** [[Queue]] adapter over a JDK [[java.util.concurrent.BlockingQueue]]. Blocking enqueue/dequeue is dispatched
    * through [[Effect.blocking]].
    */
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

    def tryDequeue(timeout: FiniteDuration): F[Option[A]] =
      effect.blocking(Option(queue.poll(timeout.toNanos, TimeUnit.NANOSECONDS)))

  /** Bounded [[Queue]] backed by [[java.util.concurrent.ArrayBlockingQueue]]. */
  def bounded[F[_], A](capacity: Int, channelType: ChannelType = ChannelType.MPMC)(using
      effect: Effect[F]
  ): F[Queue[F, A]] =
    effect.delay(new JdkQueue[F, A](new ArrayBlockingQueue[A](capacity)))

  /** Unbounded [[Queue]] backed by [[java.util.concurrent.LinkedBlockingQueue]]. */
  def unbounded[F[_], A](channelType: ChannelType = ChannelType.MPMC)(using effect: Effect[F]): F[Queue[F, A]] =
    effect.delay(new JdkQueue[F, A](new LinkedBlockingQueue[A]()))
