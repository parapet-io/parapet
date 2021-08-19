package io.parapet.core

import cats.effect.Concurrent
import com.typesafe.scalalogging.StrictLogging

/** Persistent storage for events that were rejected due to backpressure.
  *
  * @tparam F effect
  */
trait EventStore[F[_]] {

  def write(e: Envelope): F[Unit]

  def read: F[Seq[Envelope]]

}

object EventStore {

  class Stub[F[_]: Concurrent] extends EventStore[F] with StrictLogging {

    override def write(e: Envelope): F[Unit] =
      implicitly[Concurrent[F]].delay(logger.debug(s"event queue is full. drop event: $e"))

    override def read: F[Seq[Envelope]] = implicitly[Concurrent[F]].pure(Seq.empty)
  }

  def stub[F[_]: Concurrent]: EventStore[F] = new Stub()

}
