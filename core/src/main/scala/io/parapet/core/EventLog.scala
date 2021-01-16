package io.parapet.core

import cats.effect.Concurrent
import com.typesafe.scalalogging.StrictLogging
import io.parapet.core.Event.Envelope

trait EventLog[F[_]] {

  def write(e: Envelope): F[Unit]

  def read: F[Seq[Envelope]]

}

object EventLog {

  class Stub[F[_]: Concurrent] extends EventLog[F] with StrictLogging {

    override def write(e: Envelope): F[Unit] =
      implicitly[Concurrent[F]].delay(logger.debug(s"event queue is full. drop event: $e"))

    override def read: F[Seq[Envelope]] = implicitly[Concurrent[F]].pure(Seq.empty)
  }

  def stub[F[_]: Concurrent]: EventLog[F] = new Stub()

}
