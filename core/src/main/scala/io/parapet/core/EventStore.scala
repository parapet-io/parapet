package io.parapet.core

import com.typesafe.scalalogging.StrictLogging
import io.parapet.Envelope
import io.parapet.effect.Effect

trait EventStore[F[_]]:
  def write(envelope: Envelope): F[Unit]
  def read: F[Seq[Envelope]]

object EventStore:
  final class Stub[F[_]](using effect: Effect[F]) extends EventStore[F] with StrictLogging:
    def write(envelope: Envelope): F[Unit] =
      effect.delay(logger.debug(s"event queue is full. drop event: $envelope"))

    def read: F[Seq[Envelope]] =
      effect.pure(Seq.empty)

  def stub[F[_]](using effect: Effect[F]): EventStore[F] =
    new Stub[F]
