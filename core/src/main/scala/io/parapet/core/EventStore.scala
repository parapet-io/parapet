package io.parapet.core

import com.typesafe.scalalogging.StrictLogging
import io.parapet.Envelope
import io.parapet.effect.Effect

/** Spillover sink for envelopes that the runtime cannot deliver because a process's
  * mailbox is full.
  *
  * Implementations may persist to disk, replay later, or simply log the drop. The default
  * is [[EventStore.Stub]] which logs at debug.
  */
trait EventStore[F[_]]:
  /** Persists or otherwise records a rejected envelope. */
  def write(envelope: Envelope): F[Unit]

  /** Returns previously written envelopes (for replay scenarios). */
  def read: F[Seq[Envelope]]

/** Built-in [[EventStore]] implementations. */
object EventStore:
  /** No-op [[EventStore]]. Logs each dropped envelope at debug and never returns anything
    * from [[read]]. Suitable for development and tests.
    */
  final class Stub[F[_]](using effect: Effect[F]) extends EventStore[F] with StrictLogging:
    def write(envelope: Envelope): F[Unit] =
      effect.delay(logger.debug(s"event queue is full. drop event: $envelope"))

    def read: F[Seq[Envelope]] =
      effect.pure(Seq.empty)

  /** Returns the singleton stub instance. */
  def stub[F[_]](using effect: Effect[F]): EventStore[F] =
    new Stub[F]
