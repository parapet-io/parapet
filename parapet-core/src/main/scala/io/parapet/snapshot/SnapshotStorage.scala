package io.parapet.snapshot

import io.parapet.ProcessRef

trait SnapshotStorage[F[_]]:

  /** Persists `snapshot`. */
  def store(snapshot: Snapshot): F[Unit]

  /** The snapshot with the given id, if present. Fails on a corrupt entry. */
  def read(ref: ProcessRef.Unknown, id: Long): F[Option[Snapshot]]

  /** The latest intact snapshot of `ref`, if any. */
  def latest(ref: ProcessRef.Unknown): F[Option[Snapshot]]

  /** The latest intact snapshot of `ref` with `createdAt <= atMillis`, if any. */
  def latestBefore(ref: ProcessRef.Unknown, atMillis: Long): F[Option[Snapshot]]
