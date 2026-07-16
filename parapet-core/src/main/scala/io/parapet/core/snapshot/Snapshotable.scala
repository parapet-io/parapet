package io.parapet.core.snapshot

/** A process whose state can be captured and restored. The process owns serialization.
  */
trait Snapshotable:

  /** Version of the layout produced by [[serialize]].
    */
  def schemaVersion: Long = 1L

  /** Serializes the current state.
    */
  def serialize(): Array[Byte]

  /** Restores state from the given snapshot.
    */
  def restore(snapshot: Snapshot): Unit
