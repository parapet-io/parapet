package io.parapet.snapshot

/** A process whose state can be captured and restored. The process owns serialization. A snapshotable process must have
  * a stable reference. If the same process gets a new reference (renamed), it will have a brand-new snapshot history of
  * snapshots, that has no reference to previous snapshots created using old reference.
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
