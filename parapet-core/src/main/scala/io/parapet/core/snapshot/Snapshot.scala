package io.parapet.core.snapshot

import io.parapet.ProcessRef

/** Snapshot metadata.
  *
  * @param processRef
  *   the process this snapshot belongs to.
  * @param id
  *   unique snapshot id.
  * @param parentId
  *   the process's previous snapshot. `0` for a process's first snapshot.
  * @param seq
  *   position of the last delivery folded into this state: the snapshot is exact as of delivery `seq`.
  * @param createdAt
  *   creation time
  * @param schemaVersion
  *   version of the serialized state layout
  */
final case class Metadata(
    processRef: ProcessRef.Unknown,
    id: Long,
    parentId: Long,
    seq: Long,
    createdAt: Long,
    schemaVersion: Long
)

/** A captured process state.
  */
final case class Snapshot(metadata: Metadata, data: Array[Byte])
