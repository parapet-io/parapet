package io.parapet.core

/** Whether the runtime is re-folding recorded history or running normally.
  *
  *   - [[Live]]: normal operation. Processes receive {{Start}} event and fully ready to perform any operation.
  *   - [[Replaying]]: starts the app in recovery/replay mode: processes states are recovered from snapshots, events are
  *     replied. Some DSL operations are silenced (no-op), but it doesn't have any impact on process behavior. once a
  *     process receives {{Start}} event it can go live.
  */
enum BootMode:
  case Live, Replaying
