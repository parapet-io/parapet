package io.parapet.core

/** Whether the runtime is re-folding recorded history or running normally.
  *
  *   - [[Live]]: normal operation; handlers' outward effects take effect.
  *   - [[Replaying]]: recovery/replay is driving recorded deliveries; the interpreter suppresses outward effects
  *     (sends, timers) so recorded history is not re-emitted. Structural operations (process registration) still run.
  */
enum BootMode:
  case Live, Replaying
