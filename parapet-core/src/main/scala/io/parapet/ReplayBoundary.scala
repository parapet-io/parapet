package io.parapet

import io.parapet.core.Events

/** Marks a process whose recorded domain deliveries are not re-executed during recovery.
  *
  * Its snapshot and lifecycle are still restored normally. It must recreate children from
  * [[io.parapet.core.Events.Initialize]] or [[io.parapet.core.Events.Restored]]; external work resumes after
  * [[io.parapet.core.Events.Start]].
  */
trait ReplayBoundary
