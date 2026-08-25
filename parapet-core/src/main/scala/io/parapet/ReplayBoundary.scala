package io.parapet

import io.parapet.core.Events

/** Marks a process whose recorded domain deliveries are not re-executed during recovery.
  *
  * Its snapshot and lifecycle are still restored normally. It must recreate children from [[Events.Initialize]] or
  * [[Events.Restored]]; external work resumes after [[Events.Start]].
  */
trait ReplayBoundary
