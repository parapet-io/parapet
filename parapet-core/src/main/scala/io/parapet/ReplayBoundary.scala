package io.parapet

import io.parapet.Event

/** Marks a process whose recorded domain deliveries are not re-executed during recovery.
  *
  * Its snapshot and lifecycle are still restored normally. It must recreate children from
  * [[io.parapet.Event.Initialize]] or [[io.parapet.Event.Restored]]; external work resumes after
  * [[io.parapet.Event.Start]].
  */
trait ReplayBoundary
