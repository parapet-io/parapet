package io.parapet.core

import java.util.UUID

import io.parapet.core.Parapet.ParapetPrefix

case class ProcessRef(private[core] val value: String) {
  override def toString: String = value
}

object ProcessRef {
  val SystemRef: ProcessRef = ProcessRef(ParapetPrefix + "-system")
  val DeadLetterRef: ProcessRef = ProcessRef(ParapetPrefix + "-deadletter")
  val SchedulerRef: ProcessRef = ProcessRef(ParapetPrefix + "-scheduler")
  val UndefinedRef: ProcessRef = ProcessRef(ParapetPrefix + "-undefined")
  val BlackHoleRef: ProcessRef = ProcessRef(ParapetPrefix + "-blackhole")

  def apply(): ProcessRef = jdkUUIDRef

  def jdkUUIDRef: ProcessRef = new ProcessRef(UUID.randomUUID().toString)
}
