package io.parapet

import java.util.UUID

final case class ProcessRef(private[parapet] val value: String):
  override def toString: String = value

object ProcessRef:
  val ParapetPrefix = "parapet"
  val SystemRef: ProcessRef = ProcessRef(s"$ParapetPrefix-system")
  val DeadLetterRef: ProcessRef = ProcessRef(s"$ParapetPrefix-deadletter")
  val SchedulerRef: ProcessRef = ProcessRef(s"$ParapetPrefix-scheduler")
  val UndefinedRef: ProcessRef = ProcessRef(s"$ParapetPrefix-undefined")
  val NoopRef: ProcessRef = ProcessRef(s"$ParapetPrefix-noop")

  def apply(): ProcessRef =
    jdkUUIDRef

  def jdkUUIDRef: ProcessRef =
    new ProcessRef(UUID.randomUUID().toString)
