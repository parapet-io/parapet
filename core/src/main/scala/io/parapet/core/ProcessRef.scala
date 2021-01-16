package io.parapet.core

import java.util.UUID

import io.parapet.core.Parapet.ParapetPrefix

case class ProcessRef(private[core] val ref: String) {
  override def toString: String = ref

}

object ProcessRef {
  val SystemRef: ProcessRef = ProcessRef(ParapetPrefix + "-system")
  val DeadLetterRef: ProcessRef = ProcessRef(ParapetPrefix + "-deadletter")
  val UndefinedRef: ProcessRef = ProcessRef(ParapetPrefix + "-undefined")

  def jdkUUIDRef: ProcessRef = new ProcessRef(UUID.randomUUID().toString)
}
