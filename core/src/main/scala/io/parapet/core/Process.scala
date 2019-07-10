package io.parapet.core

import io.parapet.core.Dsl.{Dsl, DslF, Effects, FlowOps}
import io.parapet.core.Event.{Envelope, Failure}
import io.parapet.core.Process._
import io.parapet.core.exceptions.EventMatchException
import io.parapet.syntax.flow._

trait Process[F[_]] {
  _self =>
  type Program = DslF[F, Unit]
  type Receive = ReceiveF[F]

  protected val flowDsl: FlowOps[F, Dsl[F, ?]] = implicitly[FlowOps[F, Dsl[F, ?]]]
  protected val effectDsl: Effects[F, Dsl[F, ?]] = implicitly[Effects[F, Dsl[F, ?]]]

  val name: String = getClass.getSimpleName

  // todo add function: ref
  val selfRef: ProcessRef = ProcessRef.jdkUUIDRef

  private var state = Option.empty[Receive]

  private[core] def execute: Receive = {
    state match {
      case Some(s) => s
      case None =>
        val default = handle
        state = Some(default)
        default
    }
  }

  def handle: this.Receive

  def apply(e: Event, caller: ProcessRef): Program = {
    if (handle.isDefinedAt(e)) {
      flowDsl.invoke(caller, handle(e), selfRef)
    } else {
      flowDsl.send(Failure(Envelope(caller, e, selfRef),
        EventMatchException(s"process ${_self} handler is not defined for event: $e")), caller)
    }
  }

  def apply(e: Event): Program = handle(e)

  def switch(newHandler: => Receive): Program = {
    effectDsl.eval {
      state = Some(newHandler)
    }
  }

  def and(that: Process[F]): Process[F] = new Process[F] {
    override val selfRef: ProcessRef = _self.selfRef
    override val name: String = _self.name

    override val handle: Receive = new Receive {

      override def isDefinedAt(x: Event): Boolean = {
        _self.execute.isDefinedAt(x) && that.execute.isDefinedAt(x)
      }

      override def apply(v1: Event): DslF[F, Unit] = {
        _self.execute(v1) ++ that.execute(v1)
      }
    }
  }

  def or(that: Process[F]): Process[F] = new Process[F] {
    override val selfRef: ProcessRef = _self.selfRef
    override val name: String = _self.name
    override val handle: Receive = _self.execute.orElse(that.execute)
  }

  override def toString: String = s"[name=$name, ref=$selfRef]"
}

object Process {

  type ReceiveF[F[_]] = PartialFunction[Event, DslF[F, Unit]]

}