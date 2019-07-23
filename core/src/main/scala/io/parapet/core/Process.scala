package io.parapet.core

import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.core.Event.{Envelope, Failure}
import io.parapet.core.Process._
import io.parapet.core.exceptions.EventMatchException
import io.parapet.syntax.EventSyntax
import io.parapet.syntax.flow._

trait Process[F[_]] extends WithDsl[F] with EventSyntax[F]{
  _self =>
  type Program = DslF[F, Unit]
 // type ReceiveF[F[_]] = PartialFunction[Event, DslF[F, Unit]]
  type Receive = PartialFunction[Event, DslF[F, Unit]]

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
      dsl.invoke(caller, handle(e), selfRef)
    } else {
      dsl.send(Failure(Envelope(caller, e, selfRef),
        EventMatchException(s"process ${_self} handler is not defined for event: $e")), caller)
    }
  }

  def apply(e: Event): Program = handle(e)

  def switch(newHandler: => Receive): Program = {
    dsl.eval {
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



}