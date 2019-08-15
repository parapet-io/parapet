package io.parapet.core

import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.core.Event.{Envelope, Failure}
import io.parapet.core.exceptions.EventMatchException
import io.parapet.syntax.FlowSyntax

trait Process[F[_]] extends WithDsl[F] with FlowSyntax[F] {
  _self =>
  type Program = DslF[F, Unit]
  type Receive = PartialFunction[Event, DslF[F, Unit]]

  val name: String = getClass.getSimpleName

  val ref: ProcessRef = ProcessRef.jdkUUIDRef

  private var _handler = Option.empty[Receive]

  private[core] def handler: Receive = {
    _handler match {
      case Some(s) => s
      case None =>
        val default = handle
        _handler = Some(default)
        default
    }
  }

  // default handler
  def handle: this.Receive

  def apply(caller: ProcessRef, e: Event): Program = {
    if (canHandle(e)) {
      dsl.invoke(caller, apply(e), ref)
    } else {
      dsl.send(Failure(Envelope(caller, e, ref),
        EventMatchException(s"process ${_self} handler is not defined for event: $e")), caller)
    }
  }

  def apply(e: Event): Program = handler(e)

  def canHandle(e: Event): Boolean = handler.isDefinedAt(e)

  def switch(newHandler: => Receive): Program = {
    dsl.eval {
      _handler = Some(newHandler)
    }
  }

  def ++[B](that: Process[F]): Process[F] = this.and(that)

  def and(that: Process[F]): Process[F] = new Process[F] {
    override val ref: ProcessRef = _self.ref
    override val name: String = _self.name

    override val handle: Receive = new Receive {

      override def isDefinedAt(x: Event): Boolean = {
        _self.canHandle(x) && that.canHandle(x)
      }

      override def apply(v1: Event): DslF[F, Unit] = {
        _self(v1) ++ that(v1)
      }
    }
  }

  def or(that: Process[F]): Process[F] = new Process[F] {
    override val ref: ProcessRef = _self.ref
    override val name: String = _self.name
    override val handle: Receive = _self.handler.orElse(that.handler)
  }

  override def toString: String = s"[name=$name, ref=$ref]"
}

object Process {

  def apply[F[_]](receive: ProcessRef => PartialFunction[Event, DslF[F, Unit]]): Process[F] =
    builder(receive).build

  def builder[F[_]](receive: ProcessRef => PartialFunction[Event, DslF[F, Unit]]): Builder[F] =
    new Builder[F](receive)


  class Builder[F[_]](
                       receive: ProcessRef => PartialFunction[Event, DslF[F, Unit]],
                       private var _name: String = "undefined",
                       private var _ref: ProcessRef = ProcessRef.jdkUUIDRef

                     ) {
    def name(value: String): Builder[F] = {
      _name = value
      this
    }

    def ref(value: ProcessRef): Builder[F] = {
      _ref = value
      this
    }

    def build: Process[F] = new Process[F] {
      override val name: String = _name
      override val ref: ProcessRef = _ref

      override def handle: Receive = receive(ref)
    }
  }


}