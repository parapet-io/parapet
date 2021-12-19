package io.parapet.core

import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.{Event, ProcessRef}
import io.parapet.syntax.FlowSyntax

import java.util.concurrent.atomic.AtomicReference

trait Process[F[_]] extends WithDsl[F] with FlowSyntax[F] {
  _self =>
  type Program = DslF[F, Unit]
  type Receive = PartialFunction[Event, DslF[F, Unit]]

  val name: String = getClass.getSimpleName

  val ref: ProcessRef = ProcessRef.jdkUUIDRef

  private var _handler = Option.empty[Receive]

  private val _context = new AtomicReference[Context[F]]()

  private [parapet] def init(ctx: Context[F]): Unit = _context.compareAndSet(null, ctx)

  private [parapet] def context: Context[F] = _context.get()

  val bufferSize = -1 // unbounded

  private[core] def handler: Receive =
    _handler match {
      case Some(s) => s
      case None =>
        val default = handle
        _handler = Some(default)
        default
    }

  // default handler
  def handle: this.Receive

  def apply(e: Event): Program = handler(e)

  def canHandle(e: Event): Boolean = handler.isDefinedAt(e)

  def switch(newHandler: => Receive): Program =
    dsl.eval {
      _handler = Some(newHandler)
    }

  def ++[B](that: Process[F]): Process[F] = this.and(that)

  def and(that: Process[F]): Process[F] = new Process[F] {
    override val ref: ProcessRef = _self.ref
    override val name: String = _self.name

    override val handle: Receive = new Receive {
      override def isDefinedAt(x: Event): Boolean =
        _self.canHandle(x) && that.canHandle(x)

      override def apply(v1: Event): DslF[F, Unit] =
        _self(v1) ++ that(v1)
    }
  }

  def or(that: Process[F]): Process[F] = new Process[F] {
    override val ref: ProcessRef = _self.ref
    override val name: String = _self.name
    override val handle: Receive =
      new Receive {
        override def isDefinedAt(x: Event): Boolean =
          _self.canHandle(x) || that.canHandle(x)

        override def apply(x: Event): DslF[F, Unit] =
          if (_self.canHandle(x)) _self(x)
          else that(x)
      }
  }

  override def toString: String = s"[name=$name, ref=$ref]"
}

object Process {

  def unit[F[_]]: Process[F] = new Process[F] {
    override def handle: Receive = { case _ =>
      dsl.unit
    }
  }

  def apply[F[_]](receive: ProcessRef => PartialFunction[Event, DslF[F, Unit]]): Process[F] =
    builder(receive).build

  def builder[F[_]](receive: ProcessRef => PartialFunction[Event, DslF[F, Unit]]): Builder[F] =
    new Builder[F](receive)

  class Builder[F[_]](
      receive: ProcessRef => PartialFunction[Event, DslF[F, Unit]],
      private var _name: String = "undefined",
      private var _ref: ProcessRef = ProcessRef.jdkUUIDRef,
      private var _bufferSize: Int = -1,
  ) {
    def name(value: String): Builder[F] = {
      _name = value
      this
    }

    def ref(value: ProcessRef): Builder[F] = {
      _ref = value
      this
    }

    def bufferSize(value: Int): Builder[F] = {
      require(value > 0 || value == -1)
      _bufferSize = value
      this
    }

    def build: Process[F] = new Process[F] {
      override val name: String = _name
      override val ref: ProcessRef = _ref
      override val bufferSize: Int = _bufferSize

      override def handle: Receive = receive(_ref)
    }
  }

}
