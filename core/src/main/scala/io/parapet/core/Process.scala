package io.parapet.core

import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.syntax.FlowSyntax
import io.parapet.{Event, ProcessRef}

import java.util.concurrent.atomic.AtomicReference

trait Process[F[_]] extends WithDsl[F] with FlowSyntax[F]:
  self =>

  type Program = DslF[F, Unit]
  type Receive = PartialFunction[Event, DslF[F, Unit]]

  val name: String = getClass.getSimpleName
  val ref: ProcessRef = ProcessRef.jdkUUIDRef
  val bufferSize: Int = -1

  private var currentHandler: Option[Receive] = None
  private val contextRef = AtomicReference[Context[F]]()

  private[parapet] def init(context: Context[F]): Unit =
    contextRef.compareAndSet(null, context)

  private[parapet] def context: Context[F] =
    contextRef.get()

  private[core] def handler: Receive =
    currentHandler match
      case Some(value) => value
      case None =>
        val default = handle
        currentHandler = Some(default)
        default

  def handle: Receive

  def apply(event: Event): Program =
    handler(event)

  def canHandle(event: Event): Boolean =
    handler.isDefinedAt(event)

  def switch(newHandler: => Receive): Program =
    dsl.eval {
      currentHandler = Some(newHandler)
    }

  def ++[B](that: Process[F]): Process[F] =
    and(that)

  def and(that: Process[F]): Process[F] =
    new Process[F]:
      override val ref: ProcessRef = self.ref
      override val name: String = self.name

      override def handle: Receive =
        new Receive:
          def isDefinedAt(event: Event): Boolean =
            self.canHandle(event) && that.canHandle(event)

          def apply(event: Event): DslF[F, Unit] =
            self(event) ++ that(event)

  def or(that: Process[F]): Process[F] =
    new Process[F]:
      override val ref: ProcessRef = self.ref
      override val name: String = self.name

      override def handle: Receive =
        new Receive:
          def isDefinedAt(event: Event): Boolean =
            self.canHandle(event) || that.canHandle(event)

          def apply(event: Event): DslF[F, Unit] =
            if self.canHandle(event) then self(event) else that(event)

  override def toString: String =
    s"[name=$name, ref=$ref]"

object Process:
  def unit[F[_]]: Process[F] =
    new Process[F]:
      override def handle: Receive = { case _ => dsl.unit }

  def apply[F[_]](receive: ProcessRef => PartialFunction[Event, DslF[F, Unit]]): Process[F] =
    builder(receive).build

  def builder[F[_]](receive: ProcessRef => PartialFunction[Event, DslF[F, Unit]]): Builder[F] =
    Builder(receive)

  final case class Builder[F[_]](
      receive: ProcessRef => PartialFunction[Event, DslF[F, Unit]],
      private val processName: String = "undefined",
      private val processRef: ProcessRef = ProcessRef.jdkUUIDRef,
      private val processBufferSize: Int = -1
  ):
    def name(value: String): Builder[F] =
      copy(processName = value)

    def ref(value: ProcessRef): Builder[F] =
      copy(processRef = value)

    def bufferSize(value: Int): Builder[F] =
      require(value > 0 || value == -1)
      copy(processBufferSize = value)

    def build: Process[F] =
      new Process[F]:
        override val name: String = processName
        override val ref: ProcessRef = processRef
        override val bufferSize: Int = processBufferSize

        override def handle: Receive =
          receive(processRef)
