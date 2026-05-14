package io.parapet.core

import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.core.Events.SystemEvent
import io.parapet.syntax.FlowSyntax
import io.parapet.{Event, ProcessRef}

import java.util.concurrent.atomic.AtomicReference

/** Base trait for an actor in the parapet runtime.
  *
  * A `Process` is an isolated unit of behavior identified by a [[ProcessRef]] and driven by a single mailbox.
  * Implementations override [[handle]] to declare which [[Event]]s the process can react to and what `Dsl` program to
  * run for each. Processes never share mutable state; they communicate exclusively by sending events through the
  * runtime.
  *
  * The runtime guarantees:
  *   - [[handle]] is invoked sequentially per process - no concurrent invocations on the same instance, even across
  *     worker threads.
  *   - Each event is delivered at most once, in mailbox order.
  *   - Lifecycle events ([[io.parapet.core.Events.Start]], [[io.parapet.core.Events.Stop]]) are issued automatically by
  *     the [[Scheduler]].
  *
  * A process can dynamically swap its receive function via [[switch]], compose with another process via [[and]] /
  * [[or]] (aliases [[++]] / [[or]]), and access the [[Context]] it has been registered with via the protected `context`
  * accessor (initialized by the runtime).
  *
  * @tparam F
  *   the effect type used by the surrounding runtime.
  */
trait Process[F[_], In <: Event] extends WithDsl[F] with FlowSyntax[F]:
  self =>

  /** Convenience alias for the program type produced by [[handle]] cases. */
  type Program = DslF[F, Unit]

  /** Convenience alias for a partial function from [[Event]]s to programs - the type of value returned by [[handle]].
    */
  type Receive = PartialFunction[In | SystemEvent, DslF[F, Unit]]

  /** Human-readable label used in logs and dead-letter messages. Defaults to the simple class name; subclasses may
    * override for clarity.
    */
  val name: String = getClass.getSimpleName

  /** Stable address for this process. Override to reserve a well-known identifier (e.g. for a singleton service);
    * otherwise a fresh UUID is allocated.
    */
  val ref: ProcessRef[In] = ProcessRef.jdkUUIDRef[In]

  /** Maximum number of pending events this process will buffer, or `-1` to use the global default from
    * [[Parapet.ParConfig]]. Beyond the limit, [[Scheduler]] returns [[Scheduler.ProcessQueueIsFull]] and the sender is
    * responsible for backpressure.
    */
  val bufferSize: Int = -1

  private var currentHandler: Option[Receive] = None
  private val contextRef                      = AtomicReference[Context[F]]()

  /** Internal hook invoked by the runtime when the process is registered with a [[Context]]. Idempotent - only the
    * first call wins.
    */
  private[parapet] def init(context: Context[F]): Unit =
    contextRef.compareAndSet(null, context)

  /** Returns the [[Context]] this process was registered with. Only valid after [[init]]. */
  private[parapet] def context: Context[F] =
    contextRef.get()

  /** The currently active receive function. Lazily initialized from [[handle]] on first dispatch; subsequently swapped
    * by [[switch]].
    */
  private[core] def handler: Receive =
    currentHandler match
      case Some(value) => value
      case None        =>
        val default = handle
        currentHandler = Some(default)
        default

  /** Defines the process's reaction to incoming events.
    *
    * Implementations return a partial function pairing each handled [[Event]] with a `Dsl` program. Events that fall
    * outside the partial function's domain produce a [[io.parapet.core.Events.Failure]] which is routed to the
    * dead-letter handler unless the sender registered for failure notifications.
    */
  def handle: Receive

  /** Dispatches `event` through the active [[handler]]. Used by the runtime; user code generally shouldn't invoke this
    * directly.
    */
  def apply(event: Event): Program =
    handler.asInstanceOf[PartialFunction[Event, DslF[F, Unit]]](event)

  /** True if the active [[handler]] can handle `event`. */
  def canHandle(event: Event): Boolean =
    handler.asInstanceOf[PartialFunction[Event, DslF[F, Unit]]].isDefinedAt(event)

  /** Replaces the active receive function with `newHandler` for all subsequent events.
    *
    * The swap takes effect synchronously inside the program returned by this call, so it is safe to use mid-handler to
    * express state-machine transitions.
    */
  def switch(newHandler: => Receive): Program =
    dsl.eval {
      currentHandler = Some(newHandler)
    }

  /** Alias for [[and]]. */
  def ++[In2 <: In](that: Process[F, In2]): Process[F, In2] =
    and(that)

  /** Composes two processes so that an event is dispatched to *both* if both can handle it. Programs run sequentially
    * via `++`. The resulting process inherits this process's [[ref]] and [[name]].
    */
  def and[In2 <: In](that: Process[F, In2]): Process[F, In2] =
    new Process[F, In2]:
      override val ref: ProcessRef[In2] = self.ref
      override val name: String         = self.name

      override def handle: Receive =
        new Receive:
          def isDefinedAt(event: In2 | SystemEvent): Boolean =
            self.canHandle(event) && that.canHandle(event)

          def apply(event: In2 | SystemEvent): DslF[F, Unit] =
            self(event) ++ that(event)

  /** Composes two processes so that an event is dispatched to the first that can handle it. The resulting process
    * inherits this process's [[ref]] and [[name]].
    */
  def or[In2 <: In](that: Process[F, In2]): Process[F, In2] =
    new Process[F, In2]:
      override val ref: ProcessRef[In2] = self.ref
      override val name: String         = self.name

      override def handle: Receive =
        new Receive:
          def isDefinedAt(event: In2 | SystemEvent): Boolean =
            self.canHandle(event) || that.canHandle(event)

          def apply(event: In2 | SystemEvent): DslF[F, Unit] =
            if self.canHandle(event) then self(event) else that(event)

  override def toString: String =
    s"[name=$name, ref=$ref]"

/** Constructors and ad-hoc builders for [[Process]]. */
object Process:
  /** A no-op process that silently consumes any event. Useful as a placeholder or sink. */
  def unit[F[_]]: Process[F, Event] =
    new Process[F, Event]:
      override def handle: Receive = { case _ => dsl.unit }

  /** Builds a process inline from a function that, given the process's ref, returns a receive partial function. The
    * resulting process gets a fresh UUID ref and default name/buffer size - use [[builder]] for full control.
    */
  def apply[F[_]](
      receive: ProcessRef[Event] => PartialFunction[Event, DslF[F, Unit]]
  ): Process[F, Event] =
    builder(receive).build

  /** Builds a typed process inline from a function that accepts the process's typed ref. */
  def typed[F[_], In <: Event](
      receive: ProcessRef[In] => PartialFunction[In | SystemEvent, DslF[F, Unit]]
  ): Process[F, In] =
    typedBuilder(receive).build

  /** Returns a [[Builder]] for fluent inline process construction. */
  def builder[F[_]](
      receive: ProcessRef[Event] => PartialFunction[Event, DslF[F, Unit]]
  ): Builder[F, Event] =
    Builder(receive)

  /** Returns a typed [[Builder]] for fluent inline process construction. */
  def typedBuilder[F[_], In <: Event](
      receive: ProcessRef[In] => PartialFunction[In | SystemEvent, DslF[F, Unit]]
  ): Builder[F, In] =
    Builder(receive)

  /** Fluent constructor for inline processes. Configure the name, ref, and buffer size, then call [[build]] to
    * materialize a [[Process]].
    *
    * @param receive
    *   handler factory parameterized by the process's own ref so the body can reference its own address.
    * @param processName
    *   label for logs.
    * @param processRef
    *   address to assign - defaults to a fresh UUID.
    * @param processBufferSize
    *   mailbox capacity, or `-1` to use the global default.
    */
  final case class Builder[F[_], In <: Event](
      receive: ProcessRef[In] => PartialFunction[In | SystemEvent, DslF[F, Unit]],
      private val processName: String = "undefined",
      private val processRef: ProcessRef[In] = ProcessRef.jdkUUIDRef[In],
      private val processBufferSize: Int = -1
  ):
    /** Sets the process's display name. */
    def name(value: String): Builder[F, In] =
      copy(processName = value)

    /** Pins the process to a specific [[ProcessRef]]. */
    def ref(value: ProcessRef[In]): Builder[F, In] =
      copy(processRef = value)

    /** Sets the mailbox capacity. Must be positive or `-1` (use default). */
    def bufferSize(value: Int): Builder[F, In] =
      require(value > 0 || value == -1)
      copy(processBufferSize = value)

    /** Materializes the configured [[Process]]. */
    def build: Process[F, In] =
      new Process[F, In]:
        override val name: String        = processName
        override val ref: ProcessRef[In] = processRef
        override val bufferSize: Int     = processBufferSize

        override def handle: Receive =
          receive(processRef)
