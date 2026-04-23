package io.parapet

import com.typesafe.scalalogging.Logger
import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.{Context, DslInterpreter, EventStore, EventTransformer, EventTransformers, Parallel, Process, Scheduler}
import io.parapet.effect.Effect
import io.parapet.syntax.FlowSyntax
import org.slf4j.LoggerFactory

/** Bootstraps a parapet application on top of an arbitrary effect type `F[_]`.
  *
  * `ParApp` is the user-facing entry point: subclass it, override [[processes]] to return
  * the application's actor population, and call [[main]] (or [[run]]) to start the runtime.
  *
  * The class wires together the supporting machinery — [[Context]], [[Scheduler]],
  * [[DslInterpreter]], [[EventTransformer]] pipeline, and [[DeadLetterProcess]] — so
  * subclasses normally only need to express their domain logic.
  *
  * Concrete effect implementations are exposed via dedicated subclasses such as
  * [[ParIOApp]]. Users targeting a different effect type implement their own subclass
  * providing the [[effectInstance]] / [[parallelInstance]] capabilities and an
  * [[unsafeRun]] entry point.
  *
  * @tparam F the effect type used to express asynchronous and side-effecting computations.
  */
trait ParApp[F[_]] extends FlowSyntax[F]:
  /** The [[Effect]] type-class instance for `F`. Subclasses provide this to wire the
    * runtime against their chosen effect monad.
    */
  protected def effectInstance: Effect[F]

  /** The [[Parallel]] type-class instance for `F`, providing fork/join primitives needed
    * by the scheduler.
    */
  protected def parallelInstance: Parallel[F]

  protected given Effect[F] = effectInstance
  protected given Parallel[F] = parallelInstance

  /** Convenience alias for a process's program type — a `Dsl` computation in `F`
    * producing `Unit`. Lets subclasses write `Program` instead of the verbose `DslF[F, Unit]`.
    */
  type Program = io.parapet.core.Dsl.DslF[F, Unit]

  /** Logger bound to the concrete app's class name; available to subclasses for startup
    * diagnostics.
    */
  lazy val logger: Logger =
    Logger(LoggerFactory.getLogger(getClass.getCanonicalName))

  /** Scheduler / mailbox configuration. Override for tuning thread counts, queue sizes, etc. */
  val config: ParConfig = ParConfig.default

  private val eventTransformers = EventTransformers.builder

  /** Append-only journal of all events delivered through the runtime. The default is a
    * no-op stub; production deployments may override with a persistent or in-memory store
    * for replay/debugging.
    */
  val eventLog: EventStore[F] = EventStore.stub

  /** Returns the set of [[Process]] instances that make up the application.
    *
    * Called once at startup. The runtime wraps each process in a mailbox, registers it
    * with the [[Context]], and begins dispatching events.
    *
    * @param args command-line arguments passed to [[main]] / [[run]].
    */
  def processes(args: Array[String]): F[Seq[Process[F]]]

  /** Returns the [[DeadLetterProcess]] used for events that could not be delivered. By
    * default a logging implementation is installed; override to plug in custom behavior
    * (metrics, alerting, persistence).
    */
  def deadLetter: F[DeadLetterProcess[F]] =
    summon[Effect[F]].pure(DeadLetterProcess.logging)

  /** Constructs the [[DslInterpreter]] used to translate a process's `Dsl` program into
    * effects in `F`. Override to substitute a custom interpreter (e.g. for tracing).
    */
  def interpreter(context: Context[F]): Interpreter[F] =
    DslInterpreter(context)

  /** Synchronously executes the given `F`-program, blocking the calling thread until it
    * completes. Concrete subclasses bind this to the underlying effect runtime.
    */
  def unsafeRun(program: F[Unit]): Unit

  /** Convenience overload of [[run]] with no command-line arguments. */
  def run: F[Unit] =
    run(Array.empty)

  /** Registers a per-process [[EventTransformer]] that intercepts events on their way to
    * `ref`. Multiple transformers compose in registration order.
    */
  def eventTransformer(ref: ProcessRef, transformer: EventTransformer): Unit =
    eventTransformers.add(ref, transformer)

  /** Boots the runtime end-to-end: builds the [[Context]] and [[Scheduler]], registers
    * the user processes plus the dead-letter handler, and starts dispatching.
    *
    * The returned `F[Unit]` completes when the scheduler returns control — typically once
    * `Stop` propagates. Use [[main]] for a blocking entry point.
    *
    * @throws RuntimeException if [[processes]] returns an empty sequence.
    */
  def run(args: Array[String]): F[Unit] =
    val effect = summon[Effect[F]]

    for
      ps <- processes(args)
      _ <-
        if ps.isEmpty then
          effect.raiseError[Unit](new RuntimeException("Initialization error: at least one process must be provided"))
        else effect.pure(())
      context <- Context(config, eventLog, eventTransformers.build)
      scheduler <- Scheduler(config.schedulerConfig, context, interpreter(context))
      _ <- context.start(scheduler)
      deadLetterProcess <- deadLetter
      _ <- context.registerAll(ps.toList :+ deadLetterProcess)
      _ <- scheduler.start
    yield ()

  /** Standard JVM entry point; runs [[run]] under [[unsafeRun]]. Subclasses normally do
    * not need to override this.
    */
  def main(args: Array[String]): Unit =
    unsafeRun(run(args))
