package io.parapet

import com.typesafe.scalalogging.Logger
import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.{Context, DslInterpreter, EventStore, EventTransformer, EventTransformers, Parallel, Process, Scheduler}
import io.parapet.effect.Effect
import io.parapet.syntax.FlowSyntax
import org.slf4j.LoggerFactory

trait ParApp[F[_]] extends FlowSyntax[F]:
  protected def effectInstance: Effect[F]
  protected def parallelInstance: Parallel[F]

  protected given Effect[F] = effectInstance
  protected given Parallel[F] = parallelInstance

  type Program = io.parapet.core.Dsl.DslF[F, Unit]

  lazy val logger: Logger =
    Logger(LoggerFactory.getLogger(getClass.getCanonicalName))

  val config: ParConfig = ParConfig.default

  private val eventTransformers = EventTransformers.builder

  val eventLog: EventStore[F] = EventStore.stub

  def processes(args: Array[String]): F[Seq[Process[F]]]

  def deadLetter: F[DeadLetterProcess[F]] =
    summon[Effect[F]].pure(DeadLetterProcess.logging)

  def interpreter(context: Context[F]): Interpreter[F] =
    DslInterpreter(context)

  def unsafeRun(program: F[Unit]): Unit

  def run: F[Unit] =
    run(Array.empty)

  def eventTransformer(ref: ProcessRef, transformer: EventTransformer): Unit =
    eventTransformers.add(ref, transformer)

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

  def main(args: Array[String]): Unit =
    unsafeRun(run(args))
