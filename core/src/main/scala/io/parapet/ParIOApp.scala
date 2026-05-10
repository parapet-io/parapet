package io.parapet

import io.parapet.effect.{Effect, ParIO, ParIORuntime}
import io.parapet.core.SchedulerRuntime

/** A [[ParApp]] specialization bound to parapet's own [[ParIO]] effect type.
  *
  * This is the simplest entry point for applications that don't need a third-party effect runtime: extend `ParIOApp`,
  * override `processes`, and run.
  *
  * {{{
  * object Main extends ParIOApp:
  *   def processes(args: Array[String]): ParIO[Seq[Process[ParIO]]] =
  *     ParIO.pure(Seq(new MyProcess))
  * }}}
  */
trait ParIOApp extends ParApp[ParIO]:
  protected def runtime: ParIORuntime = ParIO.runtime

  protected def effectInstance: Effect[ParIO]                                     = runtime.effect
  protected def parallelInstance: core.Parallel[ParIO]                            = runtime.parallel
  override private[parapet] def schedulerRuntimeInstance: SchedulerRuntime[ParIO] =
    runtime.schedulerRuntime

  /** Synchronously executes the given [[ParIO]] program by delegating to [[ParIO.unsafeRunSync]]. Blocks the calling
    * thread until the program completes.
    */
  def unsafeRun(program: ParIO[Unit]): Unit =
    require(
      runtime.config.scheduler.size >= config.schedulerConfig.numberOfWorkers,
      s"ParIO scheduler pool size (${runtime.config.scheduler.size}) must be >= scheduler workers (${config.schedulerConfig.numberOfWorkers})"
    )
    runtime.unsafeRun(program)
