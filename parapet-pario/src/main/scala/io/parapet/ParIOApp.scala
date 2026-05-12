package io.parapet

import io.parapet.effect.{Effect, ParIO, ParIORuntime}
import io.parapet.core.SchedulerRuntime

/** A [[ParApp]] specialization bound to the reference [[ParIO]] effect type.
  *
  * Usage:
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

  def unsafeRun(program: ParIO[Unit]): Unit =
    runtime.unsafeRun(program)
