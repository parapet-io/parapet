package io.parapet

import io.parapet.effect.{Effect, ParIO}

trait ParIOApp extends ParApp[ParIO]:
  protected def effectInstance: Effect[ParIO] = ParIO.effect
  protected def parallelInstance: core.Parallel[ParIO] = ParIO.parallel

  def unsafeRun(program: ParIO[Unit]): Unit =
    program.unsafeRunSync()
