package io.parapet.core

/** Internal runtime capability for running scheduler worker loops.
  */
private[parapet] trait SchedulerRuntime[F[_]]:
  def runSchedulerWorkers(workers: Seq[F[Unit]]): F[Unit]
