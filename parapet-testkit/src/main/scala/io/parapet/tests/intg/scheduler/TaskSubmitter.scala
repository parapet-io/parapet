package io.parapet.tests.intg.scheduler

import io.parapet.core.Scheduler
import io.parapet.core.Scheduler.{Deliver, Task}
import io.parapet.effect.{Effect, Monad}
import io.parapet.effect.Monad.*
import io.parapet.core.Parallel

import scala.annotation.tailrec

/** Feeds tasks into a scheduler.
  */
object TaskSubmitter {

  /** Feeds `tasks` into `scheduler`. When `numberOfSubmitters <= 1` all tasks are submitted sequentially on a single
    * fiber; otherwise tasks are partitioned by `submitterId` and each partition is submitted sequentially on its own
    * fiber, with fibers running in parallel via [[Parallel.par]].
    */
  def submitAll[F[_]](
      scheduler: Scheduler[F],
      tasks: Seq[Task[F]],
      numberOfSubmitters: Int = 1
  )(using effect: Effect[F], parallel: Parallel[F]): F[Unit] = {
    def sequential(chunk: Seq[Task[F]]): F[Unit] =
      chunk
        .map(task => scheduler.submit(task).void)
        .foldLeft(effect.pure(()))(_ >> _)

    def submitterId(task: Task[F]): Int =
      task match {
        case deliver: Deliver[F] =>
          TestEvent.cast(deliver.envelope.event).submitterId
      }

    if (numberOfSubmitters <= 1) sequential(tasks)
    else {
      val chunks: Seq[Seq[Task[F]]] =
        tasks
          .groupBy(submitterId)
          .toSeq
          .sortBy { case (submitterId, _) => submitterId }
          .map { case (_, submitterTasks) => submitterTasks }

      parallel.par(chunks.map(sequential))
    }
  }

  /** Asserts that a sequence of [[TestEvent]]s is in strictly increasing `seqNumber` order.
    */
  @tailrec
  def assertEventsOrder(events: Seq[TestEvent]): Unit =
    events match {
      case x :: y :: xs =>
        require(x.seqNumber < y.seqNumber, s"incorrect order of events: ${x.seqNumber} shouldBe < ${y.seqNumber}")
        assertEventsOrder(y +: xs)
      case _ =>
    }

}
