package io.parapet.tests.intg.scheduler

import io.parapet.core.{ExecutionTrace, Process}
import io.parapet.core.Scheduler.Deliver
import io.parapet.{Envelope, ProcessRef}

import scala.annotation.tailrec

/** Describes how test events are distributed across receivers. */
trait WorkDistributionStrategy {

  /** Short label surfaced in logs, failure reports, etc. */
  val name: String

  /** Produces `Deliver` tasks.
    */
  def createTasks[F[_]](n: Int, processes: Array[Process[F]], numberOfSubmitters: Int): Seq[Deliver[F]]
}

object WorkDistributionStrategy {

  /** Uniformly random receiver picking (each task has an equal probability of going to any process). Produces exactly
    * `n` tasks.
    */
  object Random extends WorkDistributionStrategy {
    override val name: String = "random"

    override def createTasks[F[_]](
        n: Int,
        processes: Array[Process[F]],
        numberOfSubmitters: Int
    ): Seq[Deliver[F]] = {
      val submitters = math.max(1, numberOfSubmitters)
      val rnd        = scala.util.Random
      (1 to n).map { i =>
        val submitterId = (i - 1) % submitters // round-robin
        val process     = processes((i - 1) % processes.length) // uniformly randomly chooses the target process
        Deliver[F](
          Envelope(ProcessRef.SystemRef, TestEvent(submitterId, i), process.ref),
          ExecutionTrace.Dummy
        )
      }
    }
  }

  /** Each receiver process gets `n` consecutive events. Produces `n * processes.length` tasks. */
  object Batch extends WorkDistributionStrategy {
    override val name: String = "batch"

    override def createTasks[F[_]](
        n: Int,
        processes: Array[Process[F]],
        numberOfSubmitters: Int
    ): Seq[Deliver[F]] = {
      val submitters = math.max(1, numberOfSubmitters)

      @tailrec
      def create(i: Int, offset: Int, size: Int, tasks: Seq[Deliver[F]]): Seq[Deliver[F]] =
        if (i < processes.length) {
          create(
            i + 1,
            offset + size,
            size,
            tasks ++ (1 to size).map { j =>
              val seqNumber   = offset + j
              val submitterId = (seqNumber - 1) % submitters
              Deliver[F](
                Envelope(ProcessRef.SystemRef, TestEvent(submitterId, seqNumber), processes(i).ref),
                ExecutionTrace.Dummy
              )
            }
          )
        } else tasks

      create(0, 0, n, Seq.empty)
    }
  }

}
