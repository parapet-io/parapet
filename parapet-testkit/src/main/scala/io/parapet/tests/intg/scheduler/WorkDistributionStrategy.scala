package io.parapet.tests.intg.scheduler

import io.parapet.core.{ExecutionTrace, Process}
import io.parapet.core.Scheduler.Deliver
import io.parapet.{Envelope, Event, ProcessRef}

/** Describes how test events are distributed across receivers. */
trait WorkDistributionStrategy {

  /** Short label surfaced in logs, failure reports, etc. */
  val name: String

  /** Produces `Deliver` tasks.
    */
  def createTasks[F[_]](n: Int, processes: Array[Process[F, Event, Event]], numberOfSubmitters: Int): Seq[Deliver[F]]
}

object WorkDistributionStrategy {

  /** Deterministic balanced receiver picking. Cycles through the process array in order and produces exactly `n` tasks.
    */
  object RoundRobin extends WorkDistributionStrategy {
    override val name: String = "round-robin"

    override def createTasks[F[_]](
        n: Int,
        processes: Array[Process[F, Event, Event]],
        numberOfSubmitters: Int
    ): Seq[Deliver[F]] = {
      val submitters = math.max(1, numberOfSubmitters)
      (1 to n).map { i =>
        val submitterId = (i - 1) % submitters // round-robin
        val process     = processes((i - 1) % processes.length) // cycle evenly across all receivers
        Deliver[F](
          Envelope(ProcessRef.SystemRef, TestEvent(submitterId, i), process.ref),
          ExecutionTrace.Dummy
        )
      }
    }
  }

  /** Uniformly random receiver picking. Uses a fixed seed per strategy instance so a given strategy value is
    * repeatable.
    */
  final case class SeededRandom(receiverSeed: Long) extends WorkDistributionStrategy {
    override val name: String = s"random(seed=$receiverSeed)"

    override def createTasks[F[_]](
        n: Int,
        processes: Array[Process[F, Event, Event]],
        numberOfSubmitters: Int
    ): Seq[Deliver[F]] = {
      val submitters = math.max(1, numberOfSubmitters)
      val rnd        = new scala.util.Random(receiverSeed)
      (1 to n).map { i =>
        val submitterId = (i - 1) % submitters // round-robin
        val process     = processes(rnd.nextInt(processes.length)) // independently sample a receiver for each event
        Deliver[F](
          Envelope(ProcessRef.SystemRef, TestEvent(submitterId, i), process.ref),
          ExecutionTrace.Dummy
        )
      }
    }
  }

  object Random {
    def seeded(seed: Long): WorkDistributionStrategy = SeededRandom(seed)

    def apply(): WorkDistributionStrategy = seeded(scala.util.Random.nextLong())
  }

  /** Each receiver process gets `n` consecutive events. Produces `n * processes.length` tasks. */
  object Batch extends WorkDistributionStrategy {
    override val name: String = "batch"

    override def createTasks[F[_]](
        n: Int,
        processes: Array[Process[F, Event, Event]],
        numberOfSubmitters: Int
    ): Seq[Deliver[F]] = {
      val submitters = math.max(1, numberOfSubmitters)

      processes.toSeq.zipWithIndex.flatMap { case (process, processIndex) =>
        val offset = processIndex * n

        (1 to n).map { j =>
          val seqNumber   = offset + j
          val submitterId = (seqNumber - 1) % submitters // round-robin

          Deliver[F](
            Envelope(
              ProcessRef.SystemRef,
              TestEvent(submitterId, seqNumber),
              process.ref
            ),
            ExecutionTrace.Dummy
          )
        }
      }
    }
  }

}
