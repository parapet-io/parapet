package io.parapet.tests.intg.scheduler

/** Describes how per-event handler cost is distributed across a pool of receivers.
  */
sealed trait WorkloadProfile {

  /** Short label for log MDC and diagnostic output. */
  def name: String
}

object WorkloadProfile {

  /** Every receiver uses the same processing time. */
  final case class Uniform(time: TaskProcessingTime) extends WorkloadProfile {
    override val name: String = s"uniform[${time.name}]"
  }

  /** Two-group split: the first `floor(totalReceivers * fastFraction)` receivers use [[fast]]; the rest use [[slow]].
    *
    * @param fastFraction
    *   fraction of receivers in the fast group; must be in `[0, 1]`.
    * @param fast
    *   processing time for receivers in the first group.
    * @param slow
    *   processing time for receivers in the second group.
    */
  final case class TwoGroup(
      fastFraction: Double,
      fast: TaskProcessingTime,
      slow: TaskProcessingTime
  ) extends WorkloadProfile {
    require(
      fastFraction >= 0.0 && fastFraction <= 1.0,
      s"fastFraction must be in [0, 1], got $fastFraction"
    )

    override val name: String =
      f"two-group[fastFraction=$fastFraction%.2f, fast=${fast.name}, slow=${slow.name}]"
  }

  /** Convenience: every receiver uses [[TaskProcessingTime.instant]]. */
  val instant: WorkloadProfile = Uniform(TaskProcessingTime.instant)

  /** Returns the [[TaskProcessingTime]] for receiver `receiverIdx` of `totalReceivers` under `workload`. */
  def timeFor(workload: WorkloadProfile, receiverIdx: Int, totalReceivers: Int): TaskProcessingTime =
    workload match {
      case Uniform(t)                         => t
      case TwoGroup(fastFraction, fast, slow) =>
        val splitIdx = (totalReceivers * fastFraction).toInt
        if (receiverIdx < splitIdx) fast else slow
    }
}
