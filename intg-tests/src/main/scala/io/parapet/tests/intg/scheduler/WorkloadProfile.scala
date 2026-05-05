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

  /** Two-group split: the first `floor(totalReceivers * fraction)` receivers use [[first]]; the rest use [[second]].
    *
    * @param fraction
    *   fraction of receivers in the first group; must be in `[0, 1]`.
    * @param first
    *   processing time for receivers in the first group.
    * @param second
    *   processing time for receivers in the second group.
    */
  final case class TwoGroup(
      fraction: Double,
      first: TaskProcessingTime,
      second: TaskProcessingTime
  ) extends WorkloadProfile {
    require(
      fraction >= 0.0 && fraction <= 1.0,
      s"fraction must be in [0, 1], got $fraction"
    )

    override val name: String =
      f"two-group[fraction=$fraction%.2f, first=${first.name}, second=${second.name}]"
  }

  /** Convenience: every receiver uses [[TaskProcessingTime.instant]]. */
  val instant: WorkloadProfile = Uniform(TaskProcessingTime.instant)

  /** Returns the [[TaskProcessingTime]] for receiver `receiverIdx` of `totalReceivers` under `workload`. */
  def timeFor(workload: WorkloadProfile, receiverIdx: Int, totalReceivers: Int): TaskProcessingTime =
    workload match {
      case Uniform(t)                        => t
      case TwoGroup(fraction, first, second) =>
        val splitIdx = (totalReceivers * fraction).toInt
        if (receiverIdx < splitIdx) first else second
    }
}
