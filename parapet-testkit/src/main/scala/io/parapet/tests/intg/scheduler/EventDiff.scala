package io.parapet.tests.intg.scheduler

import io.parapet.ProcessRef
import io.parapet.core.Scheduler.Deliver
import io.parapet.tests.intg.scheduler.EventDiff
import io.parapet.tests.intg.scheduler.TestEvent
import io.parapet.testutils.EventStore

/** Structured diff between the set of events submitted to the scheduler and the events the receivers actually observed
  * via [[EventStore]].
  */
final case class EventDiff(
    missing: Seq[TestEvent],
    duplicates: Seq[(TestEvent, Int)],
    unexpected: Seq[TestEvent],
    orderingBreaks: Seq[EventDiff.OrderingBreak],
    perReceiver: Map[ProcessRef[?], EventDiff.Counts]
) {

  /** True if there is no diff.
    */
  def isClean: Boolean =
    missing.isEmpty && duplicates.isEmpty && unexpected.isEmpty && orderingBreaks.isEmpty

  def summary: String =
    s"submitted=${perReceiver.values.map(_.expected).sum} delivered=${perReceiver.values.map(_.actual).sum} " +
      s"missing=${missing.size} duplicates=${duplicates.size} unexpected=${unexpected.size} " +
      s"orderingBreaks=${orderingBreaks.size}"
}

object EventDiff {

  /** Expected vs actual counts for a single receiver. */
  final case class Counts(expected: Int, actual: Int)

  /** First observed mismatch within a `(receiver, submitterId)` sub-sequence.
    *
    * @param index
    *   zero-based offset into the sub-sequence at which expected and actual first diverge.
    * @param expected
    *   `Some(e)` when the expected stream had an event at this position; `None` when the actual stream produced an
    *   extra event the expected stream did not.
    * @param actual
    *   `Some(a)` when the actual stream had an event at this position; `None` when the expected stream had an event
    *   that never arrived.
    */
  final case class OrderingBreak(
      receiver: ProcessRef[?],
      submitterId: Int,
      index: Int,
      expected: Option[TestEvent],
      actual: Option[TestEvent]
  )

  /** Computes the full diff by combining the individual checks.
    */
  def compute[F[_]](
      tasks: Seq[Deliver[F]],
      eventStore: EventStore[F, TestEvent]
  ): EventDiff = {
    val submitted: Seq[TestEvent] = tasks.map(t => TestEvent.cast(t.envelope.event))
    val delivered: Seq[TestEvent] = eventStore.allEvents

    EventDiff(
      missing = computeMissing(submitted, delivered),
      duplicates = computeDuplicates(delivered),
      unexpected = computeUnexpected(submitted, delivered),
      orderingBreaks = computeOrderingBreaks(tasks, eventStore),
      perReceiver = computePerReceiverCounts(tasks, eventStore)
    )
  }

  /** Events the test submitted but no receiver ever observed. */
  private def computeMissing(submitted: Seq[TestEvent], delivered: Seq[TestEvent]): Seq[TestEvent] =
    submitted.filterNot(delivered.toSet)

  /** Events some receiver observed but the test never submitted to anyone. Distinct, since a single spurious event can
    * land in multiple receivers, and we only care about its identity here.
    */
  private def computeUnexpected(submitted: Seq[TestEvent], delivered: Seq[TestEvent]): Seq[TestEvent] =
    delivered.filterNot(submitted.toSet).distinct

  /** Events delivered more than once, paired with their delivery count. Sorted by `(submitterId, seqNumber)` for
    * deterministic report output.
    */
  private def computeDuplicates(delivered: Seq[TestEvent]): Seq[(TestEvent, Int)] =
    delivered
      .groupBy(identity)
      .iterator
      .collect { case (e, xs) if xs.size > 1 => e -> xs.size }
      .toSeq
      .sortBy { case (e, _) => (e.submitterId, e.seqNumber) }

  /** First per-(receiver, submitterId) FIFO mismatch on each partition where the submitted and observed sub-sequences
    * disagree. Walks the union of partitions present on either side, so events delivered to a receiver from a submitter
    * that was never expected to target it surface here too.
    */
  private def computeOrderingBreaks[F[_]](
      tasks: Seq[Deliver[F]],
      eventStore: EventStore[F, TestEvent]
  ): Seq[OrderingBreak] = {
    val submittedByPair: Map[(ProcessRef[?], Int), Seq[TestEvent]] =
      tasks
        .groupBy(t => (t.envelope.receiver, TestEvent.cast(t.envelope.event).submitterId))
        .view
        .mapValues(_.map(t => TestEvent.cast(t.envelope.event)))
        .toMap

    val observedByPair: Map[(ProcessRef[?], Int), Seq[TestEvent]] =
      eventStore.groupBy((r, e) => (r, e.submitterId))

    (submittedByPair.keySet | observedByPair.keySet).toSeq
      .sortBy { case (r, s) => (r.value, s) }
      .flatMap { case key @ (receiver, submitterId) =>
        val expected = submittedByPair.getOrElse(key, Seq.empty)
        val actual   = observedByPair.getOrElse(key, Seq.empty)
        firstMismatch(expected, actual).map { case (idx, e, a) =>
          OrderingBreak(receiver, submitterId, idx, e, a)
        }
      }
  }

  /** Per-receiver expected vs actual delivery counts */
  private def computePerReceiverCounts[F[_]](
      tasks: Seq[Deliver[F]],
      eventStore: EventStore[F, TestEvent]
  ): Map[ProcessRef[?], Counts] = {
    val submittedCountByReceiver: Map[ProcessRef[?], Int] =
      tasks.groupBy(_.envelope.receiver).view.mapValues(_.size).toMap
    submittedCountByReceiver.iterator.map { case (ref, expected) =>
      ref -> Counts(expected, eventStore.count(ref))
    }.toMap
  }

  /** Returns the first position `i` where `expected` and `actual` disagree. When one sequence is a strict prefix of the
    * other, the absent side is `None` at the divergence index.
    */
  private def firstMismatch(
      expected: Seq[TestEvent],
      actual: Seq[TestEvent]
  ): Option[(Int, Option[TestEvent], Option[TestEvent])] = {
    val e = expected.iterator
    val a = actual.iterator
    var i = 0
    while (e.hasNext && a.hasNext) {
      val en = e.next()
      val an = a.next()
      if (en != an) return Some((i, Some(en), Some(an)))
      i += 1
    }
    if (e.hasNext) Some((i, Some(e.next()), None))
    else if (a.hasNext) Some((i, None, Some(a.next())))
    else None
  }
}
