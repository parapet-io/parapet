package io.parapet.journal.log

import io.parapet.Codec
import io.parapet.effect.Clock
import io.parapet.journal.{HighWaterStore, JournalEntry}
import io.parapet.journal.HighWaterStore.HighWater
import io.parapet.journal.log.DeliveryLog.*
import io.parapet.journal.log.Log.Config

/** The log of recorded deliveries. Entries must be appended in increasing sequence order; the log does not check it.
  *
  * A deleted segment takes with it the proof of the sequences and envelope IDs it held, so [[truncate]] stores them in
  * `highWaterStore` first, and [[highWater]] counts them.
  */
class DeliveryLog(config: Config, highWaterStore: HighWaterStore, clock: Clock = Clock())
    extends Log[JournalEntry, Header, Footer, State](config, clock) {

  override protected def emptyState: State = State(minSeq = Long.MaxValue, maxSeq = 0, maxId = 0)

  override protected def createHeader(state: State): Header = Header(id = state.maxId, seq = state.minSeq)

  override protected def updateState(state: State, entry: JournalEntry): State =
    State(
      minSeq = Math.min(state.minSeq, entry.seq),
      maxSeq = Math.max(state.maxSeq, entry.seq),
      maxId = Math.max(Math.max(state.maxId, entry.id), entry.cause)
    )

  override protected def createFooter(state: State): Footer =
    Footer(minSeq = state.minSeq, maxSeq = state.maxSeq, maxId = state.maxId)

  override protected def lookupCodec(version: Int): Option[LogCodec[JournalEntry, Header, Footer]] =
    version match {
      case 1 => Option(Codecs)
      case _ => Option.empty
    }

  /** The entries with a sequence above `afterSeq`, in the order they were appended: those of the sealed segments, then
    * those of the active one.
    */
  def read(afterSeq: Long): Vector[JournalEntry] = {
    val sealedEntries = readFooters().iterator.collect {
      case (path, segment) if segment.footer.maxSeq > afterSeq => readSegment(path).get.entries
    }.flatten
    (sealedEntries ++ readActive()).filter(_.seq > afterSeq).toVector
  }

  /** The highest delivery sequence and envelope ID ever recorded, deleted entries included, or None when no entry was
    * ever recorded. It is read from storage: the stored high water, the sealed footers and the active segment.
    */
  def highWater(): Option[HighWater] = {
    val sealedSegments = readFooters().map { case (_, segment) =>
      HighWater(segment.footer.maxSeq, segment.footer.maxId)
    }
    val activeEntries = readActive()
    val active        = Option.when(activeEntries.nonEmpty) {
      val state = activeEntries.foldLeft(emptyState)(updateState)
      HighWater(state.maxSeq, state.maxId)
    }
    (highWaterStore.read() ++ sealedSegments ++ active).reduceOption(_.merge(_))
  }

  /** Deletes the oldest sealed segments whose entries are all at or below `upToSeq`. Their footers are what proves the
    * high water, so it is stored before they are deleted.
    */
  def truncate(upToSeq: Long): Unit = {
    val covered = readFooters().map { case (_, segment) => segment.footer }.takeWhile(_.maxSeq <= upToSeq)
    if (covered.nonEmpty) {
      highWaterStore.advance(HighWater(covered.map(_.maxSeq).max, covered.map(_.maxId).max))
      truncate(segment => segment.footer.maxSeq > upToSeq)
    }
  }

  override protected def currentCodec: LogCodec[JournalEntry, Header, Footer] = Codecs

  override protected def fileName(state: State): String = s"${state.minSeq}-${state.maxSeq}"

}

object DeliveryLog {

  case class Header(id: Long, seq: Long) derives Codec
  case class Footer(minSeq: Long, maxSeq: Long, maxId: Long) derives Codec
  case class State(
      minSeq: Long,
      maxSeq: Long,
      maxId: Long
  ) derives Codec

  object Codecs extends LogCodec[JournalEntry, Header, Footer] {
    override def version: Int                    = 1
    override def headerCodec: Codec[Header]      = summon[Codec[Header]]
    override def entryCodec: Codec[JournalEntry] = summon[Codec[JournalEntry]]
    override def footerCodec: Codec[Footer]      = summon[Codec[Footer]]

  }

}
