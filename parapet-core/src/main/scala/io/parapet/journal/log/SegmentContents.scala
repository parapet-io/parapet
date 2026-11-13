package io.parapet.journal.log

/** What recovery finds in the open file, as far as its frames can be read. It holds no entries: recovery folds them
  * into what sealing needs while it reads. Invariant, like Segment, so a match recovers the exact types.
  */
sealed trait SegmentContents[Header, Footer] {

  /** The bytes after the readable data, which recovery does not keep. */
  def discardedBytes: Long

  /** A one-line description, for logs and error messages. */
  def summary: String
}

object SegmentContents {

  /** No readable header: the file is empty, or its header is cut off or damaged. */
  final case class NoHeader[Header, Footer](size: Long) extends SegmentContents[Header, Footer] {
    def discardedBytes: Long = size
    def summary: String      = s"no readable header in $size bytes"
  }

  /** A readable header but no valid entry: the first entry is missing, cut off or damaged. The header is discarded with
    * it, since it was built from that entry.
    */
  final case class NoEntries[Header, Footer](header: SegmentHeader[Header], size: Long)
      extends SegmentContents[Header, Footer] {
    def discardedBytes: Long = size
    def summary: String      = s"segment ${header.segmentId} with no valid entries in $size bytes"
  }

  /** A header and valid entries without a complete footer. `footer` and `targetFileName` are built from the state after
    * the last valid entry, which ends at `validEnd`; the bytes after it are discarded.
    */
  final case class Unsealed[Header, Footer](
      header: SegmentHeader[Header],
      footer: Footer,
      targetFileName: String,
      entryCount: Long,
      validEnd: Long,
      size: Long
  ) extends SegmentContents[Header, Footer] {
    def discardedBytes: Long = size - validEnd
    def summary: String      =
      s"segment ${header.segmentId} with $entryCount valid entries and $discardedBytes bytes after them"
  }

  /** A complete segment whose footer agrees with its entries. `targetFileName` is built from the state after its last
    * entry.
    */
  final case class Complete[Header, Footer](
      header: SegmentHeader[Header],
      footer: SegmentFooter[Footer],
      targetFileName: String
  ) extends SegmentContents[Header, Footer] {
    def discardedBytes: Long = 0L
    def summary: String      = s"complete segment ${header.segmentId} with ${footer.entryCount} entries"
  }
}
