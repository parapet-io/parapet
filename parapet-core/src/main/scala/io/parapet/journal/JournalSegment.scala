package io.parapet.journal

/** A journal segment: its [[JournalMetadata]] summary paired with the entries it covers. */
final case class JournalSegment(metadata: JournalMetadata, entries: Vector[JournalEntry])
