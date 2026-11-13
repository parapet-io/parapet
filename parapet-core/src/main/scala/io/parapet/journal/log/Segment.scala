package io.parapet.journal.log

import io.parapet.Codec

case class Segment[E, Header, Footer](
    id: Long,
    header: Header,
    entries: List[E],
    footer: Footer,
    codecVersion: Int,
    createdAtMillis: Long,
    sealedAtMillis: Long
)

case class SegmentHeader[Header](
    formatVersion: Int,
    segmentId: Long,
    codecVersion: Int,
    createdAtMillis: Long,
    header: Header
) derives Codec

case class SegmentFooter[Footer](
    entryCount: Long,
    codecVersion: Int,
    createdAtMillis: Long,
    sealedAtMillis: Long,
    footer: Footer
) derives Codec
