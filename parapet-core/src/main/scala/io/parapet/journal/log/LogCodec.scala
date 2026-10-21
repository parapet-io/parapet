package io.parapet.journal.log

import io.parapet.Codec

/** The application's codecs of one codec version. A segment's header, entries and footer are all encoded with the
  * codecs of the version its header names. An entry must encode to at least one byte; the header and footer may encode
  * to none.
  */
trait LogCodec[Entry, Header, Footer] {
  def version: Int
  def headerCodec: Codec[Header]
  def entryCodec: Codec[Entry]
  def footerCodec: Codec[Footer]
}
