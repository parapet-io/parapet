package io.parapet.journal

import io.parapet.effect.Clock

import java.nio.ByteBuffer
import java.nio.channels.{FileChannel, SeekableByteChannel, WritableByteChannel}
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import java.util.zip.CRC32C
import scala.jdk.CollectionConverters.*

/** Physical append-only storage for delivery journal entries.
  *
  * All fixed-width values are big-endian. File layout:
  *
  * {{{
  * header:
  * [magic: 4][format version: 2][first sequence: 8][created at millis: 8][checksum: 4]
  *
  * entry:
  * [magic: 4][sequence: 8][payload length: 4][encoded journal entry][checksum: 4]
  *
  * footer:
  * [start magic: 4][footer payload][checksum: 4][end magic: 4]
  *
  * footer payload:
  * [first sequence: 8][last sequence: 8][entry count: 8][entries end offset: 8]
  * [created at millis: 8][sealed at millis: 8][maximum envelope ID: 8]
  *
  * delivery high-water:
  * [magic: 4][format version: 2][maximum sequence: 8][maximum envelope ID: 8][checksum: 4]
  * }}}
  *
  * All checksums are CRC32C. Active files omit the footer until sealed.
  */
final private[journal] class DeliveryLog(
    config: DeliveryLog.Config,
    clock: Clock = Clock()
):

  import DeliveryLog.*

  require(
    config.maxSegmentBytes > MinimumSegmentBytes,
    s"maxSegmentBytes must be greater than $MinimumSegmentBytes"
  )
  require(config.maxEntryBytes > 0, "maxEntryBytes must be positive")
  require(
    config.maxEntryBytes <= Int.MaxValue - EntryFrame.HeaderSize - EntryFrame.ChecksumSize,
    "maxEntryBytes is too large to frame"
  )

  private val lock = new Object

  private var initialized           = false
  private var dataDirectoryPrepared = false
  private var checkpointHighWater   = Option.empty[HighWater]
  private var sealedMetadata        = Vector.empty[Metadata]
  private var activeMetadata        = Option.empty[Metadata]

  /** Appends a strictly increasing batch above the durable delivery-sequence high-water. */
  def append(entries: Vector[JournalEntry]): Unit =
    if entries.nonEmpty then
      lock.synchronized {
        try
          initialize()
          validateBatch(entries)
          validateNewRange(entries)
          appendNew(entries)
        catch
          case error: Throwable =>
            invalidate()
            throw error
      }

  /** Returns entries with sequences greater than `afterSeq`, in ascending sequence order. */
  def read(afterSeq: Long): Vector[JournalEntry] =
    lock.synchronized {
      initialize()
      val entries = allMetadata
        .filter(_.lastSeq > afterSeq)
        .flatMap(metadata => readSegment(metadata).entries)
        .filter(_.seq > afterSeq)
        .sortBy(_.seq)
      validateBatch(entries)
      entries
    }

  /** The durable delivery-sequence high-water, including discarded entries. */
  def maxSeq: Option[Long] =
    lock.synchronized {
      initialize()
      effectiveHighWater.map(_.maxSeq)
    }

  /** The durable envelope-ID high-water, including discarded entries. */
  def maxEnvelopeId: Option[Long] =
    lock.synchronized {
      initialize()
      effectiveHighWater.map(_.maxEnvelopeId)
    }

  /** Deletes physical segments whose entries are all at or below `upToSeq`. */
  def truncate(upToSeq: Long): Unit =
    lock.synchronized {
      try
        initialize()
        val (discardedSealed, retainedSealed) = sealedMetadata.partition(_.lastSeq <= upToSeq)
        val discardedActive                   = activeMetadata.filter(_.lastSeq <= upToSeq)
        val discarded                         = discardedSealed ++ discardedActive

        if discarded.nonEmpty then
          val highWater = effectiveHighWater.getOrElse {
            throw new IllegalStateException("cannot truncate delivery segments without a durable high-water")
          }
          // Publish the monotonic checkpoint before removing the segment files that currently prove it.
          persistHighWater(highWater)
          discarded.foreach(metadata => Files.deleteIfExists(metadata.path))
          forceDataDirectory()
          sealedMetadata = retainedSealed
          if discardedActive.nonEmpty then activeMetadata = None
      catch
        case error: Throwable =>
          invalidate()
          throw error
    }

  private def initialize(): Unit =
    if !initialized then
      try
        if Files.exists(config.dataDir) then
          if !Files.isDirectory(config.dataDir) then
            throw new java.nio.file.NotDirectoryException(config.dataDir.toString)
          ensureDataDirectory()

          val highWaterPath   = config.dataDir.resolve(HighWaterFileName)
          val loadedHighWater =
            if Files.exists(highWaterPath) then Some(readHighWater(highWaterPath))
            else None

          val listing = Files.list(config.dataDir)
          val paths   =
            try listing.iterator().asScala.toVector
            finally listing.close()

          val sealedPaths = paths.filter(_.getFileName.toString.endsWith(SealedSuffix))
          val openPaths   = paths.filter(_.getFileName.toString.endsWith(OpenSuffix))
          if openPaths.length > 1 then
            throw new CorruptLogException(s"${config.dataDir} contains multiple active delivery files")

          val loadedMetadata = (sealedPaths.map(readMetadata) ++ openPaths.flatMap(recoverOpen)).sortBy(_.firstSeq)
          validateMetadataRanges(loadedMetadata)
          checkpointHighWater = loadedHighWater
          sealedMetadata = loadedMetadata

        initialized = true
      catch
        case error: Throwable =>
          invalidate()
          throw error

  private def invalidate(): Unit =
    initialized = false
    dataDirectoryPrepared = false
    checkpointHighWater = None
    sealedMetadata = Vector.empty
    activeMetadata = None

  private def allMetadata: Vector[Metadata] =
    activeMetadata match
      case Some(active) => sealedMetadata :+ active
      case None         => sealedMetadata

  private def segmentHighWater: Option[HighWater] =
    allMetadata.lastOption.map { latest =>
      HighWater(latest.lastSeq, allMetadata.map(_.maxEnvelopeId).max)
    }

  private def effectiveHighWater: Option[HighWater] =
    (checkpointHighWater, segmentHighWater) match
      case (Some(checkpoint), Some(segments)) => Some(checkpoint.merge(segments))
      case (some @ Some(_), None)             => some
      case (None, some @ Some(_))             => some
      case (None, None)                       => None

  private def validateBatch(entries: Vector[JournalEntry]): Unit =
    var previous = -1L
    entries.foreach { entry =>
      check(entry.seq >= 0L, s"negative delivery sequence: ${entry.seq}")
      check(entry.seq > previous, s"delivery sequences must be strictly increasing: $previous then ${entry.seq}")
      previous = entry.seq
    }

  private def validateMetadataRanges(segments: Vector[Metadata]): Unit =
    segments.foreach { metadata =>
      val (fileFirstSeq, fileLastSeq) = parseSealedRange(metadata.path)
      check(
        fileFirstSeq == metadata.firstSeq,
        s"filename first sequence $fileFirstSeq does not match metadata ${metadata.firstSeq}"
      )
      check(
        fileLastSeq == metadata.lastSeq,
        s"filename last sequence $fileLastSeq does not match metadata ${metadata.lastSeq}"
      )
    }
    segments.sliding(2).foreach {
      case Vector(previous, next) =>
        check(
          previous.lastSeq < next.firstSeq,
          s"overlapping delivery ranges [${previous.firstSeq}, ${previous.lastSeq}] and " +
            s"[${next.firstSeq}, ${next.lastSeq}]"
        )
      case _ => ()
    }

  private def validateNewRange(entries: Vector[JournalEntry]): Unit =
    effectiveHighWater.foreach { highWater =>
      if entries.head.seq <= highWater.maxSeq then
        throw new IllegalStateException(
          s"delivery sequence ${entries.head.seq} does not follow durable high-water ${highWater.maxSeq}"
        )
    }

  private def appendNew(entries: Vector[JournalEntry]): Unit =
    activeMetadata match
      case Some(active) =>
        check(entries.head.seq > active.lastSeq, "new entries overlap the active segment")
        activeMetadata = Some(appendToFile(active, entries, create = false))
      case None =>
        val firstSeq = entries.head.seq
        val metadata = Metadata(
          path = config.dataDir.resolve(openFileName(firstSeq)),
          firstSeq = firstSeq,
          lastSeq = firstSeq,
          entryCount = 0L,
          entriesEndOffset = Header.EncodedSize.toLong,
          createdAtMillis = clock.currentTimeMillis,
          maxEnvelopeId = 0L,
          state = SegmentState.Open
        )
        activeMetadata = Some(appendToFile(metadata, entries, create = true))

    activeMetadata.foreach { active =>
      if active.entriesEndOffset + Footer.EncodedSize.toLong >= config.maxSegmentBytes then sealActive(active)
    }

  private def appendToFile(metadata: Metadata, entries: Vector[JournalEntry], create: Boolean): Metadata =
    ensureDataDirectory()
    val frames = entries.map { entry =>
      val payload = JournalEntryBinaryFormat.encode(entry)
      check(payload.length <= config.maxEntryBytes, s"delivery ${entry.seq} exceeds maxEntryBytes")
      encodeFrame(entry.seq, payload)
    }
    val appendedBytes = frames.foldLeft(0L)((total, frame) => Math.addExact(total, frame.length.toLong))
    val updated       = metadata.copy(
      lastSeq = entries.last.seq,
      entryCount = Math.addExact(metadata.entryCount, entries.size.toLong),
      entriesEndOffset = Math.addExact(metadata.entriesEndOffset, appendedBytes),
      maxEnvelopeId = entries.foldLeft(metadata.maxEnvelopeId) { (current, entry) =>
        math.max(current, math.max(entry.id, entry.cause))
      }
    )

    val channel =
      if create then FileChannel.open(metadata.path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
      else FileChannel.open(metadata.path, StandardOpenOption.WRITE)
    try
      if create then
        writeFully(
          channel,
          ByteBuffer.wrap(encodeHeader(Header(metadata.firstSeq, metadata.createdAtMillis)))
        )
      else
        check(
          channel.size() == metadata.entriesEndOffset,
          s"active file size changed: expected ${metadata.entriesEndOffset}, got ${channel.size()}"
        )
        channel.position(metadata.entriesEndOffset)

      frames.foreach(frame => writeFully(channel, ByteBuffer.wrap(frame)))
      forceAppend(channel)
    finally channel.close()

    if create then forceDataDirectory()

    updated

  /** Applies the configured durability barrier to an ordinary entry append. */
  private def forceAppend(channel: FileChannel): Unit =
    config.durability match
      case JournalDurability.HostCrash    => channel.force(true)
      case JournalDurability.ProcessCrash => ()

  private def sealActive(metadata: Metadata): Unit =
    val sealedMetadataForFile = metadata.copy(state = SegmentState.Sealed(clock.currentTimeMillis))
    appendFooter(metadata.path, footerOf(sealedMetadataForFile))
    val target = config.dataDir.resolve(sealedFileName(metadata.firstSeq, metadata.lastSeq))
    moveAtomically(metadata.path, target)
    sealedMetadata :+= sealedMetadataForFile.copy(path = target)
    activeMetadata = None

  private def readMetadata(path: Path): Metadata =
    val header = readHeader(path)
    val footer = readFooter(path)
    metadataFromFooter(path, header, footer)

  private def recoverOpen(path: Path): Option[Metadata] =
    val filenameFirstSeq = parseOpenFirstSeq(path)
    val fileSize         = Files.size(path)
    if fileSize < Header.EncodedSize then
      deleteRecoveryFile(path)
      None
    else
      val segment = scanSegment(path)
      check(
        segment.metadata.firstSeq == filenameFirstSeq,
        s"active filename sequence $filenameFirstSeq does not match metadata"
      )
      val entriesEndOffset = segment.metadata.entriesEndOffset
      // Keep only the prefix through the last complete entry. The discarded suffix can be an interrupted frame or
      // footer, or a complete footer whose following rename was interrupted. Sealing below writes a fresh footer.
      if fileSize != entriesEndOffset then
        val channel = FileChannel.open(path, StandardOpenOption.WRITE)
        try
          channel.truncate(entriesEndOffset)
          channel.force(true)
        finally channel.close()

      if segment.entries.isEmpty then
        deleteRecoveryFile(path)
        None
      else sealActiveForRecovery(segment.metadata.copy(state = SegmentState.Open))

  private def sealActiveForRecovery(metadata: Metadata): Option[Metadata] =
    val sealedMetadataForFile = metadata.copy(state = SegmentState.Sealed(clock.currentTimeMillis))
    appendFooter(metadata.path, footerOf(sealedMetadataForFile))
    val target = config.dataDir.resolve(sealedFileName(metadata.firstSeq, metadata.lastSeq))
    moveAtomically(metadata.path, target)
    Some(sealedMetadataForFile.copy(path = target))

  private def footerOf(metadata: Metadata): Footer =
    val sealedAtMillis = metadata.state match
      case SegmentState.Sealed(value) => value
      case SegmentState.Open          =>
        throw new IllegalStateException("cannot create a footer for open delivery metadata")
    Footer(
      metadata.firstSeq,
      metadata.lastSeq,
      metadata.entryCount,
      metadata.entriesEndOffset,
      metadata.createdAtMillis,
      sealedAtMillis,
      metadata.maxEnvelopeId
    )

  private def readSegment(metadata: Metadata): Segment =
    val segment = scanSegment(metadata.path)
    validateMetadata(segment.metadata, metadata)
    metadata.state match
      case SegmentState.Open =>
        check(
          Files.size(metadata.path) == segment.metadata.entriesEndOffset,
          s"open segment ${metadata.path} has an incomplete tail"
        )
      case SegmentState.Sealed(_) => ()
    segment

  private def scanSegment(path: Path): Segment =
    withReadChannel(path)(channel => scanSegment(path, channel))

  // scanSegment assumes that file has valid header otherwise it throws
  private[journal] def scanSegment(path: Path, channel: SeekableByteChannel): Segment =
    val header     = DeliveryLog.readHeader(path, channel)
    val segmentEnd = channel.size()

    var position       = Header.EncodedSize.toLong
    var previous       = -1L
    var count          = 0
    var first          = Option.empty[Long]
    var last           = Option.empty[Long]
    var maxEnvelopeId  = 0L
    val entries        = Vector.newBuilder[JournalEntry]
    var footerMetadata = Option.empty[Metadata]
    var scanning       = true

    while position < segmentEnd && scanning do
      val remaining = segmentEnd - position
      if remaining < MagicSize then scanning = false
      else
        val magic = ByteBuffer.wrap(readExactly(channel, position, MagicSize, path)).getInt()
        if magic == Footer.StartMagic then // we reached footer
          if remaining < Footer.EncodedSize then scanning = false
          else
            check(
              remaining == Footer.EncodedSize,
              s"unexpected bytes after footer at offset $position in $path"
            )
            val footer = DeliveryLog.readFooter(path, channel)
            footerMetadata = Some(metadataFromFooter(path, header, footer))
            scanning = false // fully finished reading the segment
        else if magic != EntryFrame.Magic then throw corrupt(path, s"bad entry magic at offset $position")
        else if remaining < EntryFrame.HeaderSize then scanning = false
        else
          val frameHeader = ByteBuffer.wrap(readExactly(channel, position, EntryFrame.HeaderSize, path))
          frameHeader.getInt() // discard magic
          val seq           = frameHeader.getLong()
          val payloadLength = frameHeader.getInt()
          check(payloadLength >= 0, s"negative payload length for delivery $seq in entry at offset $position")
          val frameSize =
            EntryFrame.HeaderSize.toLong + payloadLength.toLong + EntryFrame.ChecksumSize.toLong
          if remaining < frameSize then scanning = false
          else
            check(seq >= 0L, s"negative delivery sequence at offset $position")
            check(seq > previous, s"delivery sequences are not increasing at offset $position")
            val payload        = readExactly(channel, position + EntryFrame.HeaderSize, payloadLength, path)
            val storedChecksum = ByteBuffer
              .wrap(
                readExactly(
                  channel,
                  position + EntryFrame.HeaderSize + payloadLength,
                  EntryFrame.ChecksumSize,
                  path
                )
              )
              .getInt()
            check(frameChecksum(seq, payload) == storedChecksum, s"delivery $seq checksum mismatch")
            val entry = JournalEntryBinaryFormat.decode(payload)
            check(entry.seq == seq, s"delivery payload sequence ${entry.seq} does not match frame sequence $seq")
            entries += entry
            if first.isEmpty then first = Some(seq)
            last = Some(seq)
            maxEnvelopeId = math.max(maxEnvelopeId, math.max(entry.id, entry.cause))
            previous = seq
            count += 1
            position += frameSize

    first.foreach { actual =>
      check(actual == header.firstSeq, s"header first sequence does not match entries in $path")
    }
    val metadata = Metadata(
      path,
      header.firstSeq,
      last.getOrElse(header.firstSeq),
      count,
      position,
      header.createdAtMillis,
      maxEnvelopeId,
      footerMetadata.fold[SegmentState](SegmentState.Open)(_.state)
    )
    footerMetadata.foreach(expected => validateMetadata(metadata, expected))
    Segment(entries.result(), metadata)

  private def metadataFromFooter(
      path: Path,
      header: Header,
      footer: Footer
  ): Metadata =
    check(footer.firstSeq == header.firstSeq, s"footer first sequence does not match header in $path")
    check(footer.entryCount > 0L, s"empty sealed segment in $path")
    check(footer.firstSeq >= 0L, s"negative footer first sequence in $path")
    check(footer.lastSeq >= footer.firstSeq, s"invalid footer range in $path")
    check(footer.entriesEndOffset >= Header.EncodedSize, s"invalid footer entries end offset in $path")
    check(footer.createdAtMillis == header.createdAtMillis, s"footer creation time does not match header in $path")
    check(footer.sealedAtMillis >= footer.createdAtMillis, s"footer seal time precedes creation time in $path")
    check(footer.maxEnvelopeId >= 0L, s"negative footer maximum envelope ID in $path")

    Metadata(
      path,
      footer.firstSeq,
      footer.lastSeq,
      footer.entryCount,
      footer.entriesEndOffset,
      footer.createdAtMillis,
      footer.maxEnvelopeId,
      SegmentState.Sealed(footer.sealedAtMillis)
    )

  private def validateMetadata(actual: Metadata, expected: Metadata): Unit =
    check(actual.path == expected.path, "segment path changed")
    check(actual.firstSeq == expected.firstSeq, "segment first sequence changed")
    check(actual.lastSeq == expected.lastSeq, "segment last sequence changed")
    check(actual.entryCount == expected.entryCount, "segment entry count changed")
    check(actual.entriesEndOffset == expected.entriesEndOffset, "segment entries end offset changed")
    check(actual.createdAtMillis == expected.createdAtMillis, "segment creation time changed")
    check(actual.maxEnvelopeId == expected.maxEnvelopeId, "segment maximum envelope ID changed")
    check(actual.state == expected.state, "segment state changed")

  private def appendFooter(path: Path, footer: Footer): Unit =
    val channel = FileChannel.open(path, StandardOpenOption.WRITE)
    try
      check(
        channel.size() == footer.entriesEndOffset,
        s"cannot append footer at ${footer.entriesEndOffset}; size is ${channel.size()}"
      )
      channel.position(footer.entriesEndOffset)
      writeFully(channel, ByteBuffer.wrap(encodeFooter(footer)))
      channel.force(true)
    finally channel.close()

  private def persistHighWater(highWater: HighWater): Unit =
    checkpointHighWater.foreach { current =>
      require(
        highWater.maxSeq >= current.maxSeq && highWater.maxEnvelopeId >= current.maxEnvelopeId,
        s"delivery high-water cannot regress from $current to $highWater"
      )
    }

    ensureDataDirectory()
    val temporaryPath  = config.dataDir.resolve(HighWaterTempFileName)
    val checkpointPath = config.dataDir.resolve(HighWaterFileName)
    val channel        = FileChannel.open(
      temporaryPath,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE
    )
    try
      writeFully(channel, ByteBuffer.wrap(encodeHighWater(highWater)))
      channel.force(true)
    finally channel.close()

    Files.move(
      temporaryPath,
      checkpointPath,
      StandardCopyOption.ATOMIC_MOVE,
      StandardCopyOption.REPLACE_EXISTING
    )
    forceDataDirectory()
    checkpointHighWater = Some(highWater)

  private def forceDataDirectory(): Unit =
    forceDirectory(config.dataDir)

  private def forceDirectory(path: Path): Unit =
    val channel = FileChannel.open(path, StandardOpenOption.READ)
    try channel.force(true)
    finally channel.close()

  private def ensureDataDirectory(): Unit =
    if !dataDirectoryPrepared then
      Files.createDirectories(config.dataDir)

      // Existing directories may be remnants of an attempt that failed or crashed before its parent entries were
      // forced. Re-establish the full chain on the first access after startup or invalidation; mark it prepared only
      // after every force succeeds so a retry repeats the entire barrier.
      var current = config.dataDir.toAbsolutePath.normalize()
      while current != null do
        forceDirectory(current)
        current = current.getParent
      dataDirectoryPrepared = true

  private def deleteRecoveryFile(path: Path): Unit =
    if Files.deleteIfExists(path) then forceDataDirectory()

  private def moveAtomically(source: Path, target: Path): Unit =
    if Files.exists(target) then throw corrupt(target, "sealed segment already exists")
    Files.move(source, target, StandardCopyOption.ATOMIC_MOVE)
    forceDataDirectory()

  private def readHeader(path: Path): Header =
    withReadChannel(path)(channel => DeliveryLog.readHeader(path, channel))

  private def readFooter(path: Path): Footer =
    withReadChannel(path)(channel => DeliveryLog.readFooter(path, channel))

  private def readHighWater(path: Path): HighWater =
    withReadChannel(path)(channel => DeliveryLog.readHighWater(path, channel))

  private def withReadChannel[A](path: Path)(f: SeekableByteChannel => A): A =
    val channel = FileChannel.open(path, StandardOpenOption.READ)
    try f(channel)
    finally channel.close()

  private def parseSealedRange(path: Path): (Long, Long) =
    val name  = path.getFileName.toString
    val range = name.stripSuffix(SealedSuffix).split('-') match
      case Array(first, last) => first.toLongOption.zip(last.toLongOption)
      case _                  => None
    range.getOrElse(throw corrupt(path, "malformed sealed filename"))

  private def parseOpenFirstSeq(path: Path): Long =
    path.getFileName.toString.stripSuffix(OpenSuffix).toLongOption.getOrElse {
      throw corrupt(path, "malformed active filename")
    }

  private def openFileName(firstSeq: Long): String = s"${pad(firstSeq)}$OpenSuffix"

  private def sealedFileName(firstSeq: Long, lastSeq: Long): String =
    s"${pad(firstSeq)}-${pad(lastSeq)}$SealedSuffix"

  private def pad(seq: Long): String = String.format(s"%0${SeqWidth}d", seq)

  private def check(condition: Boolean, message: => String): Unit =
    if !condition then throw new CorruptLogException(message)

  private def corrupt(path: Path, message: String): CorruptLogException =
    new CorruptLogException(s"${path.getFileName}: $message")

private[journal] object DeliveryLog:

  val MagicSize: Int = java.lang.Integer.BYTES

  final case class Config(
      dataDir: Path,
      maxSegmentBytes: Long,
      maxEntryBytes: Int,
      durability: JournalDurability = JournalDurability.HostCrash
  )

  final class CorruptLogException(message: String) extends RuntimeException(message)

  final private[journal] case class HighWater(maxSeq: Long, maxEnvelopeId: Long):
    def merge(other: HighWater): HighWater =
      HighWater(math.max(maxSeq, other.maxSeq), math.max(maxEnvelopeId, other.maxEnvelopeId))

  private[journal] object HighWater:
    val Magic: Int           = 0x50444857 // "PDHW"
    val FormatVersion: Short = 1

    private val FormatVersionSize = java.lang.Short.BYTES
    private val MaxSeqSize        = java.lang.Long.BYTES
    private val MaxEnvelopeIdSize = java.lang.Long.BYTES
    private val ChecksumSize      = java.lang.Integer.BYTES

    val WithoutChecksumSize: Int = DeliveryLog.MagicSize + FormatVersionSize + MaxSeqSize + MaxEnvelopeIdSize
    val EncodedSize: Int         = WithoutChecksumSize + ChecksumSize

  private[journal] enum SegmentState:
    case Open
    case Sealed(sealedAtMillis: Long)

  final private[journal] case class Metadata(
      // File containing this segment.
      path: Path,
      // Sequence of the first entry, also recorded in the header.
      firstSeq: Long,
      // Sequence of the last complete entry, or `firstSeq` when there are no complete entries.
      lastSeq: Long,
      // Number of complete entry frames.
      entryCount: Long,
      // Byte offset immediately after the last complete entry; excludes the footer and any incomplete tail.
      entriesEndOffset: Long,
      // Wall-clock time recorded when the segment was created.
      createdAtMillis: Long,
      // Greatest envelope ID or cause referenced by a complete entry, or zero when there are no entries.
      maxEnvelopeId: Long,
      // Whether the segment can still accept entries or has a durable footer.
      state: SegmentState
  )

  final private[journal] case class Segment(
      entries: Vector[JournalEntry],
      metadata: Metadata
  )

  final private[journal] case class Header(firstSeq: Long, createdAtMillis: Long)

  private[journal] object Header:
    val Magic: Int           = 0x50444c47 // "PDLG"
    val FormatVersion: Short = 1

    private val FormatVersionSize   = java.lang.Short.BYTES
    private val FirstSeqSize        = java.lang.Long.BYTES
    private val CreatedAtMillisSize = java.lang.Long.BYTES
    private val ChecksumSize        = java.lang.Integer.BYTES

    val WithoutChecksumSize: Int = DeliveryLog.MagicSize + FormatVersionSize + FirstSeqSize + CreatedAtMillisSize
    val EncodedSize: Int         = WithoutChecksumSize + ChecksumSize

  final private[journal] case class Footer(
      firstSeq: Long,
      lastSeq: Long,
      entryCount: Long,
      entriesEndOffset: Long,
      createdAtMillis: Long,
      sealedAtMillis: Long,
      maxEnvelopeId: Long
  )

  private[journal] object Footer:
    val StartMagic: Int = 0x50444653 // "PDFS"
    val EndMagic: Int   = 0x50444654 // "PDFT"

    private val FirstSeqSize         = java.lang.Long.BYTES
    private val LastSeqSize          = java.lang.Long.BYTES
    private val EntryCountSize       = java.lang.Long.BYTES
    private val EntriesEndOffsetSize = java.lang.Long.BYTES
    private val CreatedAtMillisSize  = java.lang.Long.BYTES
    private val SealedAtMillisSize   = java.lang.Long.BYTES
    private val MaxEnvelopeIdSize    = java.lang.Long.BYTES
    private val ChecksumSize         = java.lang.Integer.BYTES

    val PayloadSize: Int =
      FirstSeqSize + LastSeqSize + EntryCountSize + EntriesEndOffsetSize + CreatedAtMillisSize + SealedAtMillisSize +
        MaxEnvelopeIdSize
    val TrailerSize: Int = ChecksumSize + DeliveryLog.MagicSize
    val EncodedSize: Int = DeliveryLog.MagicSize + PayloadSize + TrailerSize

  private[journal] object EntryFrame:
    val Magic: Int = 0x50444c45 // "PDLE"

    val ChecksumSize = java.lang.Integer.BYTES

    private val SeqSize           = java.lang.Long.BYTES
    private val PayloadLengthSize = java.lang.Integer.BYTES

    val HeaderSize: Int = DeliveryLog.MagicSize + SeqSize + PayloadLengthSize

    def checksumInputSize(payloadSize: Int): Int = SeqSize + PayloadLengthSize + payloadSize

    def encodedSize(payloadSize: Int): Int = HeaderSize + payloadSize + ChecksumSize

  private[journal] val HighWaterFileName     = "delivery.high-water"
  private[journal] val HighWaterTempFileName = "delivery.high-water.tmp"
  private[journal] val MinimumSegmentBytes   = Header.EncodedSize.toLong + Footer.EncodedSize.toLong

  private val SeqWidth     = 20
  private val OpenSuffix   = ".open"
  private val SealedSuffix = ".jrnl"

  private def encodeHighWater(highWater: HighWater): Array[Byte] =
    val bytes = ByteBuffer.allocate(HighWater.WithoutChecksumSize)
    bytes.putInt(HighWater.Magic)
    bytes.putShort(HighWater.FormatVersion)
    bytes.putLong(highWater.maxSeq)
    bytes.putLong(highWater.maxEnvelopeId)
    val encoded = bytes.array()
    ByteBuffer.allocate(HighWater.EncodedSize).put(encoded).putInt(checksum(encoded)).array()

  private def decodeHighWater(bytes: Array[Byte]): HighWater =
    checkFormat(bytes.length == HighWater.EncodedSize, "invalid delivery high-water length")
    val buf = ByteBuffer.wrap(bytes)
    checkFormat(buf.getInt() == HighWater.Magic, "bad delivery high-water magic")
    val version = buf.getShort()
    checkFormat(version == HighWater.FormatVersion, s"unsupported delivery high-water version: $version")
    val maxSeq        = buf.getLong()
    val maxEnvelopeId = buf.getLong()
    val encoded       = java.util.Arrays.copyOf(bytes, HighWater.WithoutChecksumSize)
    checkFormat(buf.getInt() == checksum(encoded), "delivery high-water checksum mismatch")
    checkFormat(maxSeq >= 0L, s"negative delivery high-water sequence: $maxSeq")
    checkFormat(maxEnvelopeId >= 0L, s"negative delivery high-water envelope ID: $maxEnvelopeId")
    HighWater(maxSeq, maxEnvelopeId)

  private def encodeHeader(header: Header): Array[Byte] =
    val bytes = ByteBuffer.allocate(Header.WithoutChecksumSize)
    bytes.putInt(Header.Magic)
    bytes.putShort(Header.FormatVersion)
    bytes.putLong(header.firstSeq)
    bytes.putLong(header.createdAtMillis)
    val encoded = bytes.array()
    ByteBuffer.allocate(Header.EncodedSize).put(encoded).putInt(checksum(encoded)).array()

  private def decodeHeader(bytes: Array[Byte]): Header =
    checkFormat(bytes.length == Header.EncodedSize, "invalid delivery-log header length")
    val buf = ByteBuffer.wrap(bytes)
    checkFormat(buf.getInt() == Header.Magic, "bad delivery-log header magic")
    val version = buf.getShort()
    checkFormat(version == Header.FormatVersion, s"unsupported delivery-log version: $version")
    val firstSeq        = buf.getLong()
    val createdAtMillis = buf.getLong()
    val encoded         = java.util.Arrays.copyOf(bytes, Header.WithoutChecksumSize)
    checkFormat(buf.getInt() == checksum(encoded), "delivery-log header checksum mismatch")
    Header(firstSeq, createdAtMillis)

  private def encodeFrame(seq: Long, payload: Array[Byte]): Array[Byte] =
    val checksumInput = ByteBuffer.allocate(EntryFrame.checksumInputSize(payload.length))
    checksumInput.putLong(seq)
    checksumInput.putInt(payload.length)
    checksumInput.put(payload)
    ByteBuffer
      .allocate(EntryFrame.encodedSize(payload.length))
      .putInt(EntryFrame.Magic)
      .putLong(seq)
      .putInt(payload.length)
      .put(payload)
      .putInt(checksum(checksumInput.array()))
      .array()

  private def frameChecksum(seq: Long, payload: Array[Byte]): Int =
    val bytes = ByteBuffer.allocate(EntryFrame.checksumInputSize(payload.length))
    bytes.putLong(seq)
    bytes.putInt(payload.length)
    bytes.put(payload)
    checksum(bytes.array())

  private def encodeFooter(footer: Footer): Array[Byte] =
    val payload = ByteBuffer.allocate(Footer.PayloadSize)
    payload.putLong(footer.firstSeq)
    payload.putLong(footer.lastSeq)
    payload.putLong(footer.entryCount)
    payload.putLong(footer.entriesEndOffset)
    payload.putLong(footer.createdAtMillis)
    payload.putLong(footer.sealedAtMillis)
    payload.putLong(footer.maxEnvelopeId)
    val encoded = payload.array()
    ByteBuffer
      .allocate(Footer.EncodedSize)
      .putInt(Footer.StartMagic)
      .put(encoded)
      .putInt(checksum(encoded))
      .putInt(Footer.EndMagic)
      .array()

  private[journal] def readHeader(path: Path, channel: SeekableByteChannel): Header =
    decodeHeader(readExactly(channel, 0L, Header.EncodedSize, path))

  private[journal] def readHighWater(path: Path, channel: SeekableByteChannel): HighWater =
    val size = channel.size()
    checkFormat(size == HighWater.EncodedSize, s"invalid delivery high-water file length: $size")
    decodeHighWater(readExactly(channel, 0L, HighWater.EncodedSize, path))

  private[journal] def readFooter(path: Path, channel: SeekableByteChannel): Footer =
    val size = channel.size()
    checkFormat(size >= Footer.TrailerSize, "file is too short to contain a footer trailer")
    val trailer = ByteBuffer.wrap(
      readExactly(channel, size - Footer.TrailerSize, Footer.TrailerSize, path)
    )
    val storedChecksum = trailer.getInt()
    val magic          = trailer.getInt()
    checkFormat(magic == Footer.EndMagic, "bad footer end magic")
    val footerStart = size - Footer.EncodedSize
    checkFormat(footerStart >= Header.EncodedSize, "footer starts before the delivery entries")
    val startMagic = ByteBuffer
      .wrap(readExactly(channel, footerStart, MagicSize, path))
      .getInt()
    checkFormat(startMagic == Footer.StartMagic, "bad footer start magic")
    val payload = readExactly(
      channel,
      footerStart + MagicSize,
      Footer.PayloadSize,
      path
    )
    checkFormat(checksum(payload) == storedChecksum, "footer checksum mismatch")
    val footer = decodeFooterPayload(payload)
    checkFormat(footer.entriesEndOffset == footerStart, "footer entries end offset does not match its position")
    footer

  private def decodeFooterPayload(bytes: Array[Byte]): Footer =
    checkFormat(bytes.length == Footer.PayloadSize, "invalid footer payload length")
    val buf = ByteBuffer.wrap(bytes)
    Footer(
      buf.getLong(),
      buf.getLong(),
      buf.getLong(),
      buf.getLong(),
      buf.getLong(),
      buf.getLong(),
      buf.getLong()
    )

  private def readExactly(
      channel: SeekableByteChannel,
      position: Long,
      length: Int,
      path: Path
  ): Array[Byte] =
    val buffer = ByteBuffer.allocate(length)
    channel.position(position)
    var offset = position
    while buffer.hasRemaining do
      val read = channel.read(buffer)
      if read < 0 then throw new CorruptLogException(s"truncated $path at offset $offset")
      else if read == 0 then throw new CorruptLogException(s"reading $path made no progress at offset $offset")
      offset += read.toLong
    buffer.array()

  private[journal] def writeFully(channel: WritableByteChannel, buffer: ByteBuffer): Unit =
    while buffer.hasRemaining do
      val written = channel.write(buffer)
      if written <= 0 then throw new java.io.IOException("writing the delivery log made no progress")

  private def checksum(bytes: Array[Byte]): Int =
    val crc = new CRC32C()
    crc.update(bytes)
    crc.getValue.toInt

  private def checkFormat(condition: Boolean, message: => String): Unit =
    if !condition then throw new CorruptLogException(message)
