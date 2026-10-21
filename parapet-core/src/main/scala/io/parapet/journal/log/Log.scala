package io.parapet.journal.log

import io.parapet.effect.Clock
import io.parapet.journal.{FileUtils, JournalDurability}
import io.parapet.journal.JournalDurability.HostCrash

import java.nio.channels.{FileChannel, SeekableByteChannel}
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import scala.annotation.tailrec
import scala.collection.immutable.SortedMap
import scala.util.Try
import scala.util.control.NonFatal
import scala.jdk.CollectionConverters.*
import org.slf4j.LoggerFactory
import FrameReader.Frame
import Log.*
import SegmentFormat.*
import SegmentReader.{decodeEntry, decodeFooter, decodeHeader, lookup}

/** Single-writer segmented log in one directory: sealed segments named by [[fileName]] with .jrl, the active segment in
  * `open`, and a `running` marker while a Log has the directory open. Only one Log may use a directory at a time;
  * nothing enforces this. Recovery changes only the open file. After a failed append or sync the Log must not be used
  * again, except to close it; nothing guards against this.
  *
  * Under HostCrash durability, file creations, renames and deletions are forced to storage except on Windows, where
  * Java cannot force a directory, so there their survival after a host crash depends on the file system.
  */
abstract class Log[E, Header, Footer, State](config: Config, clock: Clock = Clock()) extends AutoCloseable {
  require(config.maxSegmentSize > 0, "maxSegmentSize must be positive")
  require(config.maxEntrySize > 0, "maxEntrySize must be positive")

  /** The codecs new segments are written with. Constant for the lifetime of this Log instance. */
  protected def currentCodec: LogCodec[E, Header, Footer]

  /** The codecs of a stored codec version, which must report that version, or None for a version this Log cannot read.
    * Segments are read back only through this lookup, so it must also return the current version. Historical codecs
    * decode, except that recovery seals an unsealed segment under its own version, so a historical footer codec must
    * also encode.
    */
  protected def lookupCodec(version: Int): Option[LogCodec[E, Header, Footer]]
  protected def emptyState: State

  /** Must be pure: recovery rebuilds the state by replaying the valid entries of the open file. */
  protected def updateState(state: State, entry: E): State

  /** Builds the segment header from the state after its first entry. Must be pure: the header is written once, and
    * recovery keeps it only together with that entry.
    */
  protected def createHeader(state: State): Header

  /** Builds the segment footer from the state after its last entry. Must be pure: recovery rebuilds it from the
    * replayed state when it seals the open file.
    */
  protected def createFooter(state: State): Footer

  /** The sealed file's base name, built from the state after its last entry; Log adds .jrl. Must be pure, for the same
    * reason as [[createFooter]]. It must be a single file name, unique among the log's segments: a name that is taken
    * fails the seal.
    */
  protected def fileName(state: State): String

  private var segmentWriter: SegmentWriter[E, Header, Footer] = null
  private var state: Option[State]                            = None
  private var initialized                                     = false
  private var headerWritten                                   = false
  private val openFilePath                                    = config.dataDir.resolve("open")
  // Exists from init until a clean close: without it, a crash that leaves an undamaged open file, or none, would look
  // like a clean stop.
  private val runningFilePath = config.dataDir.resolve("running")
  private val logger          = LoggerFactory.getLogger(getClass)

  /** Opens the log, first sealing the open file the previous run left, or deleting it when it holds no entries. A clean
    * stop removes the running marker, and sealing its open file discards no bytes. The marker, or bytes to discard,
    * mean the previous run did not stop cleanly: Repair mode seals the valid entries or discards the file, and Fail
    * mode throws UncleanStopException. Returns what init found in the open file and did with it, or None when there is
    * no open file. A failure leaves no file open.
    */
  final def init(): Option[RecoveryReport[Header, Footer]] = {
    require(!initialized, "log already initialized")
    Files.createDirectories(config.dataDir)
    if (config.durability == HostCrash) FileUtils.forceDirectoryChain(config.dataDir)
    val maxSegmentId = maxSealedSegmentId()
    val report       = recover(maxSegmentId)
    // Created before the open file, whose directory force under HostCrash also makes the marker durable.
    if (!Files.exists(runningFilePath)) Files.createFile(runningFilePath)
    val lastSegmentId =
      report.map(_.action).collect { case RecoveryAction.Sealed(id, _) => id }.getOrElse(maxSegmentId)
    startSegment(Math.addExact(lastSegmentId, 1L))
    initialized = true
    logger.info(
      s"Log opened: dir=${config.dataDir} segmentId=${segmentWriter.segmentId} durability=${config.durability} " +
        s"recoveryMode=${config.recoveryMode}"
    )
    report
  }

  private def recover(maxSegmentId: Long): Option[RecoveryReport[Header, Footer]] =
    if (Files.exists(openFilePath)) Some(recoverOpen(maxSegmentId))
    else {
      if (Files.exists(runningFilePath)) recoverNoOpen()
      None
    }

  /** Seals the active segment first when the entry would not fit, so the entry goes to the next segment. A segment
    * always accepts its first entry, even one larger than maxSegmentSize. A returned entry survives a process crash;
    * under HostCrash durability it survives a host crash once a later [[sync]] succeeds.
    *
    * An entry that encodes to more than maxEntrySize bytes fails with IllegalArgumentException before anything is
    * written, and the Log stays usable. Any other failure leaves no valid entry: a write that fails may leave part of
    * the entry, which the next init finds as damage and discards.
    */
  final def append(entry: E): Try[Unit] = Try {
    require(initialized, "log is not initialized")
    // The entry is encoded once: this frame is what is measured and what is written.
    val frame     = encodeFrame(EntryMagic, entry)(using currentCodec.entryCodec)
    val entrySize = frame.length - FrameOverhead
    require(entrySize <= config.maxEntrySize, s"the entry encodes to $entrySize bytes, above maxEntrySize")
    if (segmentWriter.entryCount > 0 && segmentWriter.size + frame.length > config.maxSegmentSize) {
      seal()
      startSegment(Math.addExact(segmentWriter.segmentId, 1L))
    }
    val nextState = updateState(state.getOrElse(emptyState), entry)
    if (!headerWritten) {
      segmentWriter.writeHeader(createHeader(nextState))
      headerWritten = true
    }
    segmentWriter.writeEntry(frame)
    state = Some(nextState)
  }

  /** Under HostCrash durability, forces every appended entry to storage. Under ProcessCrash this does nothing: an
    * appended entry already survives a process crash once append returns.
    */
  final def sync(): Try[Unit] = Try {
    require(initialized, "log is not initialized")
    if (config.durability == HostCrash) segmentWriter.force()
  }

  /** Reads the sealed segment at `path`, as the channel overload does. */
  final def readSegment(path: Path): Try[Segment[E, Header, Footer]] = Try {
    val channel = FileChannel.open(path, StandardOpenOption.READ)
    try readSegment(channel).get
    finally channel.close()
  }

  /** Reads a sealed segment from the beginning of the channel, whatever its position; the caller retains ownership of
    * the channel. A segment is sealed only once it is complete, so damage in it is a CorruptLogException, an
    * InvalidChecksumException for a failed checksum. An unknown storage format or codec version is an
    * UnsupportedLogVersionException.
    */
  final def readSegment(channel: SeekableByteChannel): Try[Segment[E, Header, Footer]] = Try {
    val reader  = SegmentReader.open(channel, lookupCodec)
    val header  = reader.header
    val footer  = reader.footer
    val entries = List.newBuilder[E]
    while (reader.hasEntry) entries += reader.readEntry()
    val segment = Segment(
      header.segmentId,
      header.header,
      entries.result(),
      footer.footer,
      header.codecVersion,
      header.createdAtMillis,
      footer.sealedAtMillis
    )
    checkFooter(header, footer, segment.entries.size.toLong)
    segment
  }

  /** Reads the entries of the active segment, in the order they were appended. They are the entries appended since the
    * last seal: init seals what the previous run left, so right after init there are none.
    */
  final def readActive(): List[E] = {
    require(initialized, "log is not initialized")
    // Every byte the writer has written is a complete frame. The header is written with the first entry.
    val end = segmentWriter.size
    if (end == 0) Nil
    else {
      val channel = FileChannel.open(openFilePath, StandardOpenOption.READ)
      try {
        val frames     = FrameReader(channel)
        val (_, codec) = decodeHeader(readActiveFrame(frames), lookupCodec)
        val entries    = List.newBuilder[E]
        while (frames.position < end) {
          val start = frames.position
          entries += decodeEntry(start, readActiveFrame(frames).data, codec)
        }
        entries.result()
      } finally channel.close()
    }
  }

  /** The frame at the position of the active segment, below the writer's size, where damage is corruption. */
  private def readActiveFrame(frames: FrameReader): Frame = {
    val start = frames.position
    frames.read().getOrElse(throw new CorruptLogException(s"at offset $start: damaged frame in the active segment"))
  }

  /** The path and footer of every sealed segment, oldest first. The active segment has no footer until it is sealed, so
    * it is not included. Only the two ends of each file are read: the header, for the segment ID that orders it, and
    * the footer.
    */
  final def readFooters(): List[(Path, SegmentFooter[Footer])] = {
    require(initialized, "log is not initialized")
    sealedPaths().values.toList.map(path => path -> readSealed(path)(SegmentReader.readFooter(_, lookupCodec)))
  }

  /** Deletes sealed segments from the oldest forward, up to the first one whose footer `keep` accepts. The active
    * segment is never deleted. A crash part-way leaves the newer of the segments that were to go, never a gap.
    */
  final def truncate(keep: SegmentFooter[Footer] => Boolean): Unit = {
    val paths = readFooters().takeWhile { case (_, footer) => !keep(footer) }.map { case (path, _) => path }
    if (paths.nonEmpty) {
      paths.foreach(Files.delete)
      if (config.durability == HostCrash) FileUtils.forceDirectory(config.dataDir)
      logger.info(s"Log truncated: deleted ${paths.size} sealed segments, up to ${paths.last}")
    }
  }

  /** Reads the open file for recovery, frame by frame from its start. A crash can cut this file anywhere, so a frame
    * counts only when every frame before it is valid: reading stops at the first damaged frame, at the end of the file,
    * or at a seal that the crash cut short. The valid entries are folded into the state as they are read, and only the
    * footer and file name built from it are kept. The header is written with the first entry and is durable only after
    * a sync, so a damaged header is crash damage like a damaged entry.
    */
  private def readContents(channel: SeekableByteChannel): SegmentContents[Header, Footer] = {
    val frames = FrameReader(channel)
    val size   = frames.size
    frames.read() match {
      case Left(_)      => SegmentContents.NoHeader(size)
      case Right(frame) =>
        val (header, codec) = decodeHeader(frame, lookupCodec)

        @tailrec
        def readEntries(state: State, entryCount: Long): SegmentContents[Header, Footer] = {
          val start = frames.position
          frames.read() match {
            case Right(Frame(FrameKind.Entry, data)) =>
              readEntries(updateState(state, decodeEntry(start, data, codec)), entryCount + 1)
            // The footer counts only with the trailer after it, which ends the file and points back at the footer.
            case Right(Frame(FrameKind.Footer, data))
                if frames.position == size - TrailerSize && frames.readTrailer().contains(start) =>
              val footer = decodeFooter(start, data, lookupCodec)
              checkFooter(header, footer, entryCount)
              SegmentContents.Complete(header, footer, fileName(state))
            case Right(Frame(FrameKind.Header, _)) =>
              throw new CorruptLogException(s"at offset $start: unexpected Header frame")
            case _ if entryCount == 0 => SegmentContents.NoEntries(header, size)
            case _                    =>
              SegmentContents.Unsealed(header, createFooter(state), fileName(state), entryCount, start, size)
          }
        }

        readEntries(emptyState, 0L)
    }
  }

  /** The footer must agree with the header and with the `entryCount` entries read before it. */
  private def checkFooter(header: SegmentHeader[Header], footer: SegmentFooter[Footer], entryCount: Long): Unit = {
    check(
      footer.codecVersion == header.codecVersion,
      s"the footer names codec version ${footer.codecVersion}, but the header names ${header.codecVersion}"
    )
    check(
      footer.entryCount == entryCount,
      s"the footer counts ${footer.entryCount} entries, but the segment holds $entryCount"
    )
    check(
      footer.createdAtMillis == header.createdAtMillis,
      s"the footer creation time ${footer.createdAtMillis} differs from the header's ${header.createdAtMillis}"
    )
  }

  /** The highest sealed segment ID, or -1 when there are none. */
  private def maxSealedSegmentId(): Long = sealedPaths().lastOption.fold(-1L) { case (id, _) => id }

  /** The sealed files by segment ID, oldest first. Only the header of each is read, so damage after it is found only
    * when the segment is read. Two files with one ID are corruption.
    */
  private def sealedPaths(): SortedMap[Long, Path] = {
    val files = Files.list(config.dataDir)
    try {
      val sealedFiles = files.iterator().asScala.filter(_.getFileName.toString.endsWith(SegmentExtension))
      sealedFiles.foldLeft(SortedMap.empty[Long, Path]) { (paths, path) =>
        val id = readSealed(path)(SegmentReader.readHeader(_, lookupCodec).segmentId)
        paths.get(id).foreach { other =>
          throw new CorruptLogException(s"sealed segments $other and $path have the same ID $id")
        }
        paths.updated(id, path)
      }
    } finally files.close()
  }

  /** Reads from the sealed file at `path`. A failure is logged with the path, which the exception does not name. */
  private def readSealed[A](path: Path)(read: SeekableByteChannel => A): A = {
    val channel = FileChannel.open(path, StandardOpenOption.READ)
    try read(channel)
    catch {
      case NonFatal(error) =>
        logger.error(s"Log cannot read sealed segment: file=$path", error)
        throw error
    } finally channel.close()
  }

  /** Seals the open file the previous run left, or deletes it, and reports what it found and did. Fail mode refuses an
    * unclean stop: without reading the file when the marker shows it, and after reading it when the file holds bytes
    * that sealing would discard.
    */
  private def recoverOpen(maxSegmentId: Long): RecoveryReport[Header, Footer] = {
    val marked = Files.exists(runningFilePath)
    if (marked && config.recoveryMode == RecoveryMode.Fail) throw new UncleanStopException(openFilePath)
    // The reader is closed before the file is deleted, moved or truncated.
    val openSegment = {
      val channel = FileChannel.open(openFilePath, StandardOpenOption.READ)
      try readContents(channel)
      finally channel.close()
    }
    if (!marked && openSegment.discardedBytes == 0L) {
      val action = sealOrDiscard(maxSegmentId, openSegment)
      logger.info(s"Log init after a clean stop: file=$openFilePath found=${openSegment.summary} action=$action")
      RecoveryReport(openSegment, action, cleanStop = true)
    } else {
      if (config.recoveryMode == RecoveryMode.Fail) throw new UncleanStopException(openFilePath)
      // Logged before acting, so a failure below still leaves what was found in the log.
      logger.warn(s"Log recovery: file=$openFilePath found=${openSegment.summary}")
      val action = sealOrDiscard(maxSegmentId, openSegment)
      logger.warn(s"Log recovery: file=$openFilePath action=$action")
      RecoveryReport(openSegment, action, cleanStop = false)
    }
  }

  /** Seals what can be read of the open file, or deletes it when no entry is valid. */
  private def sealOrDiscard(maxSegmentId: Long, openSegment: SegmentContents[Header, Footer]): RecoveryAction =
    openSegment match {
      case SegmentContents.NoHeader(_) | SegmentContents.NoEntries(_, _) =>
        Files.delete(openFilePath)
        RecoveryAction.Discarded
      case SegmentContents.Complete(header, _, targetFileName) =>
        checkOpenSegmentId(header.segmentId, maxSegmentId)
        RecoveryAction.Sealed(header.segmentId, publishRecovered(targetFileName))
      case unsealed: SegmentContents.Unsealed[Header, Footer] =>
        checkOpenSegmentId(unsealed.header.segmentId, maxSegmentId)
        RecoveryAction.Sealed(unsealed.header.segmentId, sealRecovered(unsealed))
    }

  /** The previous run left the running marker but no open file: it stopped after sealing or deleting one open file and
    * before starting the next, in a roll or in init. Every sealed segment is complete, so Repair has nothing to
    * recover; in a roll, the entry that started the next segment was never written.
    */
  private def recoverNoOpen(): Unit = {
    if (config.recoveryMode == RecoveryMode.Fail) throw new UncleanStopException(runningFilePath)
    logger.warn(s"Log recovery: file=$runningFilePath found=an unclean stop with no open file; nothing to recover")
  }

  private def checkOpenSegmentId(id: Long, maxSegmentId: Long): Unit =
    check(id > maxSegmentId, s"open segment ID $id is not above the highest sealed segment ID $maxSegmentId")

  /** The open segment is complete: the previous run stopped after writing its footer, before publishing it. */
  private def publishRecovered(targetFileName: String): Path = {
    val target = segmentPath(targetFileName)
    publishSealed(target)
    target
  }

  /** Seals the valid entries under the recovered footer, encoded with the codec version the header names, so a segment
    * uses one version. Each step leaves the open file in a state the next init can recover.
    */
  private def sealRecovered(unsealed: SegmentContents.Unsealed[Header, Footer]): Path = {
    val header = unsealed.header
    // The name is checked and the footer encoded before open is changed, so a failure leaves the bytes recovery would
    // discard.
    val target = segmentPath(unsealed.targetFileName)
    val codec  = lookup(lookupCodec, header.codecVersion)
    // Only to fail before open is changed: the writer encodes the footer again when it seals.
    codec.footerCodec.encode(unsealed.footer).get
    val writer = SegmentWriter.resumeOpen(
      truncateOpen(unsealed.validEnd),
      header.segmentId,
      header.createdAtMillis,
      unsealed.entryCount,
      unsealed.validEnd,
      codec,
      clock
    )
    seal(writer, unsealed.footer, target)
    target
  }

  /** Removes the bytes after the valid entry prefix and returns the file opened for writing, positioned at its end. */
  private def truncateOpen(validEnd: Long): FileChannel = {
    val channel = FileChannel.open(openFilePath, StandardOpenOption.WRITE)
    try {
      channel.truncate(validEnd)
      // The footer written next may be shorter than the removed tail, which must not reappear after it following a host
      // crash: the file would no longer end with its trailer.
      if (config.durability == HostCrash) channel.force(true)
      channel.position(validEnd)
      channel
    } catch {
      case NonFatal(error) =>
        channel.close()
        throw error
    }
  }

  private def segmentPath(baseName: String): Path = {
    val name = Path.of(baseName)
    require(
      baseName.nonEmpty && !name.isAbsolute && name.getNameCount == 1 &&
        baseName != "." && baseName != "..",
      "invalid segment filename"
    )
    val target = config.dataDir.resolve(baseName + SegmentExtension)
    if (Files.exists(target)) throw new java.nio.file.FileAlreadyExistsException(target.toString)
    target
  }

  /** Seals the active segment under the footer and file name built from its state. */
  private def seal(): Unit = seal(segmentWriter, createFooter(state.get), segmentPath(fileName(state.get)))

  /** Writes the footer and the trailer, closes the writer, then publishes its segment as `target`, which
    * [[segmentPath]] has checked.
    */
  private def seal(writer: SegmentWriter[E, Header, Footer], footer: Footer, target: Path): Unit = {
    try writer.writeFooter(footer)
    finally writer.close()
    publishSealed(target)
    logger.info(
      s"Log sealed: segmentId=${writer.segmentId} entries=${writer.entryCount} bytes=${writer.size} file=$target"
    )
  }

  /** Moves the complete open file to its sealed name. Under HostCrash its bytes are forced first, so a host crash never
    * leaves a corrupt sealed segment, and the directory after.
    */
  private def publishSealed(target: Path): Unit = {
    if (config.durability == HostCrash) FileUtils.forceFile(openFilePath)
    Files.move(openFilePath, target, StandardCopyOption.ATOMIC_MOVE)
    if (config.durability == HostCrash) FileUtils.forceDirectory(config.dataDir)
  }

  /** Starts an empty open file for segment `id`; its header is written with the first entry. */
  private def startSegment(id: Long): Unit = {
    val writer = SegmentWriter(createChannel(), id, currentCodec, clock)
    // A sync makes entries durable only if the new file's directory entry also survives a host crash.
    try if (config.durability == HostCrash) FileUtils.forceDirectory(config.dataDir)
    catch {
      case NonFatal(error) =>
        writer.close()
        throw error
    }
    segmentWriter = writer
    state = None
    headerWritten = false
  }

  private def createChannel(): FileChannel =
    FileChannel.open(openFilePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)

  /** Writes nothing: the open file stays for the next init to seal. Under HostCrash the file is forced first, so a
    * clean stop keeps every appended entry. Then the running marker is removed, which records the clean stop. A write
    * that failed halfway shows up at the next init as damage in the open file, which makes the stop unclean. Without an
    * open writer, as after a failed init or roll, it does nothing, so it records no clean stop.
    */
  override def close(): Unit =
    if (segmentWriter != null && segmentWriter.isOpen) {
      try if (config.durability == HostCrash) segmentWriter.force()
      finally segmentWriter.close()
      // Removed last, so a crash before this point leaves an unclean stop for the next init.
      Files.delete(runningFilePath)
      // Otherwise the marker may reappear after a host crash, and the next init would see an unclean stop.
      if (config.durability == HostCrash) FileUtils.forceDirectory(config.dataDir)
      logger.info(s"Log closed: dir=${config.dataDir}")
    }

  private def check(condition: Boolean, message: String): Unit =
    if (!condition) throw new CorruptLogException(message)
}

object Log {

  /** `maxSegmentSize` limits the header and entries of a segment: an entry that would exceed it starts the next
    * segment. The footer and trailer are not counted, so a sealed file is larger by their size, and a segment always
    * accepts its first entry, even one larger than the limit.
    *
    * `maxEntrySize` limits the bytes one entry encodes to; append rejects a larger entry. It applies to appends only:
    * entries already in the log are read whatever their size.
    */
  case class Config(
      dataDir: Path,
      maxSegmentSize: Long,
      durability: JournalDurability = JournalDurability.HostCrash,
      recoveryMode: RecoveryMode = RecoveryMode.Repair,
      maxEntrySize: Int = Int.MaxValue
  )

  /** What init does when the previous run did not stop cleanly. */
  enum RecoveryMode:
    /** Seal the valid entries of the open file, or delete it when none are valid. Without an open file, there is
      * nothing to recover.
      */
    case Repair

    /** Throw UncleanStopException without changing any file, so an operator can audit the stop. The open file is read
      * only when the running marker is absent, to find bytes that sealing would discard.
      */
    case Fail

  enum RecoveryAction:
    /** Segment `segmentId`, complete or as its valid entries under a new footer, was published at `path`. */
    case Sealed(segmentId: Long, path: Path)

    /** No entry was valid, so the open file was deleted. */
    case Discarded

  /** What init found in the open file the previous run left, and what it did with it. `cleanStop` is false when the run
    * left the running marker or bytes that sealing discarded, so entries may have been lost.
    */
  final case class RecoveryReport[Header, Footer](
      openSegment: SegmentContents[Header, Footer],
      action: RecoveryAction,
      cleanStop: Boolean
  )

  val SegmentExtension = ".jrl"
}
