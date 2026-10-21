package io.parapet.journal.log

import io.parapet.Codec
import io.parapet.effect.Clock
import io.parapet.journal.{InMemorySeekableByteChannel, JournalDurability}
import io.parapet.journal.log.Log.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.zip.CRC32C
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Try

class LogSpec extends AnyFunSuite:
  private val headerMagic = 0x504c4844
  private val recordMagic = 0x504c5243
  private val footerMagic = 0x504c4654
  private val endMagic    = 0x504c454e

  private def appCodec(version: Int): Codec[Int] = version match
    case 1 => summon[Codec[Int]]
    case 2 =>
      new Codec[Int]:
        def encode(value: Int)         = summon[Codec[Long]].encode(value.toLong)
        def decode(buffer: ByteBuffer) = summon[Codec[Long]].decode(buffer).map(Math.toIntExact)
    case _ => throw new UnsupportedLogVersionException(s"unsupported application codec version: $version")

  private def logCodec(
      codecVersion: Int,
      header: Codec[Int],
      entry: Codec[Int],
      footer: Codec[Int]
  ): LogCodec[Int, Int, Int] =
    new LogCodec[Int, Int, Int]:
      val version     = codecVersion
      val headerCodec = header
      val entryCodec  = entry
      val footerCodec = footer

  private class TestLog(
      dir: Path = Path.of("unused"),
      limit: Long = 1024L,
      clock: Clock = Clock(),
      recoveryMode: RecoveryMode = RecoveryMode.Repair,
      version: Int = 2,
      durability: JournalDurability = JournalDurability.ProcessCrash,
      maxEntrySize: Int = Int.MaxValue
  ) extends Log[Int, Int, Int, Int](Config(dir, limit, durability, recoveryMode, maxEntrySize), clock):
    var lookups = List.empty[Int]

    protected def currentCodec: LogCodec[Int, Int, Int] = codecs(version)

    /** The test log reads application codec versions 1 and 2. */
    protected def lookupCodec(version: Int): Option[LogCodec[Int, Int, Int]] =
      lookups = lookups :+ version
      Option.when(version == 1 || version == 2)(codecs(version))

    private def codecs(version: Int): LogCodec[Int, Int, Int] =
      logCodec(version, headerCodec(version), entryCodec(version), footerCodec(version))

    // The codec of each kind, which a test may replace.
    protected def headerCodec(version: Int): Codec[Int] = appCodec(version)
    protected def entryCodec(version: Int): Codec[Int]  = appCodec(version)
    protected def footerCodec(version: Int): Codec[Int] = appCodec(version)

    protected def emptyState                          = 0
    protected def updateState(state: Int, entry: Int) = state + entry
    protected def createHeader(state: Int)            = state
    protected def createFooter(state: Int)            = state
    protected def fileName(state: Int)                = s"segment-$state"

  private def channel(bytes: Array[Byte], maxReadSize: Int = Int.MaxValue): SeekableByteChannel =
    new InMemorySeekableByteChannel(bytes, maxReadSize)

  private def crc(bytes: Array[Byte]): Int =
    val value = new CRC32C
    value.update(bytes)
    value.getValue.toInt

  private def ints(values: Int*): Array[Byte] =
    val buffer = ByteBuffer.allocate(values.size * 4)
    values.foreach(buffer.putInt)
    buffer.array()

  private def frame(magic: Int, payload: Array[Byte]): Array[Byte] =
    val header = ints(magic, payload.length)
    header ++ ints(crc(header)) ++ payload ++ ints(crc(payload))

  private case class Fixture(bytes: Array[Byte], headerLength: Int, footerStart: Int)

  private def fixture(
      entries: List[Int] = List(10, 20, 30),
      version: Int = 1,
      format: Int = 1,
      count: Long = 3L,
      id: Long = Int.MaxValue.toLong + 10,
      created: Long = 100L,
      footerCreated: Long = 100L,
      sealedAt: Long = 200L,
      extraEntryBytes: Array[Byte] = Array.emptyByteArray,
      // The codec version the footer names, when it is not the header's.
      footerVersion: Option[Int] = None
  ): Fixture =
    // Unknown versions still get a structurally valid fixture; lookup must reject them before decoding application data.
    def app(version: Int) = appCodec(if version == 2 then 2 else 1)
    val fixedHeader       = ByteBuffer.allocate(24).putInt(format).putLong(id).putInt(version).putLong(created).array()
    val header            = frame(headerMagic, fixedHeader ++ app(version).encode(7).get)
    val records           =
      entries.flatMap(value => frame(recordMagic, app(version).encode(value).get ++ extraEntryBytes)).toArray
    val sealedWith  = footerVersion.getOrElse(version)
    val fixedFooter =
      ByteBuffer.allocate(28).putLong(count).putInt(sealedWith).putLong(footerCreated).putLong(sealedAt).array()
    val footer = frame(footerMagic, fixedFooter ++ app(sealedWith).encode(60).get)
    Fixture(header ++ records ++ footer ++ ints(footer.length, endMagic), header.length, header.length + records.length)

  private def corrupt(bytes: Array[Byte]): Unit =
    new TestLog().readSegment(channel(bytes)).failed.get shouldBe a[CorruptLogException]

  test("reads a sealed segment through partial reads without closing the caller's channel") {
    for chunk <- List(1, 3, Int.MaxValue) do
      val source = channel(fixture().bytes, chunk)
      source.position(9)
      val result = new TestLog().readSegment(source).get
      result.id shouldBe Int.MaxValue.toLong + 10
      result.header shouldBe 7
      result.entries shouldBe List(10, 20, 30)
      result.footer shouldBe 60
      result.codecVersion shouldBe 1
      result.createdAtMillis shouldBe 100L
      result.sealedAtMillis shouldBe 200L
      source.isOpen shouldBe true
      source.close()
  }

  test("selects all application codecs using the stored version, including historical versions") {
    for version <- List(1, 2) do
      val log = new TestLog()
      log.readSegment(channel(fixture(version = version).bytes)).get.entries shouldBe List(10, 20, 30)
      // Looked up once for the version the header names and once for the one the footer names.
      log.lookups shouldBe List(version, version)
  }

  test("rejects an unknown storage version before application codec lookup") {
    val log = new TestLog()
    log.readSegment(channel(fixture(format = 99).bytes)).failed.get shouldBe a[UnsupportedLogVersionException]
    log.lookups shouldBe empty
  }

  test("unknown application version is unsupported, not corruption") {
    new TestLog()
      .readSegment(channel(fixture(version = 99).bytes))
      .failed
      .get shouldBe a[UnsupportedLogVersionException]
  }

  test("validates every checksum region") {
    val data = fixture()
    for offset <- List(
        0,
        8,
        12,
        data.headerLength - 1,
        data.headerLength,
        data.headerLength + 8,
        data.headerLength + 12,
        data.headerLength + 19,
        data.footerStart,
        data.footerStart + 8,
        data.footerStart + 12,
        data.bytes.length - 9
      )
    do
      val bytes = data.bytes.clone()
      bytes(offset) = (bytes(offset) ^ 1).toByte
      new TestLog().readSegment(channel(bytes)).failed.get shouldBe a[InvalidChecksumException]
  }

  test("rejects every truncation of a sealed segment") {
    val bytes = fixture().bytes
    for length <- 0 until bytes.length do corrupt(bytes.take(length))
  }

  test("rejects negative or oversized payload lengths even with a valid header checksum") {
    val data = fixture()
    for start <- List(0, data.headerLength, data.footerStart); length <- List(-1, Int.MaxValue) do
      val bytes  = data.bytes.clone()
      val buffer = ByteBuffer.wrap(bytes)
      buffer.putInt(start + 4, length)
      buffer.putInt(start + 8, crc(bytes.slice(start, start + 8)))
      corrupt(bytes)
  }

  test("rejects malformed trailer lengths and end magic") {
    val data = fixture()
    for length <- List(-1, 0, 16, data.bytes.length, Int.MaxValue) do
      val bytes = data.bytes.clone()
      ByteBuffer.wrap(bytes).putInt(bytes.length - 8, length)
      new TestLog().readSegment(channel(bytes)).failed.get shouldBe a[CorruptLogException]
    val bytes = data.bytes.clone()
    ByteBuffer.wrap(bytes).putInt(bytes.length - 4, 0)
    new TestLog().readSegment(channel(bytes)).failed.get shouldBe a[CorruptLogException]
  }

  test("footer-looking bytes inside an incomplete entry do not prevent recovery") {
    val data        = fixture(version = 2, id = 0L)
    val validFooter = data.bytes.drop(data.footerStart)
    val badChecksum = validFooter.clone()
    badChecksum(8) = (badChecksum(8) ^ 1).toByte
    val missingMagic = validFooter.clone()
    ByteBuffer.wrap(missingMagic).putInt(0, recordMagic)
    for tail <- List(validFooter, badChecksum, missingMagic) do
      withDirectory { dir =>
        val prefix = data.bytes.take(data.footerStart)
        // The entry frame is unfinished; its payload happens to end with a footer and trailer.
        val unfinished = frame(recordMagic, tail ++ Array.fill[Byte](20)(0)).take(12 + tail.length)
        crashedRun(dir, prefix ++ unfinished)
        val log = new TestLog(dir):
          override protected def footerCodec(version: Int): Codec[Int] =
            new Codec[Int]:
              def encode(value: Int)                   = appCodec(version).encode(value)
              def decode(buffer: ByteBuffer): Try[Int] =
                scala.util.Failure(new IOException("must not decode a footer inside an entry"))
        try
          val report = log.init()
          report.get.openSegment.discardedBytes shouldBe unfinished.length.toLong
          sealedSegment(report).entries shouldBe List(10, 20, 30)
          Files.size(dir.resolve("open")) shouldBe 0L
        finally log.close()
      }
  }

  /** The segment that recovery sealed, read back with the standard codecs. */
  private def sealedSegment(report: Option[RecoveryReport[Int, Int]]): Segment[Int, Int, Int] =
    report.map(_.action) match
      case Some(RecoveryAction.Sealed(_, path)) => new TestLog().readSegment(path).get
      case other                                => fail(s"expected a sealed segment, got $other")

  private val inconsistentFooters =
    List(fixture(count = 0), fixture(count = 2), fixture(footerCreated = 999L), fixture(footerVersion = Some(2)))

  test("a footer whose trailer is cut off is discarded and valid entries are sealed under their own codec version") {
    // Without a complete trailer the footer is never decoded, so its contents do not matter.
    for
      data                <- fixture() :: inconsistentFooters
      missingTrailerBytes <- 1 to 8
    do
      withDirectory { dir =>
        val bytes = data.bytes.dropRight(missingTrailerBytes)
        crashedRun(dir, bytes)
        val log = new TestLog(dir)
        try
          val segment = sealedSegment(log.init())
          segment.codecVersion shouldBe 1
          segment.entries shouldBe List(10, 20, 30)
        finally log.close()
      }
  }

  test("a complete footer that disagrees with its header or entries fails recovery and leaves open untouched") {
    // A crash cannot write a complete footer and trailer with the wrong contents.
    for data <- inconsistentFooters do
      withDirectory { dir =>
        crashedRun(dir, data.bytes)
        val log = new TestLog(dir)
        try intercept[CorruptLogException](log.init())
        finally log.close()
        Files.readAllBytes(dir.resolve("open")).toVector shouldBe data.bytes.toVector
      }
  }

  test("initialization without an existing open file returns no recovery report") {
    withDirectory { dir =>
      val log = new TestLog(dir)
      try log.init() shouldBe None
      finally log.close()
    }
  }

  test("count must be positive and match exactly the frames before the footer") {
    for count <- List(-1L, 0L, 1L, 2L, 4L, Long.MaxValue) do corrupt(fixture(count = count).bytes)
  }

  test("rejects negative IDs and inconsistent creation times but permits a clock moving backwards") {
    corrupt(fixture(id = -1L).bytes)
    corrupt(fixture(footerCreated = 101L).bytes)
    new TestLog().readSegment(channel(fixture(sealedAt = 90L).bytes)).get.sealedAtMillis shouldBe 90L
  }

  test("rejects a footer that names another codec version than its header") {
    corrupt(fixture(version = 1, footerVersion = Some(2)).bytes)
    corrupt(fixture(version = 2, footerVersion = Some(1)).bytes)
  }

  test("rejects trailing application bytes even when the frame CRC is valid") {
    corrupt(fixture(extraEntryBytes = Array[Byte](0)).bytes)
  }

  private class DelegatingChannel(delegate: SeekableByteChannel) extends SeekableByteChannel:
    def read(buffer: ByteBuffer)                   = delegate.read(buffer)
    def write(buffer: ByteBuffer)                  = delegate.write(buffer)
    def position()                                 = delegate.position()
    def position(value: Long): SeekableByteChannel =
      delegate.position(value)
      this
    def size()                                     = delegate.size()
    def truncate(value: Long): SeekableByteChannel =
      delegate.truncate(value)
      this
    def isOpen  = delegate.isOpen
    def close() = delegate.close()

  test("preserves storage IOException and fails on a channel that makes no progress") {
    val error  = new IOException("disk failure")
    val broken = new DelegatingChannel(channel(fixture().bytes)):
      override def read(buffer: ByteBuffer): Int = throw error
    new TestLog().readSegment(broken).failed.get shouldBe error
    val stalled = new DelegatingChannel(channel(fixture().bytes)):
      override def read(buffer: ByteBuffer): Int = 0
    val failure = new TestLog().readSegment(stalled).failed.get
    failure shouldBe a[IOException]
    failure.isInstanceOf[CorruptLogException] shouldBe false
  }

  test("unexpected EOF from a channel is corruption") {
    val short = new DelegatingChannel(channel(fixture().bytes)):
      override def read(buffer: ByteBuffer): Int = -1
    new TestLog().readSegment(short).failed.get shouldBe a[CorruptLogException]
  }

  private def withDirectory(f: Path => Unit): Unit =
    val dir = Files.createTempDirectory("generic-log-spec")
    try f(dir)
    finally
      val paths = Files.walk(dir)
      try paths.iterator().asScala.toVector.reverse.foreach(Files.delete)
      finally paths.close()

  /** Leaves `open` as the open file, with the running marker, as a run that crashed leaves them. */
  private def crashedRun(dir: Path, open: Array[Byte]): Unit =
    Files.write(dir.resolve("open"), open)
    Files.createFile(dir.resolve("running"))

  test("writer persists timestamps and extension and uses one constant codec version") {
    withDirectory { dir =>
      val clock = new Clock.Mock(100.millis)
      val log   = new TestLog(dir, 1L, clock, version = 1)
      log.init()
      try
        log.append(10).get
        clock.update(90.millis)
        log.append(20).get
        clock.update(200.millis)
        log.append(30).get
      finally log.close()
      val first = log.readSegment(dir.resolve("segment-10.jrl")).get
      first.codecVersion shouldBe 1
      first.id shouldBe 0L
      first.entries shouldBe List(10)
      first.createdAtMillis shouldBe 100L
      first.sealedAtMillis shouldBe 90L
      val second = log.readSegment(dir.resolve("segment-20.jrl")).get
      second.codecVersion shouldBe 1
      second.id shouldBe 1L
      second.createdAtMillis shouldBe 90L
      second.sealedAtMillis shouldBe 200L
      second.entries shouldBe List(20)
    }
  }

  test("a naming collision fails the append without writing it and preserves the segment") {
    withDirectory { dir =>
      val log = new TestLog(dir, 1L)
      log.init()
      try
        log.append(10).get
        log.append(10).get
        val sealedPath   = dir.resolve("segment-10.jrl")
        val sealedBefore = Files.readAllBytes(sealedPath)
        val openBefore   = Files.readAllBytes(dir.resolve("open"))
        log.append(20).isFailure shouldBe true
        Files.readAllBytes(dir.resolve("open")).toVector shouldBe openBefore.toVector
        Files.readAllBytes(sealedPath).toVector shouldBe sealedBefore.toVector
      finally log.close()
    }
  }

  test("sync and close keep appended entries under both durabilities") {
    for durability <- List(JournalDurability.HostCrash, JournalDurability.ProcessCrash) do
      withDirectory { dir =>
        val first = new TestLog(dir, durability = durability)
        first.init()
        first.append(10).get
        first.sync().get
        first.append(20).get
        first.close()
        val reopened = new TestLog(dir, durability = durability)
        try
          reopened.init().get.cleanStop shouldBe true
          reopened.readSegment(dir.resolve("segment-30.jrl")).get.entries shouldBe List(10, 20)
        finally reopened.close()
      }
  }

  test("append, sync, readActive, readFooters and truncate require an initialized log") {
    val log = new TestLog()
    log.append(10).failed.get shouldBe an[IllegalArgumentException]
    log.sync().failed.get shouldBe an[IllegalArgumentException]
    intercept[IllegalArgumentException](log.readActive())
    intercept[IllegalArgumentException](log.readFooters())
    intercept[IllegalArgumentException](log.truncate(_ => false))
  }

  /** The application footer of each sealed segment, oldest first. */
  private def footers(log: TestLog): List[Int] = log.readFooters().map((_, footer) => footer.footer)

  test("readActive returns the entries appended since the last seal") {
    withDirectory { dir =>
      val headerLength = 48
      val entryLength  = 24
      // A segment holds two entries.
      val log = new TestLog(dir, headerLength + 2L * entryLength)
      log.init()
      try
        log.readActive() shouldBe empty
        log.append(10).get
        log.readActive() shouldBe List(10)
        log.append(20).get
        log.readActive() shouldBe List(10, 20)
        // The third entry seals the first two.
        log.append(40).get
        log.readActive() shouldBe List(40)
        footers(log) shouldBe List(30)
      finally log.close()
      // A close seals nothing; the next init seals what the active segment holds.
      log.readActive() shouldBe List(40)
      val reopened = new TestLog(dir)
      reopened.init()
      try
        reopened.readActive() shouldBe empty
        footers(reopened) shouldBe List(30, 40)
      finally reopened.close()
    }
  }

  test("readActive stops at what the writer has written") {
    withDirectory { dir =>
      val log = new TestLog(dir)
      log.init()
      try
        log.append(10).get
        log.append(20).get
        // What a write that failed halfway leaves after the entries.
        Files.write(dir.resolve("open"), Array[Byte](1, 2, 3), StandardOpenOption.APPEND)
        log.readActive() shouldBe List(10, 20)
      finally log.close()
    }
  }

  test("readActive fails on a damaged entry") {
    withDirectory { dir =>
      val log = new TestLog(dir)
      log.init()
      try
        log.append(10).get
        log.append(20).get
        val open  = dir.resolve("open")
        val bytes = Files.readAllBytes(open)
        bytes(bytes.length - 1) = (bytes.last ^ 1).toByte
        Files.write(open, bytes)
        intercept[CorruptLogException](log.readActive())
      finally log.close()
    }
  }

  test("readFooters returns the path and footer of each sealed segment, oldest first, without the active segment") {
    withDirectory { dir =>
      // With a limit of one byte, every entry seals the one before it.
      val log = new TestLog(dir, 1L, new Clock.Mock(100.millis))
      log.init() shouldBe None
      try
        log.readFooters() shouldBe empty
        // The file names sort the other way than the segment IDs.
        List(30, 10, 20).foreach(log.append(_).get)
        log.readFooters() shouldBe List(
          dir.resolve("segment-30.jrl") -> SegmentFooter(1L, 2, 100L, 100L, 30),
          dir.resolve("segment-10.jrl") -> SegmentFooter(1L, 2, 100L, 100L, 10)
        )
      finally log.close()
      // The next init seals the segment that was active.
      val reopened = new TestLog(dir, 1L)
      reopened.init()
      try footers(reopened) shouldBe List(30, 10, 20)
      finally reopened.close()
    }
  }

  test("readFooters reads footers of historical codec versions") {
    withDirectory { dir =>
      Files.write(dir.resolve("old.jrl"), fixture(version = 1, id = 0L).bytes)
      val log = new TestLog(dir)
      log.init()
      try log.readFooters() shouldBe List(dir.resolve("old.jrl") -> SegmentFooter(3L, 1, 100L, 200L, 60))
      finally log.close()
    }
  }

  test("truncate deletes sealed segments from the oldest up to the first one to keep") {
    for durability <- List(JournalDurability.HostCrash, JournalDurability.ProcessCrash) do
      withDirectory { dir =>
        val log = new TestLog(dir, 1L, durability = durability)
        log.init()
        try
          List(10, 20, 30, 40, 50).foreach(log.append(_).get)
          footers(log) shouldBe List(10, 20, 30, 40)
          log.truncate(_ => true)
          footers(log) shouldBe List(10, 20, 30, 40)
          // 30 is the first one to keep, so 40 stays although `keep` rejects it.
          log.truncate(footer => footer.footer == 30)
          footers(log) shouldBe List(30, 40)
          Files.exists(dir.resolve("segment-10.jrl")) shouldBe false
          Files.exists(dir.resolve("segment-20.jrl")) shouldBe false
          log.truncate(_ => false)
          footers(log) shouldBe empty
          // The active segment is never deleted, and segment IDs continue after it.
          log.append(60).get
          log.readSegment(dir.resolve("segment-50.jrl")).get.id shouldBe 4L
          footers(log) shouldBe List(50)
        finally log.close()
      }
  }

  test("after every sealed segment is deleted, a restart continues after the ID of the open segment") {
    withDirectory { dir =>
      val first = new TestLog(dir, 1L)
      first.init()
      List(10, 20, 30).foreach(first.append(_).get)
      first.truncate(_ => false)
      first.close()
      val reopened = new TestLog(dir, 1L)
      reopened.init()
      try
        reopened.append(40).get
        reopened.append(50).get
        reopened.readSegment(dir.resolve("segment-30.jrl")).get.id shouldBe 2L
        reopened.readSegment(dir.resolve("segment-40.jrl")).get.id shouldBe 3L
      finally reopened.close()
    }
  }

  test("a damaged footer fails readFooters, and truncate before it deletes anything") {
    withDirectory { dir =>
      val data    = fixture(version = 2, id = 1L)
      val damaged = data.bytes.clone()
      damaged(data.footerStart + 12) = (damaged(data.footerStart + 12) ^ 1).toByte
      Files.write(dir.resolve("a.jrl"), fixture(version = 2, id = 0L).bytes)
      Files.write(dir.resolve("b.jrl"), damaged)
      val log = new TestLog(dir)
      log.init()
      try
        intercept[InvalidChecksumException](log.readFooters())
        intercept[InvalidChecksumException](log.truncate(_ => false))
        Files.exists(dir.resolve("a.jrl")) shouldBe true
        Files.exists(dir.resolve("b.jrl")) shouldBe true
      finally log.close()
    }
  }

  test("an entry that would exceed the size limit starts the next segment") {
    withDirectory { dir =>
      val headerLength = 48
      val entryLength  = 24
      val log          = new TestLog(dir, headerLength + 2L * entryLength)
      log.init()
      try
        List(10, 20, 30, 40, 50).foreach(log.append(_).get)
        log.readSegment(dir.resolve("segment-30.jrl")).get.entries shouldBe List(10, 20)
        log.readSegment(dir.resolve("segment-70.jrl")).get.entries shouldBe List(30, 40)
      finally log.close()
    }
  }

  test("derived codecs round trip nested records and reject malformed primitive data") {
    case class Nested(name: String, count: Long, bytes: Array[Byte]) derives Codec
    val selected = summon[Codec[Nested]]
    val original = Nested("é中", 7L, Array[Byte](1, 2))
    val decoded  = selected.decode(selected.encode(original).get).get
    decoded.name shouldBe original.name
    decoded.count shouldBe original.count
    decoded.bytes.toVector shouldBe original.bytes.toVector
    summon[Codec[Array[Byte]]].decode(ints(-1)).failed.get shouldBe a[CorruptLogException]
    summon[Codec[String]].decode(ints(1) ++ Array[Byte](0xff.toByte)).failed.get shouldBe a[CorruptLogException]
    summon[Codec[Int]].decode(ints(1, 2)).failed.get shouldBe a[CorruptLogException]
  }

  test("close leaves the open file, which the next init seals before continuing with the next segment ID") {
    withDirectory { dir =>
      val original = new TestLog(dir)
      original.init()
      original.append(10).get
      original.append(20).get
      original.close()
      Files.exists(dir.resolve("segment-30.jrl")) shouldBe false
      // With a limit of one byte, the second entry seals the first.
      val reopened = new TestLog(dir, 1L)
      try
        reopened.init().get.cleanStop shouldBe true
        val segment = reopened.readSegment(dir.resolve("segment-30.jrl")).get
        segment.id shouldBe 0L
        segment.header shouldBe 10
        segment.entries shouldBe List(10, 20)
        segment.footer shouldBe 30
        reopened.append(40).get
        reopened.append(50).get
        reopened.readSegment(dir.resolve("segment-40.jrl")).get.id shouldBe 1L
      finally reopened.close()
    }
  }

  test("every incomplete write in open recovers only fully validated entries") {
    val data        = fixture(version = 2, id = 0L)
    val entryLength = 24
    for cut <- data.bytes.indices do
      withDirectory { dir =>
        crashedRun(dir, data.bytes.take(cut))
        val log = new TestLog(dir)
        try
          val count    = if cut < data.headerLength then 0 else math.min(3, (cut - data.headerLength) / entryLength)
          val retained = if count == 0 then 0 else data.headerLength + entryLength * count
          val report   = log.init()
          report.get.openSegment.discardedBytes shouldBe (cut - retained).toLong
          if count == 0 then report.get.action shouldBe RecoveryAction.Discarded
          else
            val segment = sealedSegment(report)
            segment.entries shouldBe List(10, 20, 30).take(count)
            segment.footer shouldBe List(10, 20, 30).take(count).sum
          Files.size(dir.resolve("open")) shouldBe 0L
        finally log.close()
      }
  }

  test("restart after a close before the first append needs no recovery") {
    withDirectory { dir =>
      val first = new TestLog(dir, 1L)
      first.init()
      first.close()
      val reopened = new TestLog(dir, 1L)
      try
        reopened.init().get.cleanStop shouldBe true
        reopened.append(10).get
        reopened.append(20).get
        reopened.readSegment(dir.resolve("segment-10.jrl")).get.id shouldBe 0L
      finally reopened.close()
    }
  }

  test("an empty open file is discarded and the log continues with the next segment ID") {
    withDirectory { dir =>
      // A crash between sealing a segment and writing the next entry leaves an empty open file.
      Files.write(dir.resolve("existing.jrl"), fixture(version = 2, id = 0L).bytes)
      crashedRun(dir, Array.emptyByteArray)
      val log = new TestLog(dir, 1L)
      try
        val report = log.init().get
        report.action shouldBe RecoveryAction.Discarded
        report.openSegment.discardedBytes shouldBe 0L
        log.append(20).get
        log.append(30).get
        log.readSegment(dir.resolve("segment-20.jrl")).get.id shouldBe 1L
      finally log.close()
    }
  }

  test("a fully written open segment is renamed without rewriting its footer") {
    withDirectory { dir =>
      val data = fixture(version = 2, id = 5L, sealedAt = 80L)
      crashedRun(dir, data.bytes)
      val log      = new TestLog(dir, 1L)
      val recovery = log.init()
      try
        recovery.get.cleanStop shouldBe false
        recovery.get.action shouldBe RecoveryAction.Sealed(5L, dir.resolve("segment-60.jrl"))
        Files.readAllBytes(dir.resolve("segment-60.jrl")).toVector shouldBe data.bytes.toVector
        log.append(70).get
        log.append(80).get
        log.readSegment(dir.resolve("segment-70.jrl")).get.id shouldBe 6L
      finally log.close()
    }
  }

  test("recovery seals old entries under their own codec version, and new segments use the current one") {
    withDirectory { dir =>
      val data = fixture(version = 1, id = 3L)
      crashedRun(dir, data.bytes.take(data.footerStart))
      val log = new TestLog(dir, 1L)
      log.init()
      try
        log.append(70).get
        val old = log.readSegment(dir.resolve("segment-60.jrl")).get
        old.codecVersion shouldBe 1
        old.createdAtMillis shouldBe 100L
        old.entries shouldBe List(10, 20, 30)
        log.append(80).get
        val next = log.readSegment(dir.resolve("segment-70.jrl")).get
        next.id shouldBe 4L
        next.codecVersion shouldBe 2
      finally log.close()
    }
  }

  test("a damaged header discards open, later damage preserves only the valid entry prefix") {
    val data = fixture(version = 2, id = 0L)
    for offset <- List(
        0,
        8,
        12,
        data.headerLength,
        data.headerLength + 12,
        data.headerLength + 24 + 12,
        data.footerStart,
        data.footerStart + 12,
        data.bytes.length - 1
      )
    do
      withDirectory { dir =>
        val bytes = data.bytes.clone()
        bytes(offset) = (bytes(offset) ^ 1).toByte
        crashedRun(dir, bytes)
        val log = new TestLog(dir)
        try
          val count  = if offset < data.headerLength then 0 else math.min(3, (offset - data.headerLength) / 24)
          val report = log.init()
          if count == 0 then report.get.action shouldBe RecoveryAction.Discarded
          else sealedSegment(report).entries shouldBe List(10, 20, 30).take(count)
          Files.size(dir.resolve("open")) shouldBe 0L
        finally log.close()
      }
  }

  test("a header and first entry that a host crash left as zeros are discarded, and the log continues") {
    // Under HostCrash, nothing is durable before the first sync; unflushed bytes may read back as zeros.
    withDirectory { dir =>
      Files.write(dir.resolve("existing.jrl"), fixture(version = 2, id = 0L).bytes)
      val zeros = new Array[Byte](fixture(version = 2).headerLength + 24)
      crashedRun(dir, zeros)
      val log = new TestLog(dir, 1L)
      try
        val report = log.init().get
        report.action shouldBe RecoveryAction.Discarded
        report.openSegment shouldBe SegmentContents.NoHeader(zeros.length.toLong)
        Files.size(dir.resolve("open")) shouldBe 0L
        log.append(20).get
        log.append(30).get
        log.readSegment(dir.resolve("segment-20.jrl")).get.id shouldBe 1L
      finally log.close()
    }
  }

  test("an entry that encodes to no bytes is rejected without writing it") {
    withDirectory { dir =>
      val log = new TestLog(dir):
        override protected def entryCodec(version: Int): Codec[Int] =
          val current = appCodec(version)
          new Codec[Int]:
            def encode(value: Int): Try[Array[Byte]] =
              if value == 0 then scala.util.Success(Array.emptyByteArray) else current.encode(value)
            def decode(buffer: ByteBuffer): Try[Int] = current.decode(buffer)
      log.init()
      try
        log.append(0).failed.get shouldBe a[IllegalArgumentException]
        Files.size(dir.resolve("open")) shouldBe 0L
      finally log.close()
    }
  }

  test("an entry larger than maxEntrySize is rejected before anything is written, and the log stays usable") {
    withDirectory { dir =>
      // Codec version 2 encodes an entry to 8 bytes; here 99 encodes to one byte more.
      class LimitedLog(maxEntrySize: Int) extends TestLog(dir, 1L, maxEntrySize = maxEntrySize):
        override protected def entryCodec(version: Int): Codec[Int] =
          val current = appCodec(version)
          new Codec[Int]:
            def encode(value: Int): Try[Array[Byte]] =
              if value == 99 then scala.util.Success(new Array[Byte](9)) else current.encode(value)
            def decode(buffer: ByteBuffer): Try[Int] = current.decode(buffer)
      val log = new LimitedLog(maxEntrySize = 8)
      log.init()
      try
        log.append(10).get
        val open = Files.readAllBytes(dir.resolve("open"))
        log.append(99).failed.get shouldBe an[IllegalArgumentException]
        // With a limit of one byte a second entry seals the first; the rejected one did not.
        Files.readAllBytes(dir.resolve("open")).toVector shouldBe open.toVector
        footers(log) shouldBe empty
        log.append(20).get
        log.readSegment(dir.resolve("segment-10.jrl")).get.entries shouldBe List(10)
      finally log.close()
      // The limit applies to appends only: a lower one still reads what the log holds.
      val reopened = new LimitedLog(maxEntrySize = 1)
      reopened.init()
      try reopened.readSegment(dir.resolve("segment-20.jrl")).get.entries shouldBe List(20)
      finally reopened.close()
    }
  }

  test("maxEntrySize must be positive") {
    intercept[IllegalArgumentException](new TestLog(maxEntrySize = 0))
  }

  test("an entry that passes its checksum but fails to decode fails recovery and leaves open untouched") {
    withDirectory { dir =>
      val data  = fixture(version = 2, id = 0L, extraEntryBytes = Array[Byte](1))
      val bytes = data.bytes.take(data.footerStart)
      crashedRun(dir, bytes)
      val log = new TestLog(dir)
      try intercept[CorruptLogException](log.init())
      finally log.close()
      Files.readAllBytes(dir.resolve("open")).toVector shouldBe bytes.toVector
    }
  }

  test("a damaged first entry discards the header built from it") {
    withDirectory { dir =>
      val data  = fixture(version = 2, id = 0L)
      val bytes = data.bytes.take(data.footerStart)
      bytes(data.headerLength + 12) = (bytes(data.headerLength + 12) ^ 1).toByte
      crashedRun(dir, bytes)
      val log = new TestLog(dir, 1L)
      try
        val report = log.init().get
        report.action shouldBe RecoveryAction.Discarded
        // The report keeps the readable header, so it names the discarded segment.
        report.openSegment shouldBe SegmentContents.NoEntries(SegmentHeader(1, 0L, 2, 100L, 7), bytes.length.toLong)
        Files.size(dir.resolve("open")) shouldBe 0L
        log.append(70).get
        log.append(80).get
        val segment = log.readSegment(dir.resolve("segment-70.jrl")).get
        segment.header shouldBe 70
        segment.entries shouldBe List(70)
      finally log.close()
    }
  }

  test("recovery with an unchanged codec version seals the valid entries without re-encoding them") {
    withDirectory { dir =>
      val data = fixture(version = 2, id = 3L)
      crashedRun(dir, data.bytes.take(data.footerStart + 5))
      val log = new TestLog(dir):
        override protected def entryCodec(version: Int)  = decoderOnly(version)
        override protected def headerCodec(version: Int) = decoderOnly(version)
      try
        val report     = log.init().get
        val sealedPath = dir.resolve("segment-60.jrl")
        report.action shouldBe RecoveryAction.Sealed(3L, sealedPath)
        report.openSegment.discardedBytes shouldBe 5L
        Files.readAllBytes(sealedPath).take(data.footerStart).toVector shouldBe
          data.bytes.take(data.footerStart).toVector
      finally log.close()
    }
  }

  test("unsupported versions are never discarded by automatic recovery") {
    for data <- List(fixture(format = 99), fixture(version = 99)) do
      withDirectory { dir =>
        crashedRun(dir, data.bytes)
        val log = new TestLog(dir)
        try intercept[UnsupportedLogVersionException](log.init())
        finally log.close()
        Files.readAllBytes(dir.resolve("open")).toVector shouldBe data.bytes.toVector
      }
  }

  test("Fail mode refuses any open file left by an unclean stop and leaves it untouched") {
    // Even a complete segment or an empty file means entries may have been lost before they reached the log. With the
    // marker present the file is not read, so a corrupt header is reported the same way.
    val data          = fixture()
    val corruptHeader = data.bytes.clone()
    corruptHeader(12) = (corruptHeader(12) ^ 1).toByte
    for bytes <- List(data.bytes, data.bytes.take(data.footerStart + 5), Array.emptyByteArray, corruptHeader) do
      withDirectory { dir =>
        crashedRun(dir, bytes)
        val log = new TestLog(dir, recoveryMode = RecoveryMode.Fail)
        try intercept[UncleanStopException](log.init()).path shouldBe dir.resolve("open")
        finally log.close()
        Files.readAllBytes(dir.resolve("open")).toVector shouldBe bytes.toVector
      }
  }

  test("Fail mode starts normally after a clean close") {
    withDirectory { dir =>
      val first = new TestLog(dir, recoveryMode = RecoveryMode.Fail)
      first.init() shouldBe None
      first.append(10).get
      first.close()
      val reopened = new TestLog(dir, recoveryMode = RecoveryMode.Fail)
      try reopened.init().get.cleanStop shouldBe true
      finally reopened.close()
    }
  }

  test("the running marker exists from init until a clean close removes it and leaves the open file") {
    for
      durability <- List(JournalDurability.HostCrash, JournalDurability.ProcessCrash)
      entries    <- List(Nil, List(10, 20))
    do
      withDirectory { dir =>
        // With a limit of one byte, the second entry starts a new segment.
        val log = new TestLog(dir, 1L, durability = durability)
        log.init()
        try
          entries.foreach(log.append(_).get)
          Files.exists(dir.resolve("running")) shouldBe true
        finally log.close()
        Files.exists(dir.resolve("running")) shouldBe false
        Files.exists(dir.resolve("open")) shouldBe true
      }
  }

  test("a run that stopped with no open file is an unclean stop: Fail mode refuses it, Repair continues") {
    withDirectory { dir =>
      // A crash after sealing a segment and before starting the next leaves no open file: the sealed segment is
      // complete, but the entry that started the next segment was never written.
      Files.write(dir.resolve("existing.jrl"), fixture(version = 2, id = 0L).bytes)
      Files.createFile(dir.resolve("running"))
      val failMode = new TestLog(dir, recoveryMode = RecoveryMode.Fail)
      try intercept[UncleanStopException](failMode.init()).path shouldBe dir.resolve("running")
      finally failMode.close()
      Files.exists(dir.resolve("running")) shouldBe true
      Files.exists(dir.resolve("open")) shouldBe false
      // Every sealed segment is complete, so Repair has nothing to recover and continues at the next segment ID.
      val repair = new TestLog(dir)
      try
        repair.init() shouldBe None
        repair.append(40).get
      finally repair.close()
      val reopened = new TestLog(dir, recoveryMode = RecoveryMode.Fail)
      try
        reopened.init().get.cleanStop shouldBe true
        reopened.readSegment(dir.resolve("segment-40.jrl")).get.id shouldBe 1L
      finally reopened.close()
    }
  }

  test("bytes after the last entry of a cleanly closed open file are an unclean stop") {
    withDirectory { dir =>
      val first = new TestLog(dir)
      first.init()
      first.append(10).get
      first.append(20).get
      first.close()
      // What a write that failed halfway before the close leaves.
      val open = dir.resolve("open")
      Files.write(open, Array[Byte](1, 2, 3), StandardOpenOption.APPEND)
      val damaged  = Files.readAllBytes(open)
      val failMode = new TestLog(dir, recoveryMode = RecoveryMode.Fail)
      try intercept[UncleanStopException](failMode.init()).path shouldBe open
      finally failMode.close()
      Files.readAllBytes(open).toVector shouldBe damaged.toVector
      val repair = new TestLog(dir)
      try
        val report = repair.init()
        report.get.openSegment.discardedBytes shouldBe 3L
        report.get.cleanStop shouldBe false
        sealedSegment(report).entries shouldBe List(10, 20)
      finally repair.close()
    }
  }

  test("restart without open derives the next ID from sealed segments") {
    withDirectory { dir =>
      Files.write(dir.resolve("existing.jrl"), fixture(version = 2, id = 8L).bytes)
      val log = new TestLog(dir, 1L)
      log.init()
      try
        log.append(70).get
        log.append(80).get
        log.readSegment(dir.resolve("segment-70.jrl")).get.id shouldBe 9L
      finally log.close()
    }
  }

  test("startup reads only each sealed header: the highest ID wins, and later damage and other files are ignored") {
    withDirectory { dir =>
      val data    = fixture(version = 2, id = 8L)
      val damaged = data.bytes.clone()
      damaged(data.headerLength + 12) = (damaged(data.headerLength + 12) ^ 1).toByte
      Files.write(dir.resolve("a.jrl"), fixture(version = 2, id = 2L).bytes)
      Files.write(dir.resolve("b.jrl"), damaged)
      Files.write(dir.resolve("notes.txt"), Array[Byte](1, 2, 3))
      val log = new TestLog(dir, 1L)
      log.init()
      try
        log.append(70).get
        log.append(80).get
        log.readSegment(dir.resolve("segment-70.jrl")).get.id shouldBe 9L
        log.readSegment(dir.resolve("b.jrl")).failed.get shouldBe a[InvalidChecksumException]
      finally log.close()
    }
  }

  test("a damaged sealed header fails startup and is never recovered") {
    withDirectory { dir =>
      val bytes = fixture().bytes
      bytes(12) = (bytes(12) ^ 1).toByte
      Files.write(dir.resolve("broken.jrl"), bytes)
      val log = new TestLog(dir)
      try intercept[CorruptLogException](log.init())
      finally log.close()
      Files.readAllBytes(dir.resolve("broken.jrl")).toVector shouldBe bytes.toVector
      Files.exists(dir.resolve("open")) shouldBe false
      Files.exists(dir.resolve("running")) shouldBe false
    }
  }

  test("a damaged sealed header fails startup before an open file is changed") {
    withDirectory { dir =>
      val broken = fixture().bytes
      broken(12) = (broken(12) ^ 1).toByte
      Files.write(dir.resolve("broken.jrl"), broken)
      val data = fixture(version = 2, id = 3L)
      val open = data.bytes.take(data.footerStart + 5)
      Files.write(dir.resolve("open"), open)
      val log = new TestLog(dir)
      try intercept[CorruptLogException](log.init())
      finally log.close()
      Files.readAllBytes(dir.resolve("open")).toVector shouldBe open.toVector
      Files.exists(dir.resolve("running")) shouldBe false
    }
  }

  test("two sealed segments with the same ID fail startup and name both files") {
    withDirectory { dir =>
      val bytes = fixture(version = 2, id = 5L).bytes
      Files.write(dir.resolve("a.jrl"), bytes)
      Files.write(dir.resolve("b.jrl"), bytes)
      val log   = new TestLog(dir)
      val error =
        try intercept[CorruptLogException](log.init())
        finally log.close()
      error.getMessage should (include("a.jrl").and(include("b.jrl")))
    }
  }

  test("an open segment ID that is not above every sealed ID fails recovery and leaves open untouched") {
    for
      openId <- List(2L, 5L)
      data = fixture(version = 2, id = openId)
      bytes <- List(data.bytes, data.bytes.take(data.footerStart))
    do
      withDirectory { dir =>
        Files.write(dir.resolve("existing.jrl"), fixture(version = 2, id = 5L).bytes)
        crashedRun(dir, bytes)
        val log = new TestLog(dir)
        try intercept[CorruptLogException](log.init())
        finally log.close()
        Files.readAllBytes(dir.resolve("open")).toVector shouldBe bytes.toVector
      }
  }

  test("the next segment ID must not overflow") {
    withDirectory { dir =>
      Files.write(dir.resolve("last.jrl"), fixture(version = 2, id = Long.MaxValue).bytes)
      val log = new TestLog(dir)
      try intercept[ArithmeticException](log.init())
      finally log.close()
    }
  }

  test("a failed init leaves close nothing to do, so the running marker stays and the next init recovers") {
    withDirectory { dir =>
      val data = fixture(version = 2, id = 3L)
      val open = data.bytes.take(data.footerStart)
      crashedRun(dir, open)
      var encodes = 0
      val log     = new TestLog(dir):
        override protected def footerCodec(version: Int): Codec[Int] =
          new Codec[Int]:
            def encode(value: Int): Try[Array[Byte]] =
              encodes += 1
              // The first encode only checks that it works; the writer's fails while the writer is open.
              if encodes == 1 then appCodec(version).encode(value)
              else scala.util.Failure(new IOException("encoding failed"))
            def decode(buffer: ByteBuffer) = appCodec(version).decode(buffer)
      try intercept[IOException](log.init())
      finally log.close()
      encodes shouldBe 2
      Files.exists(dir.resolve("running")) shouldBe true
      Files.readAllBytes(dir.resolve("open")).toVector shouldBe open.toVector
      val retry = new TestLog(dir)
      try sealedSegment(retry.init()).entries shouldBe List(10, 20, 30)
      finally retry.close()
    }
  }

  private def decoderOnly(version: Int): Codec[Int] =
    val decoder = appCodec(version)
    new Codec[Int]:
      def encode(value: Int): Try[Array[Byte]] =
        scala.util.Failure(new AssertionError(s"encoder for version $version must not be used"))
      def decode(buffer: ByteBuffer) = decoder.decode(buffer)

  private def historicalDecoder(version: Int): Codec[Int] =
    if version != 1 then appCodec(version) else decoderOnly(version)

  test("old entries are sealed under a footer of their own codec version without re-encoding them") {
    withDirectory { dir =>
      val data = fixture(version = 1, id = 3L)
      crashedRun(dir, data.bytes.take(data.footerStart))
      val log = new TestLog(dir, clock = new Clock.Mock(200.millis)):
        override protected def entryCodec(version: Int)  = decoderOnly(version)
        override protected def headerCodec(version: Int) = decoderOnly(version)
      try
        val segment = sealedSegment(log.init())
        // The fixture's footer is the one recovery builds, sealed at 200 and encoded with version 1.
        Files.readAllBytes(dir.resolve("segment-60.jrl")).toVector shouldBe data.bytes.toVector
        segment.codecVersion shouldBe 1
        segment.id shouldBe 3L
        segment.entries shouldBe List(10, 20, 30)
      finally log.close()
    }
  }

  test("a complete old footer is renamed unchanged without using historical encoders") {
    withDirectory { dir =>
      val data = fixture(version = 1, id = 3L)
      crashedRun(dir, data.bytes)
      val log = new TestLog(dir):
        override protected def entryCodec(version: Int)  = historicalDecoder(version)
        override protected def headerCodec(version: Int) = historicalDecoder(version)
        override protected def footerCodec(version: Int) = historicalDecoder(version)
      log.init()
      try Files.readAllBytes(dir.resolve("segment-60.jrl")).toVector shouldBe data.bytes.toVector
      finally log.close()
    }
  }

  test("codecs that report another version than the one looked up are rejected") {
    class MislabeledLog(dir: Path) extends TestLog(dir):
      override protected def lookupCodec(version: Int) =
        super
          .lookupCodec(version)
          .map(found => logCodec(version + 1, found.headerCodec, found.entryCodec, found.footerCodec))
    withDirectory { dir =>
      // Recovery reads the open file through the lookup.
      crashedRun(dir, fixture().bytes)
      val log = new MislabeledLog(dir)
      try intercept[IllegalStateException](log.init())
      finally log.close()
      new MislabeledLog(dir).readSegment(channel(fixture().bytes)).failed.get shouldBe an[IllegalStateException]
    }
  }

  test("recovery checks the sealed name before changing open") {
    val data     = fixture(version = 2, id = 3L)
    val original = data.bytes.take(data.footerStart + 5)
    // The recovered entries name the segment segment-60, which a sealed segment already has.
    withDirectory { dir =>
      Files.write(dir.resolve("segment-60.jrl"), fixture(version = 2, id = 0L).bytes)
      crashedRun(dir, original)
      val log = new TestLog(dir)
      try intercept[java.nio.file.FileAlreadyExistsException](log.init())
      finally log.close()
      Files.readAllBytes(dir.resolve("open")).toVector shouldBe original.toVector
    }
    withDirectory { dir =>
      crashedRun(dir, original)
      val log = new TestLog(dir):
        override protected def fileName(state: Int) = "../escape"
      try intercept[IllegalArgumentException](log.init())
      finally log.close()
      Files.readAllBytes(dir.resolve("open")).toVector shouldBe original.toVector
    }
  }

  test("an old footer encoding failure fails recovery before open is changed, and the next init seals it") {
    withDirectory { dir =>
      val data     = fixture(version = 1, id = 3L)
      val original = data.bytes.take(data.footerStart + 5)
      crashedRun(dir, original)
      val failure = new IOException("encoding failed")
      val log     = new TestLog(dir):
        override protected def footerCodec(version: Int): Codec[Int] =
          if version == 2 then appCodec(version)
          else
            new Codec[Int]:
              def encode(value: Int): Try[Array[Byte]] = scala.util.Failure(failure)
              def decode(buffer: ByteBuffer)           = appCodec(1).decode(buffer)
      try intercept[IOException](log.init()) shouldBe failure
      finally log.close()
      Files.readAllBytes(dir.resolve("open")).toVector shouldBe original.toVector
      val retry = new TestLog(dir)
      try sealedSegment(retry.init()).entries shouldBe List(10, 20, 30)
      finally retry.close()
    }
  }
