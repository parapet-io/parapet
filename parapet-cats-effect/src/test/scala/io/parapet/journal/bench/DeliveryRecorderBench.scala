package io.parapet.journal.bench

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.parallel.*
import io.parapet.cats.CatsEffectParapetRuntime
import io.parapet.effect.{Effect, EffectFiber}
import io.parapet.journal.*
import io.parapet.{Event, ProcessRef}

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Delivery-journal benchmark: N concurrent admitters against `JournalStore`, on cats-effect `IO`.
  *
  * Every run selects one [[JournalWriteMode]] and one [[JournalDurability]]. Admission latency measures only the
  * selected write-mode contract. Throughput includes a final durability fence, making buffered and write-ahead runs
  * comparable as completed workloads.
  *
  * Run:
  * {{{
  * sbt "parapetCatsEffect/Test/runMain io.parapet.journal.bench.DeliveryRecorderBench"
  *
  * sbt -Dbench.threads=1,8,32 -Dbench.modes=Buffered,WriteAhead \
  *     -Dbench.durabilities=HostCrash,ProcessCrash -Dbench.ops=5000 \
  *     "parapetCatsEffect/Test/runMain io.parapet.journal.bench.DeliveryRecorderBench"
  * }}}
  *
  * Properties: bench.threads, bench.modes, bench.durabilities, bench.ops, bench.warmup, bench.batchSize,
  * bench.segmentBytes, bench.payloadBytes, bench.dir, bench.keep.
  *
  * Nothing in any `src/main` is modified. Store append count and latency are observed through a delegating
  * [[JournalStore]]. Under [[JournalDurability.HostCrash]], each append includes one `force(true)`.
  */
object DeliveryRecorderBench:

  private val parapetRuntime               = CatsEffectParapetRuntime.default
  private given effectInstance: Effect[IO] = parapetRuntime.effect

  private val envelopeIds = new AtomicLong(0L)

  // ----------------------------------------------------------------- bench event

  final case class Payload(bytes: Array[Byte]) extends Event

  private object PayloadCodec extends EventCodec:
    val tag: String                                                 = "bench.payload"
    val version: Int                                                = 1
    def encode(event: Event): Try[Array[Byte]]                      = Try(event.asInstanceOf[Payload].bytes)
    def decode(encodedVersion: Int, bytes: Array[Byte]): Try[Event] = Try(Payload(bytes))

  private def registry: EventCodecRegistry = EventCodecRegistry(classOf[Payload] -> PayloadCodec)

  // ----------------------------------------------------------------- admitters

  private trait Admitter:
    def admit(draft: JournalDraft): IO[Long]
    def flush: IO[Unit]
    def close: IO[Unit]

  final private class RecorderAdmitter(
      recorder: DeliveryRecorder[IO],
      writer: EffectFiber[IO, Unit]
  ) extends Admitter:
    def admit(draft: JournalDraft): IO[Long] = recorder.admit(draft)
    def flush: IO[Unit]                      = recorder.flush()
    def close: IO[Unit]                      = recorder.close().flatMap(_ => writer.join)

  // ----------------------------------------------------------------- observation

  /** One observed `Store.append`. */
  final case class AppendSample(entries: Int, nanos: Long)

  final class CountingStore(delegate: JournalStore[IO]) extends JournalStore[IO]:
    private val lock    = new Object
    private val samples = ArrayBuffer.empty[AppendSample]

    def observed: Vector[AppendSample] = lock.synchronized(samples.toVector)
    def reset(): Unit                  = lock.synchronized(samples.clear())

    def append(entries: Vector[JournalEntry]): IO[Unit] =
      IO.monotonic.flatMap { started =>
        delegate.append(entries).flatMap { _ =>
          IO.monotonic.flatMap { ended =>
            IO.delay {
              val sample = AppendSample(entries.size, (ended - started).toNanos)
              lock.synchronized(samples += sample)
              ()
            }
          }
        }
      }

    def read(afterSeq: Long): IO[Vector[JournalEntry]] = delegate.read(afterSeq)
    def maxSeq: IO[Option[Long]]                       = delegate.maxSeq
    def maxEnvelopeId: IO[Option[Long]]                = delegate.maxEnvelopeId
    def truncate(upToSeq: Long): IO[Unit]              = delegate.truncate(upToSeq)

  // ----------------------------------------------------------------- run

  final case class RunConfig(
      threads: Int,
      writeMode: JournalWriteMode,
      durability: JournalDurability,
      batchSize: Int,
      maxSegmentBytes: Long,
      payloadBytes: Int,
      opsPerFiber: Int,
      warmupPerFiber: Int,
      dataRoot: Path,
      keepFiles: Boolean
  )

  final case class RunResult(
      config: RunConfig,
      dir: Path,
      wallNanos: Long,
      latencies: Array[Long],
      appends: Vector[AppendSample],
      sealedFiles: Int,
      openFiles: Int
  ):
    def ops: Int             = latencies.length
    def opsPerSecond: Double = ops.toDouble / (wallNanos.toDouble / 1e9)

  final private case class Fixture(
      dir: Path,
      store: CountingStore,
      admitter: Admitter,
      sender: ProcessRef.Unknown,
      receiver: ProcessRef.Unknown,
      payload: Payload
  )

  private def setup(cfg: RunConfig): IO[Fixture] =
    IO.delay {
      val dir   = Files.createTempDirectory(cfg.dataRoot, "bench-journal-")
      val store = new CountingStore(
        new JournalStoreLocal[IO](
          JournalStoreLocal.Config(
            dataDir = dir,
            maxSegmentBytes = cfg.maxSegmentBytes,
            maxEntryBytes = JournalStoreLocal.DefaultMaxEntryBytes,
            durability = cfg.durability
          )
        )
      )
      (dir, store)
    }.flatMap { (dir, store) =>
      recorderAdmitter(store, cfg).map { admitter =>
        Fixture(
          dir,
          store,
          admitter,
          ProcessRef[Event]("bench-sender"),
          ProcessRef[Event]("bench-receiver"),
          Payload(Array.fill(cfg.payloadBytes)(0x2a.toByte))
        )
      }
    }

  private def recorderAdmitter(store: JournalStore[IO], cfg: RunConfig): IO[Admitter] =
    val recorder = DeliveryRecorder.fresh[IO](
      store,
      registry,
      JournalConfig(batchSize = cfg.batchSize, writeMode = cfg.writeMode, durability = cfg.durability)
    )
    effectInstance.start(recorder.runWriter).map(writer => new RecorderAdmitter(recorder, writer))

  private def admitLoop(fixture: Fixture, index: Int, count: Int, out: Array[Long]): IO[Unit] =
    val base = index * count

    def step(i: Int): IO[Unit] =
      if i >= count then IO.unit
      else
        IO.delay(
          JournalDraft(envelopeIds.incrementAndGet(), fixture.sender, fixture.receiver, 0L, fixture.payload)
        ).flatMap { draft =>
          IO.monotonic.flatMap { started =>
            fixture.admitter.admit(draft).flatMap { _ =>
              IO.monotonic.flatMap { ended =>
                IO.delay { out(base + i) = (ended - started).toNanos }.flatMap(_ => step(i + 1))
              }
            }
          }
        }

    step(0)

  /** Runs every admitter concurrently. `parSequence_` cancels the siblings on the first failure - without that, a
    * failing admitter propagates while the others keep writing into a directory the run's finalizer is deleting, and
    * the resulting NoSuchFileException buries the real error.
    */
  private def parAll(tasks: Vector[IO[Unit]]): IO[Unit] =
    tasks.toList.parSequence_

  private def phase(
      fixture: Fixture,
      threads: Int,
      perFiber: Int,
      out: Array[Long]
  ): IO[Unit] =
    if perFiber <= 0 then IO.unit
    else
      val tasks = (0 until threads).toVector.map(index => admitLoop(fixture, index, perFiber, out))
      parAll(tasks)

  private def program(cfg: RunConfig): IO[RunResult] =
    setup(cfg).flatMap { fixture =>
      val warmLatencies = new Array[Long](math.max(1, cfg.threads * cfg.warmupPerFiber))
      val latencies     = new Array[Long](cfg.threads * cfg.opsPerFiber)
      val measure       =
        for
          _       <- phase(fixture, cfg.threads, cfg.warmupPerFiber, warmLatencies)
          _       <- fixture.admitter.flush
          _       <- IO.delay(fixture.store.reset())
          started <- IO.monotonic
          _       <- phase(fixture, cfg.threads, cfg.opsPerFiber, latencies)
          // Include the durability fence in wall-clock throughput. Admission percentiles remain limited to admit().
          _        <- fixture.admitter.flush
          ended    <- IO.monotonic
          observed <- IO.delay(fixture.store.observed)
          _        <- IO.delay {
            val storedEntries = observed.foldLeft(0L)((total, sample) => total + sample.entries)
            require(
              storedEntries == latencies.length.toLong,
              s"observed $storedEntries stored entries for ${latencies.length} admissions"
            )
          }
          _     <- fixture.admitter.close
          names <- IO.delay(listing(fixture.dir))
        yield RunResult(
          cfg,
          fixture.dir,
          (ended - started).toNanos,
          latencies,
          observed,
          names.count(name => name.endsWith(".jrnl")),
          names.count(name => name.endsWith(".open"))
        )
      val cleanup =
        fixture.admitter.close.attempt.flatMap { _ =>
          IO.delay(if !cfg.keepFiles then deleteRecursively(fixture.dir)).attempt.void
        }
      measure.guarantee(cleanup)
    }

  def run(cfg: RunConfig): RunResult = program(cfg).unsafeRunSync()

  // ----------------------------------------------------------------- raw fsync floor

  /** Mirrors a host-crash append: open, seek to end, write, force data and metadata, then close. */
  def fsyncFloor(dir: Path, samples: Int, bytes: Int): Array[Long] =
    val path   = dir.resolve("fsync-probe")
    val buffer = Array.fill(bytes)(0x2a.toByte)
    val out    = new Array[Long](samples)
    var i      = 0
    while i < samples do
      val started = System.nanoTime()
      val channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      try
        channel.position(channel.size())
        channel.write(ByteBuffer.wrap(buffer))
        channel.force(true)
      finally channel.close()
      out(i) = System.nanoTime() - started
      i += 1
    Files.deleteIfExists(path)
    out

  /** Same write, but the channel is opened once and held - what `appendToFile` would cost if `DeliveryLog` kept the
    * active segment's channel open instead of reopening per batch. No force, so the delta against `openCloseFloor` is
    * the open/close pair alone.
    */
  def heldChannelFloor(dir: Path, samples: Int, bytes: Int): Array[Long] =
    val path    = dir.resolve("held-probe")
    val buffer  = Array.fill(bytes)(0x2a.toByte)
    val out     = new Array[Long](samples)
    val channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    try
      var i = 0
      while i < samples do
        val started = System.nanoTime()
        channel.position(channel.size())
        channel.write(ByteBuffer.wrap(buffer))
        out(i) = System.nanoTime() - started
        i += 1
    finally channel.close()
    Files.deleteIfExists(path)
    out

  /** Open, seek, write, close - no force. Isolates the per-append fixed cost from the barrier. */
  def openCloseFloor(dir: Path, samples: Int, bytes: Int): Array[Long] =
    val path   = dir.resolve("openclose-probe")
    val buffer = Array.fill(bytes)(0x2a.toByte)
    val out    = new Array[Long](samples)
    var i      = 0
    while i < samples do
      val started = System.nanoTime()
      val channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      try
        channel.position(channel.size())
        channel.write(ByteBuffer.wrap(buffer))
      finally channel.close()
      out(i) = System.nanoTime() - started
      i += 1
    Files.deleteIfExists(path)
    out

  private def percentile(sorted: Array[Long], quantile: Double): Long =
    if sorted.isEmpty then 0L
    else sorted(math.max(0, math.min(sorted.length - 1, math.round(quantile * (sorted.length - 1)).toInt)))

  private def micros(nanos: Long): Double = nanos.toDouble / 1000.0

  private def listing(dir: Path): Vector[String] =
    val stream = Files.list(dir)
    try stream.iterator().asScala.map(path => path.getFileName.toString).toVector
    finally stream.close()

  private def deleteRecursively(dir: Path): Unit =
    if Files.exists(dir) then
      val stream = Files.walk(dir)
      val paths  =
        try stream.iterator().asScala.toVector
        finally stream.close()
      paths.reverse.foreach(path => Files.deleteIfExists(path))

  // ----------------------------------------------------------------- reporting

  private def reportHeader(): Unit =
    println(
      "%-10s %-12s %7s %8s %9s %10s %10s %10s %10s %8s %9s %8s %8s %7s %5s".format(
        "mode",
        "durability",
        "fibers",
        "ops",
        "durOps/s",
        "admP50us",
        "admP99us",
        "admP999us",
        "admMaxus",
        "appends",
        "ent/apnd",
        "apndP50",
        "apndP99",
        "sealed",
        "open"
      )
    )

  private def reportRow(result: RunResult): Unit =
    val sorted = result.latencies.clone()
    java.util.Arrays.sort(sorted)
    val appendNanos = result.appends.map(sample => sample.nanos).toArray
    java.util.Arrays.sort(appendNanos)
    val totalEntries = result.appends.map(sample => sample.entries.toLong).sum
    val entriesPer   = if result.appends.isEmpty then 0.0 else totalEntries.toDouble / result.appends.size
    println(
      "%-10s %-12s %7d %8d %9.0f %10.1f %10.1f %10.1f %10.1f %8d %9.2f %8.1f %8.1f %7d %5d".format(
        result.config.writeMode.toString,
        result.config.durability.toString,
        result.config.threads,
        result.ops,
        result.opsPerSecond,
        micros(percentile(sorted, 0.50)),
        micros(percentile(sorted, 0.99)),
        micros(percentile(sorted, 0.999)),
        micros(percentile(sorted, 1.0)),
        result.appends.size,
        entriesPer,
        micros(percentile(appendNanos, 0.50)),
        micros(percentile(appendNanos, 0.99)),
        result.sealedFiles,
        result.openFiles
      )
    )

  // ----------------------------------------------------------------- main

  private def prop(name: String, default: String): String = System.getProperty(name, default)

  private def csv(value: String): Vector[String] =
    value.split(",").toVector.map(part => part.trim).filter(part => part.nonEmpty)

  private def parseWriteMode(name: String): JournalWriteMode = name match
    case "WriteAhead" => JournalWriteMode.WriteAhead
    case "Buffered"   => JournalWriteMode.Buffered
    case other        => throw new IllegalArgumentException(s"unknown bench mode '$other'")

  private def parseDurability(name: String): JournalDurability = name match
    case "HostCrash"    => JournalDurability.HostCrash
    case "ProcessCrash" => JournalDurability.ProcessCrash
    case other          => throw new IllegalArgumentException(s"unknown bench durability '$other'")

  def main(args: Array[String]): Unit =
    val root = Path.of(prop("bench.dir", System.getProperty("java.io.tmpdir")))
    Files.createDirectories(root)

    val threadCounts = csv(prop("bench.threads", "1,2,4,8,16,32")).map(_.toInt)
    val modes        = csv(prop("bench.modes", "Buffered,WriteAhead")).map(parseWriteMode)
    val durabilities = csv(prop("bench.durabilities", "HostCrash,ProcessCrash")).map(parseDurability)
    val ops          = prop("bench.ops", "2000").toInt
    val warmup       = prop("bench.warmup", "200").toInt
    val batchSize    = prop("bench.batchSize", "1024").toInt
    val segmentBytes = prop("bench.segmentBytes", JournalStoreLocal.DefaultMaxSegmentBytes.toString).toLong
    val payloadBytes = prop("bench.payloadBytes", "64").toInt
    val keepFiles    = prop("bench.keep", "false").toBoolean

    println(s"java       ${System.getProperty("java.version")} on ${System.getProperty("os.name")}")
    println(s"cpus       ${Runtime.getRuntime.availableProcessors()}")
    println(s"dataRoot   $root  (each run writes to its own bench-journal-* subdirectory)")
    println(s"segment    $segmentBytes bytes   payload $payloadBytes bytes   batchSize $batchSize")
    println(s"ops        $ops per fiber (warmup $warmup)")

    val floor = fsyncFloor(root, 200, payloadBytes + 64)
    java.util.Arrays.sort(floor)
    val floorP50 = percentile(floor, 0.50)
    println(
      f"force(true)  p50=${micros(floorP50)}%.1fus p99=${micros(percentile(floor, 0.99))}%.1fus" +
        f"  => ${1e9 / math.max(1L, floorP50).toDouble}%.0f ops/s if one force per message" +
        "   [macOS: F_FULLFSYNC, Linux: fsync]"
    )
    if micros(floorP50) < 20.0 then
      println("  NOTE: p50 under 20us means fsync is not reaching stable storage here (tmpfs, or macOS")
      println("        without F_FULLFSYNC), or force was disabled. Read this as a control, not the real number.")
    val held = heldChannelFloor(root, 2000, payloadBytes + 64)
    java.util.Arrays.sort(held)
    val openClose = openCloseFloor(root, 2000, payloadBytes + 64)
    java.util.Arrays.sort(openClose)
    val heldP50      = percentile(held, 0.50)
    val openCloseP50 = percentile(openClose, 0.50)
    println(
      f"held channel p50=${micros(heldP50)}%.1fus   open/close p50=${micros(openCloseP50)}%.1fus" +
        f"   => open+close costs ${micros(openCloseP50 - heldP50)}%.1fus per append (no force in either)"
    )
    println()

    reportHeader()
    durabilities.foreach { durability =>
      modes.foreach { mode =>
        threadCounts.foreach { threads =>
          val result = run(
            RunConfig(
              threads = threads,
              writeMode = mode,
              durability = durability,
              batchSize = batchSize,
              maxSegmentBytes = segmentBytes,
              payloadBytes = payloadBytes,
              opsPerFiber = ops,
              warmupPerFiber = warmup,
              dataRoot = root,
              keepFiles = keepFiles
            )
          )
          reportRow(result)
          if keepFiles then println(s"    segments: ${result.dir}")
        }
      }
    }

    parapetRuntime.close()
