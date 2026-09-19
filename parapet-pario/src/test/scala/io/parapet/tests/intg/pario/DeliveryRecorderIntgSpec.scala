package io.parapet.tests.intg.pario

import io.parapet.effect.ParIO
import io.parapet.effect.ParIO.given
import io.parapet.journal.{
  DeliveryRecorder,
  EventCodec,
  EventCodecRegistry,
  JournalConfig,
  JournalDraft,
  JournalEntry,
  JournalStore,
  JournalStoreLocal,
  JournalWriteMode
}
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, CyclicBarrier, TimeUnit, TimeoutException}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

class DeliveryRecorderIntgSpec extends AnyFunSuite:

  private val ref          = ProcessRef[Event]("a")
  private val awaitSeconds = 10L

  private case class E(id: Long) extends Event

  private object ECodec extends EventCodec:
    val tag: String                            = "e"
    val version: Int                           = 1
    def encode(event: Event): Try[Array[Byte]] = event match
      case E(id) => Success(java.nio.ByteBuffer.allocate(8).putLong(id).array())
      case other => Failure(new IllegalArgumentException(s"cannot encode $other"))
    def decode(version: Int, bytes: Array[Byte]): Try[Event] =
      Success(E(java.nio.ByteBuffer.wrap(bytes).getLong))

  private val registry = EventCodecRegistry(classOf[E] -> ECodec)

  extension [A](fa: ParIO[A]) private def run(): A = fa.unsafeRunSync()

  private def draft(id: Long): JournalDraft = JournalDraft(id, ref, ref, 0L, E(id))

  private def storeAt(dir: Path): JournalStoreLocal[ParIO] =
    new JournalStoreLocal[ParIO](JournalStoreLocal.Config(dir))

  private def seqsOnDisk(dir: Path): Vector[Long] =
    storeAt(dir).read(0L).run().map(_.seq)

  private def fileRanges(dir: Path): Vector[(Long, Long)] =
    val listing = Files.list(dir)
    try
      listing
        .iterator()
        .asScala
        .map(_.getFileName.toString)
        .filter(_.endsWith(".jrnl"))
        .map { name =>
          val Array(min, max) = name.stripSuffix(".jrnl").split('-'): @unchecked
          (min.toLong, max.toLong)
        }
        .toVector
        .sortBy(_._1)
    finally listing.close()

  private def assertDisjointAndIncreasing(ranges: Vector[(Long, Long)]): Unit =
    ranges.foreach { case (min, max) => min should be <= max }
    ranges.sliding(2).foreach {
      case Vector((_, previousMax), (nextMin, _)) => previousMax should be < nextMin
      case _                                      => ()
    }

  private def verifyPublication(actual: Vector[Vector[JournalEntry]], expectedSeqs: Vector[Long]): Unit =
    actual.foreach { batch =>
      batch should not be empty
      batch.map(_.seq) shouldBe batch.map(_.seq).sorted
    }
    actual.flatten.map(_.seq) shouldBe expectedSeqs
    actual.sliding(2).foreach {
      case Vector(previous, next) => previous.last.seq should be < next.head.seq
      case _                      => ()
    }

  final private case class Running[A](thread: Thread, result: AtomicReference[Try[A]])

  final private case class ActiveRecorder(
      recorder: DeliveryRecorder[ParIO],
      writer: Running[Unit]
  )

  private def startThread[A](name: String)(body: => A): Running[A] =
    val result = new AtomicReference[Try[A]]()
    val thread = new Thread(() => result.set(Try(body)), name)
    thread.start()
    Running(thread, result)

  private def startRecorder(recorder: DeliveryRecorder[ParIO]): ActiveRecorder =
    ActiveRecorder(recorder, startThread("delivery-recorder-writer")(recorder.runWriter.run()))

  private def fresh(
      store: JournalStore[ParIO],
      config: JournalConfig = JournalConfig.default
  ): ActiveRecorder =
    startRecorder(DeliveryRecorder.fresh(store, registry, config))

  private def resume(
      store: JournalStore[ParIO],
      highWater: Long,
      config: JournalConfig = JournalConfig.default
  ): ActiveRecorder =
    startRecorder(DeliveryRecorder.resume(store, highWater, registry, config))

  private def awaitAll[A](running: Seq[Running[A]], clue: String): Vector[Try[A]] =
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(awaitSeconds)
    running.foreach { run =>
      val remaining = deadline - System.nanoTime()
      if remaining > 0 then run.thread.join(math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)))
    }

    val live = running.filter(_.thread.isAlive)
    if live.nonEmpty then
      live.foreach(_.thread.interrupt())
      fail(s"$clue did not terminate: ${live.map(_.thread.getName).mkString(", ")}")

    running.toVector.map { run =>
      Option(run.result.get()).getOrElse(fail(s"${run.thread.getName} terminated without reporting an outcome"))
    }

  private def awaitOne[A](running: Running[A], clue: String): Try[A] =
    awaitAll(Vector(running), clue).head

  private def await(latch: CountDownLatch, clue: String): Unit =
    withClue(clue) {
      latch.await(awaitSeconds, TimeUnit.SECONDS) shouldBe true
    }

  private def stopThreads(threads: Seq[Thread]): Unit =
    threads.filter(_.isAlive).foreach(_.interrupt())
    threads.foreach(_.join(1000L))

  private def stop(active: ActiveRecorder): Unit =
    stopThreads(Vector(active.writer.thread))

  private def close(active: ActiveRecorder): Unit =
    active.recorder.close().run()
    awaitOne(active.writer, "delivery recorder writer").get

  private def failureOf[A](result: Try[A]): Throwable = result match
    case Failure(error) => error
    case Success(value) => fail(s"expected failure, got $value")

  private def assertSameFailure(expected: Throwable)(body: => Any): Unit =
    val actual = intercept[Throwable](body)
    withClue(s"expected the original failure instance, got $actual") {
      (actual eq expected) shouldBe true
    }

  final private class ControlledStore(
      firstResult: Either[Throwable, Unit] = Right(()),
      gateFirst: Boolean = true
  ) extends JournalStore[ParIO]:
    val firstEntered = new CountDownLatch(if gateFirst then 1 else 0)

    private val releaseFirst   = new CountDownLatch(if gateFirst then 1 else 0)
    private val calls          = new AtomicInteger(0)
    private val attempts       = new CopyOnWriteArrayList[Vector[JournalEntry]]()
    private val durableBatches = new CopyOnWriteArrayList[Vector[JournalEntry]]()
    private val truncations    = new AtomicInteger(0)

    override def append(entries: Vector[JournalEntry]): ParIO[Unit] =
      ParIO.blocking {
        val call = calls.getAndIncrement()
        attempts.add(entries)
        if call == 0 && gateFirst then
          firstEntered.countDown()
          if !releaseFirst.await(awaitSeconds, TimeUnit.SECONDS) then
            throw new TimeoutException("test did not release the first append")
          firstResult match
            case Left(error) => throw error
            case Right(())   => ()
        durableBatches.add(entries)
        ()
      }

    override def read(afterSeq: Long): ParIO[Vector[JournalEntry]] =
      ParIO.delay(durableBatches.asScala.toVector.flatten.filter(_.seq > afterSeq).sortBy(_.seq))

    override def maxSeq: ParIO[Option[Long]] =
      ParIO.delay(durableBatches.asScala.iterator.flatMap(_.iterator).map(_.seq).maxOption)

    override def maxEnvelopeId: ParIO[Option[Long]] =
      ParIO.delay(
        durableBatches.asScala.iterator.flatMap(_.iterator).flatMap(entry => Iterator(entry.id, entry.cause)).maxOption
      )

    override def truncate(upToSeq: Long): ParIO[Unit] =
      ParIO.delay {
        truncations.incrementAndGet()
        ()
      }

    def release(): Unit = releaseFirst.countDown()

    def appendAttempts: Vector[Vector[JournalEntry]] = attempts.asScala.toVector

    def truncateCount: Int = truncations.get()

  test("buffered mode may return before a partial batch is durable") {
    val store  = new ControlledStore(gateFirst = false)
    val active = fresh(store, JournalConfig.default.copy(batchSize = 4))

    try
      active.recorder.admit(draft(1L)).run() shouldBe 1L
      store.appendAttempts shouldBe empty

      active.recorder.flush().run()
      store.appendAttempts.map(_.map(_.seq)) shouldBe Vector(Vector(1L))
      close(active)
    finally stop(active)
  }

  test("write-ahead mode waits for the admitted delivery to become durable") {
    val store     = new ControlledStore()
    val active    = fresh(store, JournalConfig(batchSize = 4, writeMode = JournalWriteMode.WriteAhead))
    var admission = Option.empty[Running[Long]]

    try
      val running = startThread("write-ahead-admit")(active.recorder.admit(draft(1L)).run())
      admission = Some(running)
      await(store.firstEntered, "write-ahead admission did not reach the store")

      running.thread.isAlive shouldBe true
      store.maxSeq.run() shouldBe None

      store.release()
      awaitOne(running, "write-ahead admission").get shouldBe 1L
      store.maxSeq.run() shouldBe Some(1L)
      store.appendAttempts.map(_.map(_.seq)) shouldBe Vector(Vector(1L))
      close(active)
    finally
      store.release()
      stopThreads(admission.toVector.map(_.thread))
      stop(active)
  }

  test("truncate publishes the buffered tail before discarding covered segments") {
    val dir    = Files.createTempDirectory("recorder-truncate")
    val active = fresh(storeAt(dir), JournalConfig(batchSize = 4))

    try
      active.recorder.admit(draft(1L)).run() shouldBe 1L
      seqsOnDisk(dir) shouldBe empty

      active.recorder.truncate(1L).run()
      active.recorder.read(0L).run() shouldBe empty
      active.recorder.maxSeq.run() shouldBe Some(1L)

      active.recorder.admit(draft(2L)).run() shouldBe 2L
      close(active)
      seqsOnDisk(dir) shouldBe Vector(2L)
    finally stop(active)
  }

  test("truncate does not reach the store when flushing the buffered tail fails") {
    val boom       = new RuntimeException("disk full")
    val store      = new ControlledStore(firstResult = Left(boom))
    val active     = fresh(store, JournalConfig(batchSize = 4))
    var truncation = Option.empty[Running[Unit]]

    try
      active.recorder.admit(draft(1L)).run() shouldBe 1L
      val running = startThread("truncate-after-flush")(active.recorder.truncate(1L).run())
      truncation = Some(running)
      await(store.firstEntered, "truncate did not start flushing the buffered tail")

      store.release()
      (failureOf(awaitOne(running, "truncate after failed flush")) eq boom) shouldBe true
      (failureOf(awaitOne(active.writer, "delivery recorder writer")) eq boom) shouldBe true
      store.truncateCount shouldBe 0

      assertSameFailure(boom)(active.recorder.admit(draft(2L)).run())
      assertSameFailure(boom)(active.recorder.advanceSequence().run())
      assertSameFailure(boom)(active.recorder.flush().run())
      assertSameFailure(boom)(active.recorder.close().run())
    finally
      store.release()
      stopThreads(truncation.toVector.map(_.thread))
      stop(active)
  }

  test("concurrent admission preserves every position and publishes batches in FIFO order") {
    val workersCount = 8
    val perWorker    = 80
    val batchSize    = 8
    val total        = workersCount * perWorker
    val store        = new ControlledStore(gateFirst = false)
    val active       = fresh(store, JournalConfig(batchSize = batchSize))
    val barrier      = new CyclicBarrier(workersCount)
    val nextId       = new AtomicLong(0L)
    val workers      = (1 to workersCount).map { worker =>
      startThread(s"admit-$worker") {
        barrier.await(awaitSeconds, TimeUnit.SECONDS)
        Vector.fill(perWorker) {
          val id = nextId.incrementAndGet()
          id -> active.recorder.admit(draft(id)).run()
        }
      }
    }

    try
      val returned = awaitAll(workers, "concurrent admissions").flatMap(_.get)
      close(active)

      val actual       = store.appendAttempts
      val expectedSeqs = (1L to total.toLong).toVector
      verifyPublication(actual, expectedSeqs)
      actual.map(_.size) shouldBe Vector.fill(total / batchSize)(batchSize)
      returned.size shouldBe total
      returned.map(_._1).distinct.size shouldBe total
      actual.flatten.map(entry => entry.id -> entry.seq).toMap shouldBe returned.toMap
    finally
      stopThreads(workers.map(_.thread))
      stop(active)
  }

  test("sequence-only admissions create gaps without changing publication order") {
    val store  = new ControlledStore(gateFirst = false)
    val active = fresh(store, JournalConfig(batchSize = 2))

    try
      active.recorder.admit(draft(1L)).run() shouldBe 1L
      active.recorder.advanceSequence().run() shouldBe 2L
      active.recorder.admit(draft(2L)).run() shouldBe 3L
      close(active)

      store.appendAttempts.map(_.map(_.seq)) shouldBe Vector(Vector(1L, 3L))
    finally stop(active)
  }

  test("sequence exhaustion rejects overflow without losing the final valid position") {
    val store  = new ControlledStore(gateFirst = false)
    val active = resume(store, Long.MaxValue - 1L, JournalConfig(batchSize = 4))

    try
      active.recorder.admit(draft(1L)).run() shouldBe Long.MaxValue
      an[IllegalStateException] should be thrownBy active.recorder.admit(draft(2L)).run()
      an[IllegalStateException] should be thrownBy active.recorder.advanceSequence().run()
      store.appendAttempts shouldBe empty

      active.recorder.flush().run()
      close(active)
      store.appendAttempts.map(_.map(_.seq)) shouldBe Vector(Vector(Long.MaxValue))
    finally stop(active)
  }

  test("reopening continues past the supplied high-water instead of overwriting segments") {
    val dir   = Files.createTempDirectory("recorder-reopen")
    val first = fresh(storeAt(dir), JournalConfig(batchSize = 4))

    try
      (1L to 6L).foreach(i => first.recorder.admit(draft(i)).run())
      close(first)
    finally stop(first)

    val highWater = storeAt(dir).maxSeq.run().getOrElse(0L)
    val second    = resume(storeAt(dir), highWater, JournalConfig(batchSize = 4))

    try
      (1L to 5L).foreach(i => second.recorder.admit(draft(i)).run())
      close(second)
    finally stop(second)

    seqsOnDisk(dir) shouldBe (1L to 11L).toVector
    assertDisjointAndIncreasing(fileRanges(dir))
  }

  test("construction rejects invalid parameters") {
    an[IllegalArgumentException] should be thrownBy DeliveryRecorder.fresh[ParIO](
      storeAt(Files.createTempDirectory("r")),
      registry,
      JournalConfig(batchSize = 0)
    )
    an[IllegalArgumentException] should be thrownBy DeliveryRecorder.resume[ParIO](
      storeAt(Files.createTempDirectory("r")),
      highWater = -1L
    )
  }
