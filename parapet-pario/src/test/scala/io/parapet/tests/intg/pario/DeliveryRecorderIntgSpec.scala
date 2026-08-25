package io.parapet.tests.intg.pario

import io.parapet.journal.*
import io.parapet.effect.ParIO
import io.parapet.effect.ParIO.given
import io.parapet.effect.{Effect, EffectFiber}
import io.parapet.journal.{DeliveryRecorder, EventCodec, EventCodecRegistry, JournalConfig, JournalDraft, JournalEntry, JournalSegment, JournalStore, JournalStoreLocal}
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, CyclicBarrier, LinkedBlockingQueue, TimeUnit, TimeoutException}
import scala.concurrent.duration.FiniteDuration
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

  private def draft(id: Long) = JournalDraft(id, ref, ref, 0L, E(id))

  private def storeAt(dir: Path) = new JournalStoreLocal[ParIO](JournalStoreLocal.Config(dir))

  private def seqsOnDisk(dir: Path): Vector[Long] = storeAt(dir).read(0L).run().map(_.seq)

  // [min, max] of every segment file, sorted by min, read straight from the filenames.
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

  private def startThread[A](name: String)(body: => A): Running[A] =
    val result = new AtomicReference[Try[A]]()
    val thread = new Thread(() => result.set(Try(body)), name)
    thread.start()
    Running(thread, result)

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

  private def take[A](queue: LinkedBlockingQueue[A], clue: String): A =
    Option(queue.poll(awaitSeconds, TimeUnit.SECONDS)).getOrElse(fail(clue))

  private def stopThreads(threads: Seq[Thread]): Unit =
    threads.foreach(_.join(1000L))
    threads.filter(_.isAlive).foreach(_.interrupt())

  private def assertSameFailure(expected: Throwable)(body: => Any): Unit =
    val actual = intercept[Throwable](body)
    withClue(s"expected the original failure instance, got $actual") {
      (actual eq expected) shouldBe true
    }

  // In-memory store with a controllable first append and a trace captured in actual append-call order.
  final private class ControlledStore(
      firstResult: Either[Throwable, Unit] = Right(()),
      gateFirst: Boolean = true
  ) extends JournalStore[ParIO]:
    val firstEntered = new CountDownLatch(if gateFirst then 1 else 0)

    private val releaseFirst   = new CountDownLatch(if gateFirst then 1 else 0)
    private val calls          = new AtomicInteger(0)
    private val attempts       = new CopyOnWriteArrayList[Vector[JournalEntry]]()
    private val durableBatches = new CopyOnWriteArrayList[Vector[JournalEntry]]()

    override def append(segment: JournalSegment): ParIO[Unit] =
      ParIO.blocking {
        val entries = segment.entries
        val call    = calls.getAndIncrement()
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

    override def truncate(upToSeq: Long): ParIO[Unit] = ParIO.unit

    def release(): Unit = releaseFirst.countDown()

    def appendAttempts: Vector[Vector[JournalEntry]] = attempts.asScala.toVector

  // Observes recorder-owned blocking and sleeping without observing the fake store, which uses ParIO directly.
  final private class ProbeEffect(
      delegate: Effect[ParIO],
      deferredWaitEntered: CountDownLatch = new CountDownLatch(0),
      drainPollEntered: CountDownLatch = new CountDownLatch(0)
  ) extends Effect[ParIO]:

    override def pure[A](value: A): ParIO[A] = delegate.pure(value)

    extension [A](fa: ParIO[A])
      def flatMap[B](f: A => ParIO[B]): ParIO[B]              = fa.flatMap(f)
      override def map[B](f: A => B): ParIO[B]                = fa.map(f)
      def handleErrorWith(f: Throwable => ParIO[A]): ParIO[A] = fa.handleErrorWith(f)

    override def delay[A](thunk: => A): ParIO[A] = delegate.delay(thunk)

    override def blocking[A](thunk: => A): ParIO[A] =
      delegate.blocking {
        deferredWaitEntered.countDown()
        thunk
      }

    override def suspend[A](thunk: => ParIO[A]): ParIO[A] = delegate.suspend(thunk)

    override def raiseError[A](error: Throwable): ParIO[A] = delegate.raiseError(error)

    override def sleep(duration: FiniteDuration): ParIO[Unit] =
      delegate.delay(drainPollEntered.countDown()).flatMap(_ => delegate.sleep(duration))

    override def start[A](fa: ParIO[A]): ParIO[EffectFiber[ParIO, A]] = delegate.start(fa)

    override def startBlocking[A](fa: ParIO[A]): ParIO[EffectFiber[ParIO, A]] = delegate.startBlocking(fa)

    override def race[A, B](left: ParIO[A], right: ParIO[B]): ParIO[Either[A, B]] = delegate.race(left, right)

    override def guarantee[A](fa: ParIO[A])(finalizer: ParIO[Unit]): ParIO[A] = delegate.guarantee(fa)(finalizer)

  final private case class AdmissionScenario(
      name: String,
      workers: Int,
      perWorker: Int,
      batchSize: Int,
      samples: Int
  )

  private val admissionMatrix = Vector(
    AdmissionScenario("single-worker", workers = 1, perWorker = 32, batchSize = 1, samples = 1),
    AdmissionScenario("contended-size-one", workers = 6, perWorker = 40, batchSize = 1, samples = 2),
    AdmissionScenario("contended-batches", workers = 8, perWorker = 80, batchSize = 8, samples = 3),
    AdmissionScenario("partial-tail", workers = 5, perWorker = 47, batchSize = 31, samples = 2)
  )

  test("single-writer admits produce contiguous, non-overlapping local segments") {
    val dir      = Files.createTempDirectory("recorder-single")
    val recorder = DeliveryRecorder.fresh[ParIO](storeAt(dir), registry, JournalConfig(batchSize = 4))
    (1L to 10L).foreach(i => recorder.admit(draft(i)).run())
    recorder.close().run()

    seqsOnDisk(dir) shouldBe (1L to 10L).toVector
    assertDisjointAndIncreasing(fileRanges(dir))
  }

  test("concurrent admission preserves every position and publishes batches in FIFO order") {
    admissionMatrix.foreach { scenario =>
      (1 to scenario.samples).foreach { sample =>
        val store    = new ControlledStore(gateFirst = false)
        val recorder = DeliveryRecorder.fresh[ParIO](
          store,
          registry,
          JournalConfig(batchSize = scenario.batchSize, maxRetries = 0)
        )
        val barrier = new CyclicBarrier(scenario.workers)
        val nextId  = new AtomicLong(0L)
        val workers = (1 to scenario.workers).map { worker =>
          startThread(s"${scenario.name}-$sample-worker-$worker") {
            barrier.await(awaitSeconds, TimeUnit.SECONDS)
            Vector.fill(scenario.perWorker) {
              val id = nextId.incrementAndGet()
              id -> recorder.admit(draft(id)).run()
            }
          }
        }

        val returned = awaitAll(workers, s"${scenario.name} sample $sample").flatMap(_.get)
        recorder.close().run()

        val total         = scenario.workers * scenario.perWorker
        val actual        = store.appendAttempts
        val expectedSeqs  = (1L to total.toLong).toVector
        val remainder     = total % scenario.batchSize
        val expectedSizes =
          Vector.fill(total / scenario.batchSize)(scenario.batchSize) ++
            Option.when(remainder > 0)(remainder)

        withClue(s"${scenario.name}, sample $sample: ") {
          verifyPublication(actual, expectedSeqs)
          actual.map(_.size) shouldBe expectedSizes
          returned.size shouldBe total
          returned.map(_._1).distinct.size shouldBe total
          actual.flatten.map(entry => entry.id -> entry.seq).toMap shouldBe returned.toMap
        }
      }
    }
  }

  test("several batches queued behind one owner retain exact FIFO publication order") {
    val store       = new ControlledStore()
    val waiterProbe = new CountDownLatch(3)
    val probe       = new ProbeEffect(summon[Effect[ParIO]], deferredWaitEntered = waiterProbe)
    val recorder    =
      DeliveryRecorder.fresh[ParIO](store, registry, JournalConfig(batchSize = 2, maxRetries = 0))(using probe)
    val completed = new LinkedBlockingQueue[Try[Long]]()
    var running   = Vector.empty[Running[Long]]

    try
      recorder.admit(draft(1L)).run() shouldBe 1L
      val owner = startThread("fifo-owner")(recorder.admit(draft(2L)).run())
      running :+= owner
      await(store.firstEntered, "the first batch never reached the store")

      val later = (3L to 9L).map { id =>
        startThread(s"fifo-admit-$id") {
          val outcome = Try(recorder.admit(draft(id)).run())
          completed.put(outcome)
          outcome.get
        }
      }.toVector
      running ++= later

      Vector
        .fill(4)(take(completed, "four buffered admits did not return"))
        .foreach(outcome => outcome.isSuccess shouldBe true)
      await(waiterProbe, "three sealed batches were not waiting behind the owner")

      store.release()
      awaitAll(running, "queued FIFO admissions").foreach(_.get)
      recorder.close().run()

      store.appendAttempts.map(_.map(_.seq)) shouldBe Vector(
        Vector(1L, 2L),
        Vector(3L, 4L),
        Vector(5L, 6L),
        Vector(7L, 8L),
        Vector(9L)
      )
    finally
      store.release()
      stopThreads(running.map(_.thread))
  }

  test("flush joins an unresolved full batch even when the active buffer is empty") {
    val store       = new ControlledStore()
    val waiterProbe = new CountDownLatch(1)
    val probe       = new ProbeEffect(summon[Effect[ParIO]], deferredWaitEntered = waiterProbe)
    val recorder    =
      DeliveryRecorder.fresh[ParIO](store, registry, JournalConfig(batchSize = 1, maxRetries = 0))(using probe)
    var threads = Vector.empty[Thread]

    try
      val owner = startThread("flush-owner")(recorder.admit(draft(1L)).run())
      threads :+= owner.thread
      await(store.firstEntered, "the owner never entered append")

      val flush = startThread("flush-waiter")(recorder.flush().run())
      threads :+= flush.thread
      await(waiterProbe, "flush did not wait for the unresolved FIFO tail")
      flush.thread.isAlive shouldBe true

      store.release()
      awaitOne(owner, "owner admit").get shouldBe 1L
      awaitOne(flush, "flush barrier").get
      recorder.close().run()

      store.appendAttempts.map(_.map(_.seq)) shouldBe Vector(Vector(1L))
    finally
      store.release()
      stopThreads(threads)
  }

  test("one store failure wakes every sealed waiter, discards the tail, and remains sticky") {
    val boom        = new RuntimeException("disk full")
    val store       = new ControlledStore(firstResult = Left(boom))
    val waiterProbe = new CountDownLatch(3)
    val probe       = new ProbeEffect(summon[Effect[ParIO]], deferredWaitEntered = waiterProbe)
    val recorder    =
      DeliveryRecorder.fresh[ParIO](store, registry, JournalConfig(batchSize = 2, maxRetries = 0))(using probe)
    val completed = new LinkedBlockingQueue[Try[Long]]()
    var running   = Vector.empty[Running[Long]]

    try
      recorder.admit(draft(1L)).run() shouldBe 1L
      val owner = startThread("failure-owner")(recorder.admit(draft(2L)).run())
      running :+= owner
      await(store.firstEntered, "the failing append was not entered")

      val later = (3L to 9L).map { id =>
        startThread(s"failure-admit-$id") {
          val outcome = Try(recorder.admit(draft(id)).run())
          completed.put(outcome)
          outcome.get
        }
      }.toVector
      running ++= later

      Vector
        .fill(4)(take(completed, "four buffered admits did not return before failure"))
        .foreach(outcome => outcome.isSuccess shouldBe true)
      await(waiterProbe, "three sealed batches were not awaiting the failing owner")

      store.release()
      val ownerOutcome = awaitOne(owner, "failing owner")
      ownerOutcome match
        case Failure(error) => (error eq boom) shouldBe true
        case Success(value) => fail(s"failing owner unexpectedly returned $value")

      val laterOutcomes = awaitAll(later, "failure waiters")
      laterOutcomes.count(_.isSuccess) shouldBe 4
      val waiterFailures = laterOutcomes.collect { case Failure(error) => error }
      waiterFailures.size shouldBe 3
      waiterFailures.foreach(error => (error eq boom) shouldBe true)

      store.appendAttempts.map(_.map(_.seq)) shouldBe Vector(Vector(1L, 2L))
      assertSameFailure(boom)(recorder.admit(draft(10L)).run())
      assertSameFailure(boom)(recorder.advanceSequence().run())
      assertSameFailure(boom)(recorder.flush().run())
      assertSameFailure(boom)(recorder.close().run())
    finally
      store.release()
      stopThreads(running.map(_.thread))
  }

  test("concurrent closes share one durability barrier and close is idempotent") {
    val store      = new ControlledStore()
    val closeProbe = new CountDownLatch(1)
    val probe      = new ProbeEffect(summon[Effect[ParIO]], drainPollEntered = closeProbe)
    val recorder   =
      DeliveryRecorder.fresh[ParIO](store, registry, JournalConfig(batchSize = 4, maxRetries = 0))(using probe)
    var running = Vector.empty[Running[Unit]]

    try
      recorder.admit(draft(1L)).run() shouldBe 1L
      val first = startThread("first-close")(recorder.close().run())
      running :+= first
      await(store.firstEntered, "close did not publish the partial tail")

      val second = startThread("second-close")(recorder.close().run())
      running :+= second
      await(closeProbe, "the concurrent close did not join the active drain")
      first.thread.isAlive shouldBe true
      second.thread.isAlive shouldBe true

      store.release()
      awaitAll(running, "concurrent closes").foreach(_.get)
      recorder.close().run()

      store.appendAttempts.map(_.map(_.seq)) shouldBe Vector(Vector(1L))
      an[IllegalStateException] should be thrownBy recorder.admit(draft(2L)).run()
      an[IllegalStateException] should be thrownBy recorder.advanceSequence().run()
      an[IllegalStateException] should be thrownBy recorder.flush().run()
    finally
      store.release()
      stopThreads(running.map(_.thread))
  }

  test("sequenceOnly creates gaps without overlapping publication ranges") {
    val store    = new ControlledStore(gateFirst = false)
    val recorder = DeliveryRecorder.fresh[ParIO](store, registry, JournalConfig(batchSize = 2, maxRetries = 0))

    recorder.admit(draft(1L)).run() shouldBe 1L
    recorder.advanceSequence().run() shouldBe 2L
    recorder.admit(draft(2L)).run() shouldBe 3L
    recorder.close().run()

    store.appendAttempts.map(_.map(_.seq)) shouldBe Vector(Vector(1L, 3L))
  }

  test("sequence exhaustion rejects overflow without losing the final valid position") {
    val store    = new ControlledStore(gateFirst = false)
    val recorder = DeliveryRecorder.resume[ParIO](
      store,
      highWater = Long.MaxValue - 1L,
      registry = registry,
      config = JournalConfig(batchSize = 4, maxRetries = 0)
    )

    recorder.admit(draft(1L)).run() shouldBe Long.MaxValue
    an[IllegalStateException] should be thrownBy recorder.admit(draft(2L)).run()
    an[IllegalStateException] should be thrownBy recorder.advanceSequence().run()
    store.appendAttempts shouldBe empty

    recorder.flush().run()
    recorder.close().run()
    store.appendAttempts.map(_.map(_.seq)) shouldBe Vector(Vector(Long.MaxValue))
  }

  test("reopening continues past the supplied high-water instead of overwriting segments") {
    val dir = Files.createTempDirectory("recorder-reopen")

    val first = DeliveryRecorder.fresh[ParIO](storeAt(dir), registry, JournalConfig(batchSize = 4))
    (1L to 6L).foreach(i => first.admit(draft(i)).run())
    first.close().run()

    val highWater = storeAt(dir).maxSeq.run().getOrElse(0L)
    val second    = DeliveryRecorder.resume[ParIO](
      storeAt(dir),
      highWater,
      registry,
      JournalConfig(batchSize = 4)
    )
    (1L to 5L).foreach(i => second.admit(draft(i)).run())
    second.close().run()

    seqsOnDisk(dir) shouldBe (1L to 11L).toVector
    assertDisjointAndIncreasing(fileRanges(dir))
  }

  test("construction rejects invalid parameters") {
    an[IllegalArgumentException] should be thrownBy DeliveryRecorder.fresh[ParIO](
      storeAt(Files.createTempDirectory("r")),
      registry,
      JournalConfig(batchSize = 0)
    )
    an[IllegalArgumentException] should be thrownBy DeliveryRecorder.fresh[ParIO](
      storeAt(Files.createTempDirectory("r")),
      registry,
      JournalConfig(maxRetries = -1)
    )
    an[IllegalArgumentException] should be thrownBy DeliveryRecorder.resume[ParIO](
      storeAt(Files.createTempDirectory("r")),
      highWater = -1L
    )
  }
