package io.parapet.journal

import io.parapet.TestUtils.{TestIO, given}
import io.parapet.journal.Recorder.{Config, Entry, Store}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList, CountDownLatch, CyclicBarrier, TimeUnit}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

class RecorderSpec extends AnyFunSuite:

  private val timeoutMillis = TimeUnit.SECONDS.toMillis(5L)

  final private case class Running[A](thread: Thread, result: AtomicReference[Try[A]])

  final private class RecordingStore extends Store[TestIO, String]:
    private val writes = new CopyOnWriteArrayList[Vector[Entry[String]]]()

    def append(entries: Vector[Entry[String]]): TestIO[Unit] =
      TestIO.delay {
        writes.add(entries)
        ()
      }

    def appended: Vector[Vector[Entry[String]]] = writes.asScala.toVector

  final private class GatedStore(result: Either[Throwable, Unit]) extends Store[TestIO, String]:
    private val entered = new CountDownLatch(1)
    private val release = new CountDownLatch(1)
    private val writes  = new CopyOnWriteArrayList[Vector[Entry[String]]]()

    def append(entries: Vector[Entry[String]]): TestIO[Unit] =
      TestIO.delay {
        entered.countDown()
        if !release.await(timeoutMillis, TimeUnit.MILLISECONDS) then
          throw new AssertionError("test did not release the store append")
        result match
          case Left(error) => throw error
          case Right(_)    =>
            writes.add(entries)
            ()
      }

    def awaitAppend(): Unit =
      entered.await(timeoutMillis, TimeUnit.MILLISECONDS) shouldBe true

    def releaseAppend(): Unit = release.countDown()

    def appended: Vector[Vector[Entry[String]]] = writes.asScala.toVector

  private def recorder(batchSize: Int, store: Store[TestIO, String], startId: Long = 0L): Recorder[TestIO, String] =
    Recorder(store, Config(startId = startId, batchSize = batchSize))

  private def startThread[A](name: String)(body: => A): Running[A] =
    val result = new AtomicReference[Try[A]]()
    val thread = new Thread(
      () =>
        val outcome =
          try Success(body)
          catch case error: Throwable => Failure(error)
        result.set(outcome),
      name
    )
    thread.start()
    Running(thread, result)

  private def startWriter(recorder: Recorder[TestIO, String]): Running[Unit] =
    startThread("recorder-writer")(recorder.runWriter.unsafeRun())

  private def awaitResult[A](running: Running[A], clue: String): Try[A] =
    running.thread.join(timeoutMillis)
    if running.thread.isAlive then
      running.thread.interrupt()
      fail(s"$clue did not terminate")
    Option(running.result.get()).getOrElse(fail(s"$clue terminated without reporting an outcome"))

  private def awaitWaiting(running: Running[?], clue: String): Unit =
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
    var waiting  = false
    while !waiting && running.result.get() == null && System.nanoTime() < deadline do
      val state = running.thread.getState
      waiting = state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING
      if !waiting then Thread.sleep(1L)

    if running.result.get() != null then fail(s"$clue terminated instead of waiting")
    waiting shouldBe true

  private def awaitCondition(clue: String)(condition: => Boolean): Unit =
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
    while !condition && System.nanoTime() < deadline do Thread.sleep(1L)
    withClue(clue)(condition shouldBe true)

  private def stop(running: Running[?]*): Unit =
    running.foreach { run =>
      if run.thread.isAlive then run.thread.interrupt()
    }
    running.foreach(_.thread.join(1000L))

  private def failureOf[A](result: Try[A]): Throwable = result match
    case Failure(error) => error
    case Success(value) => fail(s"expected failure, got $value")

  private def assertSameFailure(expected: Throwable)(body: => Any): Unit =
    val actual = intercept[Throwable](body)
    withClue(s"expected the original failure instance, got $actual") {
      (actual eq expected) shouldBe true
    }

  test("buffered admissions publish automatically when the active batch becomes full") {
    val store  = new RecordingStore
    val target = recorder(batchSize = 4, store = store)
    val writer = startWriter(target)

    try
      target.admit("a").unsafeRun() shouldBe 1L
      target.admit("b").unsafeRun() shouldBe 2L
      target.admit("c").unsafeRun() shouldBe 3L
      store.appended shouldBe empty

      target.admit("d").unsafeRun() shouldBe 4L
      awaitCondition("the full batch was not published") {
        store.appended.nonEmpty
      }

      target.close().unsafeRun()
      awaitResult(writer, "writer").get
      store.appended shouldBe Vector(
        Vector(Entry(1L, "a"), Entry(2L, "b"), Entry(3L, "c"), Entry(4L, "d"))
      )
    finally stop(writer)
  }

  test("durable admission leaves the requested active batch open until the writer seals it") {
    val store   = new RecordingStore
    val target  = recorder(batchSize = 4, store = store)
    val durable = startThread("durable-admission")(target.admitAndFlush("a").unsafeRun())
    var writer  = Option.empty[Running[Unit]]

    try
      awaitWaiting(durable, "durable admission")
      target.admit("b").unsafeRun() shouldBe 2L
      target.admit("c").unsafeRun() shouldBe 3L

      val runningWriter = startWriter(target)
      writer = Some(runningWriter)
      awaitResult(durable, "durable admission").get shouldBe 1L
      target.close().unsafeRun()
      awaitResult(runningWriter, "writer").get

      store.appended shouldBe Vector(Vector(Entry(1L, "a"), Entry(2L, "b"), Entry(3L, "c")))
    finally stop((Vector(durable) ++ writer.toVector)*)
  }

  test("flush publishes the current partial batch and waits for the writer") {
    val store  = new RecordingStore
    val target = recorder(batchSize = 4, store = store)

    target.admit("a").unsafeRun() shouldBe 1L
    target.admit("b").unsafeRun() shouldBe 2L
    val flushing = startThread("recorder-flush")(target.flush().unsafeRun())
    var writer   = Option.empty[Running[Unit]]

    try
      awaitWaiting(flushing, "flush")
      val runningWriter = startWriter(target)
      writer = Some(runningWriter)
      awaitResult(flushing, "flush").get

      store.appended shouldBe Vector(Vector(Entry(1L, "a"), Entry(2L, "b")))
      target.close().unsafeRun()
      awaitResult(runningWriter, "writer").get
    finally stop((Vector(flushing) ++ writer.toVector)*)
  }

  test("concurrent closes share one completion and close remains idempotent") {
    val store  = new GatedStore(Right(()))
    val target = recorder(batchSize = 4, store = store)
    val writer = startWriter(target)

    target.admit("a").unsafeRun() shouldBe 1L
    val firstClose = startThread("first-close")(target.close().unsafeRun())
    var secondClose = Option.empty[Running[Unit]]

    try
      store.awaitAppend()
      val second = startThread("second-close")(target.close().unsafeRun())
      secondClose = Some(second)
      awaitWaiting(firstClose, "first close")
      awaitWaiting(second, "second close")

      store.releaseAppend()
      awaitResult(firstClose, "first close").get
      awaitResult(second, "second close").get
      awaitResult(writer, "writer").get
      target.close().unsafeRun()

      store.appended shouldBe Vector(Vector(Entry(1L, "a")))
      an[IllegalStateException] should be thrownBy target.admit("b").unsafeRun()
      an[IllegalStateException] should be thrownBy target.advanceId().unsafeRun()
      an[IllegalStateException] should be thrownBy target.flush().unsafeRun()
    finally
      store.releaseAppend()
      stop((Vector(writer, firstClose) ++ secondClose.toVector)*)
  }

  test("publication preserves admission order and sequence gaps across batches") {
    val store  = new RecordingStore
    val target = recorder(batchSize = 2, store = store)
    val writer = startWriter(target)

    try
      target.admit("a").unsafeRun() shouldBe 1L
      target.admit("b").unsafeRun() shouldBe 2L
      target.advanceId().unsafeRun() shouldBe 3L
      target.admit("c").unsafeRun() shouldBe 4L
      target.admit("d").unsafeRun() shouldBe 5L
      target.close().unsafeRun()
      awaitResult(writer, "writer").get

      store.appended shouldBe Vector(
        Vector(Entry(1L, "a"), Entry(2L, "b")),
        Vector(Entry(4L, "c"), Entry(5L, "d"))
      )
    finally stop(writer)
  }

  test("concurrent buffered admissions receive unique positions and publish in position order") {
    val workers   = 8
    val perWorker = 50
    val total     = workers * perWorker
    val store     = new RecordingStore
    val target    = recorder(batchSize = 8, store = store)
    val writer    = startWriter(target)
    val barrier   = new CyclicBarrier(workers)
    val assigned  = new ConcurrentHashMap[String, Long]()
    val admissions = (0 until workers).toVector.map { worker =>
      startThread(s"admit-$worker") {
        barrier.await(timeoutMillis, TimeUnit.MILLISECONDS)
        (0 until perWorker).foreach { index =>
          val value = s"$worker-$index"
          assigned.put(value, target.admit(value).unsafeRun())
        }
      }
    }

    try
      admissions.foreach(running => awaitResult(running, running.thread.getName).get)
      target.close().unsafeRun()
      awaitResult(writer, "writer").get

      val entries = store.appended.flatten
      entries.map(_.id) shouldBe (1L to total.toLong).toVector
      entries.map(entry => entry.data -> entry.id).toMap shouldBe assigned.asScala.toMap
      store.appended.foreach(batch => batch.size should (be > 0 and be <= 8))
    finally stop((Vector(writer) ++ admissions)*)
  }

  test("a store failure fails the writer and every durability waiter with the same error") {
    val boom   = new RuntimeException("disk full")
    val store  = new GatedStore(Left(boom))
    val target = recorder(batchSize = 2, store = store)
    val writer = startWriter(target)

    target.admit("a").unsafeRun() shouldBe 1L
    val firstDurable = startThread("first-durable")(target.admitDurable("b").unsafeRun())
    var secondDurable = Option.empty[Running[Long]]
    var flushing      = Option.empty[Running[Unit]]

    try
      store.awaitAppend()
      val second = startThread("second-durable")(target.admitDurable("c").unsafeRun())
      secondDurable = Some(second)
      awaitWaiting(second, "second durable admission")
      val flush = startThread("failure-flush")(target.flush().unsafeRun())
      flushing = Some(flush)
      awaitWaiting(flush, "flush")

      store.releaseAppend()
      (failureOf(awaitResult(firstDurable, "first durable admission")) eq boom) shouldBe true
      (failureOf(awaitResult(second, "second durable admission")) eq boom) shouldBe true
      (failureOf(awaitResult(flush, "flush")) eq boom) shouldBe true
      (failureOf(awaitResult(writer, "writer")) eq boom) shouldBe true

      assertSameFailure(boom)(target.admit("d").unsafeRun())
      assertSameFailure(boom)(target.advanceId().unsafeRun())
      assertSameFailure(boom)(target.flush().unsafeRun())
      assertSameFailure(boom)(target.close().unsafeRun())
      store.appended shouldBe empty
    finally
      store.releaseAppend()
      stop((Vector(writer, firstDurable) ++ secondDurable.toVector ++ flushing.toVector)*)
  }

  test("interrupting the writer fails the recorder instead of stranding future operations") {
    val store  = new RecordingStore
    val target = recorder(batchSize = 4, store = store)
    val writer = startWriter(target)

    awaitWaiting(writer, "writer")
    writer.thread.interrupt()
    val error = failureOf(awaitResult(writer, "writer"))
    error shouldBe a[InterruptedException]

    assertSameFailure(error)(target.admit("a").unsafeRun())
    assertSameFailure(error)(target.advanceId().unsafeRun())
    assertSameFailure(error)(target.flush().unsafeRun())
    assertSameFailure(error)(target.close().unsafeRun())
  }

  test("a second writer is rejected without disturbing the active writer") {
    val store  = new RecordingStore
    val target = recorder(batchSize = 4, store = store)
    val writer = startWriter(target)

    try
      awaitWaiting(writer, "writer")
      val error = the[IllegalStateException] thrownBy target.runWriter.unsafeRun()
      error.getMessage should include("already running or has terminated")

      target.admit("a").unsafeRun() shouldBe 1L
      target.close().unsafeRun()
      awaitResult(writer, "writer").get
      store.appended shouldBe Vector(Vector(Entry(1L, "a")))
    finally stop(writer)
  }

  test("identifier exhaustion preserves and publishes the final valid position") {
    val store  = new RecordingStore
    val target = recorder(batchSize = 4, store = store, startId = Long.MaxValue - 1L)
    val writer = startWriter(target)

    try
      target.admit("last").unsafeRun() shouldBe Long.MaxValue
      an[IllegalStateException] should be thrownBy target.admit("overflow").unsafeRun()
      an[IllegalStateException] should be thrownBy target.advanceId().unsafeRun()

      target.close().unsafeRun()
      awaitResult(writer, "writer").get
      store.appended shouldBe Vector(Vector(Entry(Long.MaxValue, "last")))
    finally stop(writer)
  }

  test("continueAfter advances only the lower identifier high-water") {
    val store  = new RecordingStore
    val target = recorder(batchSize = 4, store = store, startId = 5L)
    val writer = startWriter(target)

    try
      target.continueAfter(3L)
      target.admit("a").unsafeRun() shouldBe 6L
      target.continueAfter(10L)
      target.admit("b").unsafeRun() shouldBe 11L
      target.close().unsafeRun()
      awaitResult(writer, "writer").get

      store.appended shouldBe Vector(Vector(Entry(6L, "a"), Entry(11L, "b")))
    finally stop(writer)
  }

  test("construction rejects an invalid starting identifier or batch size") {
    val store = new RecordingStore

    an[IllegalArgumentException] should be thrownBy recorder(batchSize = 1, store = store, startId = -1L)
    an[IllegalArgumentException] should be thrownBy recorder(batchSize = 0, store = store)
  }
