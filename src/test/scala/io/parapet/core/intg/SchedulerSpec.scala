package io.parapet.core.intg

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import io.parapet.core.Parapet.{Process, ProcessRef, ioEffectInterpreter, ioFlowInterpreter}
import io.parapet.core.Event
import io.parapet.core.Event._
import io.parapet.core.Queue
import io.parapet.implicits._
import io.parapet.core.Scheduler
import io.parapet.core.Scheduler._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow.{empty => emptyFlow, _}
import io.parapet.core.intg.SchedulerSpec._
import io.parapet.core.intg.SchedulerSpec.TaskProcessingTime._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Random

class SchedulerSpec extends FunSuite {

  test("work stealing #1") {

    val config = SchedulerConfig(
      queueSize = 5,
      numberOfWorkers = 2,
      workerQueueSize = 5,
      taskSubmissionTimeout = 1.second,
      workerTaskDequeueTimeout = 1.second,
      maxRedeliveryRetries = 0,
      redeliveryInitialDelay = 0.seconds)


    val eventStore = new EventStore[TestEvent]
    val p1 = createProcess(eventStore, 5.seconds) // slowProcess
    val p2 = createProcess(eventStore)
    val p3 = createProcess(eventStore)
    val p4 = createProcess(eventStore)

    // p1 and p2 assigned to w1
    // p3 and p4 assigned to w2

    val tasks: ListBuffer[IODeliver] = ListBuffer()
    tasks += Deliver[IO](Envelope(ProcessRef.SystemRef, TestEvent(1), p1.ref))
    (2 until 6).foreach { i =>
      tasks += Deliver[IO](Envelope(ProcessRef.SystemRef, TestEvent(i), p2.ref))
    }

    run(config, Array(p1, p2, p3, p4), tasks, eventStore)

    eventStore.print()

    verifyEvents(tasks, eventStore)
  }

  test("work stealing #2") {

    val config = SchedulerConfig(
      queueSize = 5,
      numberOfWorkers = 2,
      workerQueueSize = 5,
      taskSubmissionTimeout = 1.second,
      workerTaskDequeueTimeout = 1.second,
      maxRedeliveryRetries = 0,
      redeliveryInitialDelay = 0.seconds)


    val eventStore = new EventStore[TestEvent]
    val p1 = createProcess(eventStore, 5.seconds) // slowProcess
    val p2 = createProcess(eventStore)

    // p1 assigned w1
    // p2 assigned to w2

    val tasks: ListBuffer[IODeliver] = ListBuffer()
    tasks += Deliver[IO](Envelope(ProcessRef.SystemRef, TestEvent(1), p1.ref))
    (2 until 6).foreach { i =>
      tasks += Deliver[IO](Envelope(ProcessRef.SystemRef, TestEvent(i), p1.ref))
    }

    run(config, Array(p1, p2), tasks, eventStore)
    eventStore.print()

    verifyEvents(tasks, eventStore)

  }

  test("work stealing with single worker") {

    val config = SchedulerConfig(
      queueSize = 5,
      numberOfWorkers = 1,
      workerQueueSize = 5,
      taskSubmissionTimeout = 1.second,
      workerTaskDequeueTimeout = 1.second,
      maxRedeliveryRetries = 0,
      redeliveryInitialDelay = 0.seconds)

    val eventStore = new EventStore[TestEvent]
    val p1 = createProcess(eventStore)

    // p1 assigned w1
    // p2 assigned to w2

    val tasks = List()

    run(config, Array(p1), tasks, eventStore, 10.seconds)
    eventStore.print()

    verifyEvents(tasks, eventStore)
  }

  test("random spec 0.25, random workload gem") {
    val config = SchedulerConfig(
      queueSize = 1000,
      numberOfWorkers = 5,
      workerQueueSize = 20,
      taskSubmissionTimeout = 1.second,
      workerTaskDequeueTimeout = 1.second,
      maxRedeliveryRetries = 0,
      redeliveryInitialDelay = 0.seconds)

    val numberOfEvents = 1000
    val numberOfProcesses = 10
    val eventStore = new EventStore[TestEvent]
    val processes = createProcesses(numberOfProcesses, instant, 0.5, range(2.second, 3.seconds), eventStore)
    randomSpec(config, numberOfEvents, processes, eventStore, WorkloadGen.Random)
}

  test("Random task gen") {
    val numberOfEvents = 5
    val numberOfProcesses = 5
    val processes = (0 until numberOfProcesses).map(_ => dummyProcess).toArray
    val actualTasks = WorkloadGen.Random.createTasks(numberOfEvents, processes)

    actualTasks.size shouldBe numberOfEvents

    val actualEvents = groupEventsByProcess(actualTasks).values.flatten.map(toTestEvent(_).seqNumber).toList.sorted
    actualEvents shouldBe (1 to numberOfEvents)
  }

  test("Batch task gen") {
    val batchSize = 5
    val numberOfProcesses = 5
    val totalTasks = batchSize * numberOfProcesses
    val processes = (0 until numberOfProcesses).map(_ => dummyProcess).toArray
    val actualTasks = WorkloadGen.Batch.createTasks(batchSize, processes)

    actualTasks.size shouldBe totalTasks

    val groupedByEvents = groupEventsByProcess(actualTasks)

    groupedByEvents(processes(0).ref).map(e => toTestEvent(e).seqNumber) shouldBe (1 to 5)
    groupedByEvents(processes(1).ref).map(e => toTestEvent(e).seqNumber) shouldBe (6 to 10)
    groupedByEvents(processes(2).ref).map(e => toTestEvent(e).seqNumber) shouldBe (11 to 15)
    groupedByEvents(processes(3).ref).map(e => toTestEvent(e).seqNumber) shouldBe (16 to 20)
    groupedByEvents(processes(4).ref).map(e => toTestEvent(e).seqNumber) shouldBe (21 to 25)
  }

  test("create processes #1") {
    val n = 5
    val eventStore = new EventStore[TestEvent]
    val processes = createProcesses(n, TaskProcessingTime.instant, 0.5, TaskProcessingTime.instant, eventStore)
    processes.length shouldBe n
  }

  test("create processes #2") {
    val n = 5
    val eventStore = new EventStore[TestEvent]
    val processes = createProcesses(n, TaskProcessingTime.instant, 0.25, TaskProcessingTime.instant, eventStore)
    processes.length shouldBe n
  }

  test("create processes #3") {
    val n = 5
    val eventStore = new EventStore[TestEvent]
    val processes = createProcesses(n, TaskProcessingTime.instant, 0.75, TaskProcessingTime.instant, eventStore)
    processes.length shouldBe n
  }

  test("create processes #4") {
    val n = 5
    val eventStore = new EventStore[TestEvent]
    val processes = createProcesses(n, TaskProcessingTime.instant, 0, TaskProcessingTime.instant, eventStore)
    processes.length shouldBe n
  }

  test("create processes #5") {
    val n = 5
    val eventStore = new EventStore[TestEvent]
    val processes = createProcesses(n, TaskProcessingTime.instant, 1, TaskProcessingTime.instant, eventStore)
    processes.length shouldBe n
  }

  def run[A <: Event](config: SchedulerConfig,
                      processes: Array[Process[IO]],
                      tasks: Seq[IOTask],
                      eventStore: EventStore[A], shutdownDelay: FiniteDuration = 0.millis): Seq[IOTask] = {
    val executor = Executors.newFixedThreadPool(10)
    val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)
    implicit val ctx: ContextShift[IO] = IO.contextShift(ec)
    implicit val timer: Timer[IO] = IO.timer(ec)
    val program = for {
      taskQueue <- Queue.bounded[IO, IOTask](config.queueSize)
      interpreter <- IO.pure(ioFlowInterpreter(taskQueue)(ctx, timer) or ioEffectInterpreter)
      scheduler <- Scheduler[IO](config, processes, taskQueue, interpreter)
      fiber <- scheduler.run.start
      _ <- submitAll(scheduler, tasks)
      _ <- eventStore.awaitSize(tasks.size)
      _ <- IO.sleep(shutdownDelay)
      _ <- IO(println("stop scheduler"))  >> fiber.cancel
    } yield tasks

    val executedTasks = program.unsafeRunSync()
    executor.shutdownNow()
    executedTasks
  }

  test("events order assertion") {
    assertEventsOrder(Seq(TestEvent(1), TestEvent(2), TestEvent(3)))
    assertThrows[IllegalArgumentException] {
      assertEventsOrder(Seq(TestEvent(2), TestEvent(1), TestEvent(3)))
    }
    assertThrows[IllegalArgumentException] {
      assertEventsOrder(Seq(TestEvent(1), TestEvent(3), TestEvent(2)))
    }
  }

  def randomSpec(config: SchedulerConfig, numberOfEvents: Int,
                 processes: Array[Process[IO]], eventStore: EventStore[TestEvent], wt: WorkloadGen, samples: Int = 1): Unit = {
    (0 until samples).foreach { i =>
      println(s"random spec test #$i")
      val tasks = wt.createTasks(numberOfEvents, processes)
      println(tasks.size)
      run(config, processes, tasks, eventStore)
      eventStore.print()

      verifyEvents(tasks, eventStore)
    }
  }

}

object SchedulerSpec {

  type IOTask = Task[IO]
  type IODeliver = Deliver[IO]
  type IOProcess = Process[IO]
  type TaskId = Int // todo add id to the Task

  def dummyProcess: IOProcess = new Process[IO] {
    override val handle: Receive = {
      case _ => emptyFlow
    }
  }

  case class TestEvent(seqNumber: Int) extends Event

  class EventStore[A <: Event] {
    type EventList = ListBuffer[A]
    private val eventMap: java.util.Map[ProcessRef, EventList] =
      new java.util.concurrent.ConcurrentHashMap[ProcessRef, EventList]()

    private val sizeRef = new AtomicInteger()

    def add(pRef: ProcessRef, event: A): Unit = {
      sizeRef.incrementAndGet()
      eventMap.computeIfAbsent(pRef, _ => ListBuffer())
      eventMap.computeIfPresent(pRef, (_: ProcessRef, events: EventList) => events += event)
    }

    def get(pRef: ProcessRef): Seq[A] = eventMap.getOrDefault(pRef, ListBuffer.empty)

    def allEvents: Seq[A] = eventMap.values().asScala.flatten.toSeq

    def print(): Unit = {
      println("===== Event store ====")
      eventMap.forEach { (ref: ProcessRef, events: EventList) =>
        println(s"$ref  -> $events")
      }
    }

    def size: Int = sizeRef.get()

    def awaitSize(expectedSize: Int, delay: FiniteDuration = 100.millis)(implicit timer: Timer[IO]): IO[Unit] = {
      if (size == expectedSize) IO.unit
      else IO.sleep(delay) >> awaitSize(expectedSize, delay)
    }
  }


  def createProcess(eventStore: EventStore[TestEvent],
                    time: FiniteDuration = TaskProcessingTime.instant.time): IOProcess = {
    Process[IO] { self => {
      case e: TestEvent => delay(time) ++ eval(eventStore.add(self, e))
    }
    }
  }

  def submitAll(scheduler: Scheduler[IO], tasks: Seq[IOTask]): IO[Unit] = {
    tasks.map(scheduler.submit).foldLeft(IO.unit)(_ >> _)
  }

  @tailrec
  def assertEventsOrder(events: Seq[TestEvent]): Unit = {
    events match {
      case x :: y :: xs =>
        require(x.seqNumber < y.seqNumber,
          s"incorrect order of events: ${x.seqNumber} shouldBe < ${y.seqNumber}")
        assertEventsOrder(y +: xs)
      case _ =>
    }
  }

  def verifyEvents(submittedTasks: Seq[IODeliver], eventStore: EventStore[TestEvent]): Unit = {
    val groupedEventsByProcess = groupEventsByProcess(submittedTasks)

    val actualEvents = eventStore.allEvents.map(_.seqNumber).toSet
    actualEvents.size shouldBe submittedTasks.size

    groupedEventsByProcess.foreach {
      case (pRef, expectedEvents) =>
        eventStore.get(pRef) shouldBe expectedEvents
    }
  }

  def groupEventsByProcess(tasks: Seq[IODeliver]): Map[ProcessRef, Seq[Event]] = {
    tasks.groupBy(t => t.envelope.receiver).mapValues(_.map(_.envelope.event))
  }

  def toTestEvent(e: Event): TestEvent = e.asInstanceOf[TestEvent]

  trait TaskProcessingTime {
    // returns time in milliseconds
    def time: FiniteDuration
  }

  object TaskProcessingTime {

    class Interval(value: FiniteDuration) extends TaskProcessingTime {
      override def time: FiniteDuration = value.toMillis.millis
    }

    class Range(from: FiniteDuration, to: FiniteDuration) extends TaskProcessingTime {
      private val rnd = new Random

      override def time: FiniteDuration = {
        val fromMillis = from.toMillis
        val toMillis = to.toMillis

        (fromMillis + rnd.nextInt((toMillis - fromMillis).toInt + 1)).millis
      }
    }

    val instant: TaskProcessingTime = new TaskProcessingTime {
      private val now = 0.millis

      override def time: FiniteDuration = now
    }

    def range(from: FiniteDuration, to: FiniteDuration): TaskProcessingTime = new Range(from, to)

    def interval(time: FiniteDuration): TaskProcessingTime = new Interval(time)

  }

  def createProcesses(n: Int, pta: TaskProcessingTime, ratio: Double, ptb: TaskProcessingTime, eventStore: EventStore[TestEvent]): Array[Process[IO]] = {
    val proceses = new Array[Process[IO]](n)
    val aLne = (n * ratio).toInt
    val bLen = n - aLne
    (0 until aLne).foreach { i =>
      proceses(i) = createProcess(eventStore, pta.time)
    }
    (aLne until n).foreach { i =>
      proceses(i) = createProcess(eventStore, ptb.time)
    }
    proceses
  }

  trait WorkloadGen {
    def createTasks(n: Int, processes: Array[Process[IO]]): Seq[IODeliver]
  }

  object WorkloadGen {

    object Random extends WorkloadGen {
      override def createTasks(n: Int, processes: Array[Process[IO]]): Seq[IODeliver] = {
        val rnd = scala.util.Random
        (1 to n).map(i => Deliver[IO](Envelope(ProcessRef.SystemRef, TestEvent(i),
          processes(rnd.nextInt(processes.length)).ref)))
      }
    }

    object Batch extends WorkloadGen {
      override def createTasks(n: Int, processes: Array[Process[IO]]): Seq[IODeliver] = {
        def create(i: Int, offset: Int, n: Int, tasks: Seq[IODeliver]): Seq[IODeliver] = {
          if (i < processes.length) {
            create(i + 1, offset + n, n, tasks ++ (1 to n).map(j =>
              Deliver[IO](Envelope(ProcessRef.SystemRef, TestEvent(offset + j), processes(i).ref))))
          } else tasks
        }

        create(0, 0, n, Seq.empty)
      }
    }

  }



}