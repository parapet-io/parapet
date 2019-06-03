package io.parapet.core.intg

import java.util
import java.util.concurrent.{CopyOnWriteArrayList, Executors, TimeUnit}
import java.util.function.BiFunction
import java.util.{stream, List => JList}

import cats.effect.{ContextShift, IO}
import cats.syntax.flatMap._
import io.parapet.core.Parapet.{Event, _}
import io.parapet.core.Scheduler
import io.parapet.core.Scheduler._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow.{empty => emptyFlow, _}
import io.parapet.core.intg.SchedulerSpec._
import io.parapet.core.intg.SchedulerSpec.TaskGenerator._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Random

class SchedulerSpec extends FunSuite {

  test("task balancing") {
    val specs = Seq(
      Spec(
        name = "#1 Submit all tasks to the same process",
        samples = 1,
        numberOfTasks = 4,
        numberOfWorkers = 2,
        workerQueueSize = 1,
        numberOfProcesses = 1,
        taskCreator = TaskGenerator.AllToFirst
      ),
      Spec(
        name = "#2 Submit all tasks to the same process",
        samples = 1,
        numberOfTasks = 10,
        numberOfWorkers = 5,
        workerQueueSize = 1,
        numberOfProcesses = 1,
        taskCreator = TaskGenerator.AllToFirst
      ),
      Spec(
        name = "#1 Submit tasks in batches to each process",
        samples = 1,
        numberOfTasks = 5,
        numberOfWorkers = 5,
        workerQueueSize = 1,
        numberOfProcesses = 5,
        taskCreator = TaskGenerator.BatchToEach,
        taskProcessingTime = TaskProcessingTime.range(500.millis, 2.seconds)
      ),
      Spec(
        name = "#1 Submit all tasks randomly to all processes",
        samples = 1,
        numberOfTasks = 50,
        numberOfWorkers = 5,
        workerQueueSize = 10,
        numberOfProcesses = 5,
        taskCreator = TaskGenerator.AllToRandom,
        taskProcessingTime = TaskProcessingTime.range(500.millis, 2.second)
      )
    ).filter(_.enabled)

    assertUniqueSpecs(specs)

    specs.foreach(runSpec)

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

  test("BatchToEach task generator") {
    val batchSize = 5
    val numberOfProcesses = 5
    val totalTasks = batchSize * numberOfProcesses
    val processes = (0 until numberOfProcesses).map(_ => dummyProcess).toArray
    val actualTasks = BatchToEach.create(batchSize, processes)

    actualTasks.size shouldBe totalTasks

    val groupedByEvents = groupEventsByProcess(actualTasks)

    groupedByEvents(processes(0).ref).map(e => toTestEvent(e).seqNumber) shouldBe (1 to 5)
    groupedByEvents(processes(1).ref).map(e => toTestEvent(e).seqNumber) shouldBe (6 to 10)
    groupedByEvents(processes(2).ref).map(e => toTestEvent(e).seqNumber) shouldBe (11 to 15)
    groupedByEvents(processes(3).ref).map(e => toTestEvent(e).seqNumber) shouldBe (16 to 20)
    groupedByEvents(processes(4).ref).map(e => toTestEvent(e).seqNumber) shouldBe (21 to 25)
  }

  test("AllToFirst task generator") {
    val totalTasks = 5
    val processes = Array(dummyProcess)
    val actualTasks = AllToFirst.create(totalTasks, processes)

    actualTasks.size shouldBe totalTasks
    val groupedByEvents = groupEventsByProcess(actualTasks)

    groupedByEvents(processes(0).ref).map(e => toTestEvent(e).seqNumber) shouldBe (1 to 5)

  }

  test("AllToRandom task generator") {
    val totalTasks = 10
    val numberOfProcesses = 5
    val processes = (0 until numberOfProcesses).map(_ => dummyProcess).toArray

    val actualTasks = AllToRandom.create(totalTasks, processes)

    actualTasks.size shouldBe totalTasks

    val allEventIds = groupEventsByProcess(actualTasks)
      .values.flatten.map(e => toTestEvent(e).seqNumber).toList.sorted

    allEventIds shouldBe (1 to totalTasks)
  }

  def runSpec(spec: Spec): Unit = {
    println(s"run spec '${spec.name}'")

    val config = spec.toConfig
    val executor = Executors.newFixedThreadPool(10)
    val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)
    implicit val ctx: ContextShift[IO] = IO.contextShift(ec)
    implicit val timer: cats.effect.Timer[IO] = IO.timer(ec)

    for (i <- 1 to spec.samples) {
      println(s"Spec '${spec.name}' ; run = $i")
      val eventStore = new EventStore[TestEvent]
      val processes = (0 until spec.numberOfProcesses)
        .map(_ => createSlowProcess(spec.taskProcessingTime.time, eventStore)).toArray

      val program = for {
        tasks <- IO.pure(spec.taskCreator.create(spec.numberOfTasks, processes))
        _ <- IO(println(s"total tasks = ${tasks.size}"))
        taskQueue <- Queue.bounded[IO, IOTask](config.queueCapacity)
        interpreter <- IO.pure(ioFlowInterpreter(taskQueue)(ctx, timer) or ioEffectInterpreter)
        scheduler <- IO.pure(new Scheduler[IO](taskQueue, config, processes, interpreter))
        fiber <- scheduler.run.start
        _ <- submitAllAndTerminate(scheduler, tasks)
        _ <- fiber.join
      } yield tasks

      val tasks = program.unsafeRunSync()

      val groupedEventsByProcess = groupEventsByProcess(tasks)

      val actualEvents = eventStore.allEvents.map(_.seqNumber).toSet
      actualEvents.size shouldBe tasks.size

      eventStore.print()

      groupedEventsByProcess.foreach {
        case (pRef, expectedEvents) =>
          eventStore.get(pRef) shouldBe expectedEvents
      }

    }

    executor.shutdownNow()
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

  case class Spec(
                   name: String,
                   samples: Int,
                   numberOfTasks: Int,
                   numberOfWorkers: Int,
                   workerQueueSize: Int,
                   numberOfProcesses: Int,
                   taskProcessingTime: TaskProcessingTime = TaskProcessingTime.interval(2.seconds),
                   taskCreator: TaskGenerator,
                   taskSubmissionTimeout: FiniteDuration = 1.second,
                   enabled: Boolean = true) {
    def toConfig: SchedulerConfig =
      SchedulerConfig(numberOfWorkers = numberOfWorkers,
        queueCapacity = 100,
        workerQueueCapacity = workerQueueSize,
        taskSubmissionTimeout = taskSubmissionTimeout,
        maxRedeliveryRetries = 0,
        redeliveryInitialDelay = 0.seconds)

    def disable: Spec = this.copy(enabled = false)
  }

  case class TestEvent(seqNumber: Int) extends Event

  class EventStore[A <: Event] {
    type EventList = ListBuffer[A]
    private val eventMap: java.util.Map[ProcessRef, EventList] =
      new java.util.concurrent.ConcurrentHashMap[ProcessRef, EventList]()

    def add(pRef: ProcessRef, event: A): Unit = {
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
  }

  trait TaskGenerator {
    def create(n: Int, processes: Array[IOProcess]): Seq[IODeliver]
  }

  object TaskGenerator {

    object AllToFirst extends TaskGenerator {
      def create(n: Int, processes: Array[IOProcess]): Seq[IODeliver] = {
        require(processes.nonEmpty)
        createTasks(1, processes.head.ref, n)
      }
    }

    object BatchToEach extends TaskGenerator {
      def create(batchSize: Int, processes: Array[IOProcess]): Seq[IODeliver] = {
        @tailrec
        def create1(from: Int, processes1: List[IOProcess], tasks: Seq[IODeliver]): Seq[IODeliver] = {
          processes1 match {
            case x :: xs => create1(from + batchSize, xs, tasks ++ createTasks(from, x.ref, from + batchSize - 1))
            case _ => tasks
          }
        }

        create1(1, processes.toList, Seq.empty)
      }
    }

    object AllToRandom extends TaskGenerator {
      override def create(n: Int, processes: Array[IOProcess]): Seq[IODeliver] = {
        val rnd = new Random
        (1 to n).flatMap(i => createTasks(i, processes(rnd.nextInt(processes.length)).ref, i))
      }
    }

    def createTasks(from: Int, pRef: ProcessRef, to: Int): Seq[IODeliver] = {
      (from to to).map(i => {
        Deliver[IO](Envelope(ProcessRef.SystemRef, () => TestEvent(i), pRef))
      })
    }

  }

  def createSlowProcess(time: FiniteDuration, eventStore: EventStore[TestEvent]): IOProcess = {
    Process[IO] { self => {
      case e: TestEvent => delay(time) ++ eval(eventStore.add(self, e))
    }
    }
  }

  def submitAllAndTerminate(scheduler: Scheduler[IO], tasks: Seq[IOTask]): IO[Unit] = {
    (tasks :+ Terminate[IO]()).map(scheduler.submit).foldLeft(IO.unit)(_ >> _)
  }

  def assertUniqueSpecs(specs: Seq[Spec]): Unit = {
    @tailrec
    def check(names: Seq[String]): Unit = {
      names match {
        case x :: xs =>
          if (xs.contains(x)) throw new RuntimeException(s"duplicated spec name = '$x'")
          else check(xs)
        case _ =>
      }
    }

    check(specs.map(_.name))
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

  def groupEventsByProcess(tasks: Seq[IODeliver]): Map[ProcessRef, Seq[Event]] = {
    tasks.groupBy(t => t.envelope.receiver).mapValues(_.map(_.envelope.event()))
  }

  def toTestEvent(e: Event): TestEvent = e.asInstanceOf[TestEvent]

  trait TaskProcessingTime {
    // returns time in milliseconds
    def time: FiniteDuration
  }

  object TaskProcessingTime {

    object Instant extends TaskProcessingTime {
      private val now = 0.millis

      override def time: FiniteDuration = now
    }

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

    def range(from: FiniteDuration, to: FiniteDuration): TaskProcessingTime = new Range(from, to)

    def interval(time: FiniteDuration): TaskProcessingTime = new Interval(time)

  }


}