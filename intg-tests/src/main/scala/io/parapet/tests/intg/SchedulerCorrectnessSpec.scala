package io.parapet.tests.intg

import java.util.concurrent.TimeUnit

import cats.effect.Concurrent
import cats.implicits._
import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.Event._
import io.parapet.core.Scheduler._
import io.parapet.core.{Context, Event, EventLog, Parapet, Process, ProcessRef, Scheduler}
import io.parapet.implicits._
import io.parapet.syntax.logger.MDCFields
import io.parapet.tests.intg.SchedulerCorrectnessSpec.TaskProcessingTime._
import io.parapet.tests.intg.SchedulerCorrectnessSpec._
import io.parapet.testutils.Tags.Correctness
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.tagobjects.Slow

import scala.annotation.tailrec
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Random


abstract class SchedulerCorrectnessSpec[F[_]] extends FunSuite with IntegrationSpec[F] {

  def interpreter(context: Context[F]): F[Interpreter[F]]

  test("scheduler correctness under normal conditions", Slow, Correctness) {
    val specs = Seq(
      // random work distribution
      StabilitySpec(
        name = "test-1",
        config = SchedulerConfig(
          queueSize = 1000,
          numberOfWorkers = 5,
          processQueueSize = 100),
        wds = WorkDistributionStrategy.Random,
        numberOfEvents = 50,
        numberOfProcesses = 5,
        pta = instant,
        ratio = 0.5,
        ptb = range(100.millis, 500.millis)),

      StabilitySpec(
        name = "test-2",
        config = SchedulerConfig(
          queueSize = 1000,
          numberOfWorkers = 10,
          processQueueSize = 100),
        wds = WorkDistributionStrategy.Random,
        numberOfEvents = 50,
        numberOfProcesses = 5,
        pta = instant,
        ratio = 0.75,
        ptb = range(100.millis, 500.millis)),

      // batch work distribution
      StabilitySpec(
        name = "test-3",
        config = SchedulerConfig(
          queueSize = 1000,
          numberOfWorkers = 10,
          processQueueSize = 100),
        wds = WorkDistributionStrategy.Batch,
        numberOfEvents = 10,
        numberOfProcesses = 5,
        pta = instant,
        ratio = 0.5,
        ptb = range(100.millis, 500.millis)),
      StabilitySpec(
        name = "test-3",
        config = SchedulerConfig(
          queueSize = 1000,
          numberOfWorkers = 10,
          processQueueSize = 100),
        wds = WorkDistributionStrategy.Batch,
        numberOfEvents = 10,
        numberOfProcesses = 5,
        pta = instant,
        ratio = 0.75,
        ptb = range(100.millis, 500.millis))
    )

    run(specs)
  }

  test("Random task gen") {
    val numberOfEvents = 5
    val numberOfProcesses = 5
    val processes = (0 until numberOfProcesses).map(_ => dummyProcess[F]).toArray
    val actualTasks = WorkDistributionStrategy.Random.createTasks[F](numberOfEvents, processes)

    actualTasks.size shouldBe numberOfEvents

    val actualEvents = groupEventsByProcess(actualTasks).values.flatten.map(toTestEvent(_).seqNumber).toList.sorted
    actualEvents shouldBe (1 to numberOfEvents)
  }

  test("Batch task gen") {
    val batchSize = 5
    val numberOfProcesses = 5
    val totalTasks = batchSize * numberOfProcesses
    val processes = (0 until numberOfProcesses).map(_ => dummyProcess[F]).toArray
    val actualTasks = WorkDistributionStrategy.Batch.createTasks[F](batchSize, processes)

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
    val eventStore = new EventStore[F, TestEvent]
    val processes = createProcesses(n, TaskProcessingTime.instant, 0.5, TaskProcessingTime.instant, eventStore)
    processes.length shouldBe n
  }

  test("create processes #2") {
    val n = 5
    val eventStore = new EventStore[F, TestEvent]
    val processes = createProcesses(n, TaskProcessingTime.instant, 0.25, TaskProcessingTime.instant, eventStore)
    processes.length shouldBe n
  }

  test("create processes #3") {
    val n = 5
    val eventStore = new EventStore[F, TestEvent]
    val processes = createProcesses(n, TaskProcessingTime.instant, 0.75, TaskProcessingTime.instant, eventStore)
    processes.length shouldBe n
  }

  test("create processes #4") {
    val n = 5
    val eventStore = new EventStore[F, TestEvent]
    val processes = createProcesses(n, TaskProcessingTime.instant, 0, TaskProcessingTime.instant, eventStore)
    processes.length shouldBe n
  }

  test("create processes #5") {
    val n = 5
    val eventStore = new EventStore[F, TestEvent]
    val processes = createProcesses(n, TaskProcessingTime.instant, 1, TaskProcessingTime.instant, eventStore)
    processes.length shouldBe n
  }

  def run(specs: Seq[StabilitySpec]): Unit = {
    specs.filter(_.enabled).foreach(run)
  }

  def run(spec: StabilitySpec): Unit = {

    (1 to spec.samples).foreach { i =>
      val mdcFields: MDCFields = Map(
        "name" -> spec.name,
        "sample" -> i,
        "scheduler_task_queue_size" -> spec.config.queueSize,
        "process_event_queue_size" -> spec.config.processQueueSize,
        "number_of_workers" -> spec.config.numberOfWorkers,
        "number_of_processes" -> spec.numberOfProcesses,
        "number_of_events" -> spec.numberOfEvents,
        "ratio" -> spec.ratio,
        "first_group_processing_time_mode" -> spec.pta.name,
        "second_group_processing_time_mode" -> spec.ptb.name,
        "work_distribution_strategy" -> spec.wds.name)

      val eventStore = new EventStore[F, TestEvent]
      val processes = createProcesses(
        spec.numberOfProcesses,
        spec.pta, spec.ratio, spec.ptb, eventStore)
      val tasks = spec.wds.createTasks(spec.numberOfEvents, processes)

      require(tasks.size >= spec.numberOfEvents, "number of tasks must be gte number of events")

      val program = for {
        context <- Context[F](Parapet.ParConfig(spec.config), EventLog.stub)(ct)
        _ <- context.init
        _ <- context.registerAll(ProcessRef.SystemRef, processes.toList)
        it <- interpreter(context)
        scheduler <- Scheduler[F](spec.config, context, it)

        _ <- submitAll(scheduler, tasks)
        _ <- eventStore.await(tasks.size, scheduler.run)
      } yield ()

      logger.mdc(mdcFields) { _ => {
        logger.info("test is starting")
      }
      }
      val start = System.nanoTime()
      unsafeRun(program)
      val end = System.nanoTime()
      val elapsedTime = TimeUnit.NANOSECONDS.toMillis(end - start)
      logger.mdc(mdcFields) { _ => {
        logger.info(s"test completed in $elapsedTime ms")
      }
      }


      verifyEvents(tasks, eventStore)
    }

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

}

object SchedulerCorrectnessSpec {


  type TaskId = Int // todo add id to the Task

  def dummyProcess[F[_]]: Process[F] = new Process[F] {

    import dsl._

    override val handle: Receive = {
      case _ => unit
    }
  }

  case class TestEvent(seqNumber: Int) extends Event


  /**
    * Creates a  process with processing `time`
    *
    * @param eventStore in memory store for events [[EventStore]]
    * @param time       event processing time
    * @return [[Process]]
    */
  def createProcess[F[_]](eventStore: EventStore[F, TestEvent],
                          time: FiniteDuration = TaskProcessingTime.instant.time): Process[F] = {
    new Process[F] {

      import dsl._

      val handle: Receive = {
        case e: TestEvent => delay(time) ++ eval(eventStore.add(ref, e))
      }
    }
  }

  def submitAll[F[_] : Concurrent](scheduler: Scheduler[F], tasks: Seq[Task[F]]): F[Unit] = {
    val ct = implicitly[Concurrent[F]]
    tasks.map(scheduler.submit).foldLeft(ct.unit)(_ >> _)
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

  def verifyEvents[F[_]](submittedTasks: Seq[Deliver[F]], eventStore: EventStore[F, TestEvent]): Unit = {
    val groupedEventsByProcess = groupEventsByProcess(submittedTasks)

    val actualEvents = eventStore.allEvents.map(_.seqNumber).toSet
    actualEvents.size shouldBe submittedTasks.size

    groupedEventsByProcess.foreach {
      case (pRef, expectedEvents) =>
        eventStore.get(pRef) shouldBe expectedEvents
    }
  }


  def groupEventsByProcess[F[_]](tasks: Seq[Deliver[F]]): Map[ProcessRef, Seq[Event]] = {
    tasks.groupBy(t => t.envelope.receiver).mapValues(_.map(_.envelope.event))
  }

  def toTestEvent(e: Event): TestEvent = e.asInstanceOf[TestEvent]

  trait TaskProcessingTime {
    val name: String

    // returns time in milliseconds
    def time: FiniteDuration
  }

  object TaskProcessingTime {

    class Interval(value: FiniteDuration) extends TaskProcessingTime {

      override def time: FiniteDuration = value.toMillis.millis

      override val name: String = "interval"
    }

    class Range(from: FiniteDuration, to: FiniteDuration) extends TaskProcessingTime {
      private val rnd = new Random

      override def time: FiniteDuration = {
        val fromMillis = from.toMillis
        val toMillis = to.toMillis

        (fromMillis + rnd.nextInt((toMillis - fromMillis).toInt + 1)).millis
      }

      override val name: String = s"range[$from, $to]"
    }

    val instant: TaskProcessingTime = new TaskProcessingTime {
      private val now = 0.millis

      override def time: FiniteDuration = now

      override val name: String = "instant"
    }

    def range(from: FiniteDuration, to: FiniteDuration): TaskProcessingTime = new Range(from, to)

    def interval(time: FiniteDuration): TaskProcessingTime = new Interval(time)

  }

  /**
    * Creates processes with processing time based on the given `ratio`
    *
    * @param n          number of processes
    * @param pta        event processing time for (ratio * n) processes
    * @param ratio      percent of processes with processing time `pta` vs. `ptb`
    * @param ptb        event processing time for (n - ratio * n) processes
    * @param eventStore in memory store for events [[EventStore]]
    * @return array of [[Process]]
    */
  def createProcesses[F[_]](n: Int, pta: TaskProcessingTime,
                            ratio: Double, ptb: TaskProcessingTime,
                            eventStore: EventStore[F, TestEvent]): Array[Process[F]] = {
    val processes = new Array[Process[F]](n)
    val aN = (n * ratio).toInt
    (0 until aN).foreach { i =>
      processes(i) = createProcess(eventStore, pta.time)
    }
    (aN until n).foreach { i =>
      processes(i) = createProcess(eventStore, ptb.time)
    }
    processes
  }

  trait WorkDistributionStrategy {

    val name: String

    def createTasks[F[_]](n: Int, processes: Array[Process[F]]): Seq[Deliver[F]]
  }

  object WorkDistributionStrategy {

    object Random extends WorkDistributionStrategy {
      override val name: String = "random"

      override def createTasks[F[_]](n: Int, processes: Array[Process[F]]): Seq[Deliver[F]] = {
        val rnd = scala.util.Random
        (1 to n).map(i => Deliver[F](Envelope(ProcessRef.SystemRef, TestEvent(i),
          processes(rnd.nextInt(processes.length)).ref)))
      }
    }

    object Batch extends WorkDistributionStrategy {

      override val name: String = "batch"

      override def createTasks[F[_]](n: Int, processes: Array[Process[F]]): Seq[Deliver[F]] = {
        def create(i: Int, offset: Int, n: Int, tasks: Seq[Deliver[F]]): Seq[Deliver[F]] = {
          if (i < processes.length) {
            create(i + 1, offset + n, n, tasks ++ (1 to n).map(j =>
              Deliver[F](Envelope(ProcessRef.SystemRef, TestEvent(offset + j), processes(i).ref))))
          } else tasks
        }

        create(0, 0, n, Seq.empty)
      }

    }

  }

  case class StabilitySpec(
                            name: String,
                            samples: Int = 1,
                            config: SchedulerConfig,
                            wds: WorkDistributionStrategy,
                            numberOfEvents: Int,
                            numberOfProcesses: Int,
                            pta: TaskProcessingTime,
                            ratio: Double,
                            ptb: TaskProcessingTime,
                            enabled: Boolean = true
                          ) {
    def disable: StabilitySpec = this.copy(enabled = false)
  }


}