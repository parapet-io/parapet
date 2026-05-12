package io.parapet.tests.intg.scheduler

import io.parapet.core.Scheduler.*
import io.parapet.core.{ExecutionTrace, Process}
import io.parapet.tests.intg.scheduler.*
import io.parapet.tests.intg.scheduler.TaskProcessingTime.*
import io.parapet.tests.intg.scheduler.TaskSubmitter.assertEventsOrder
import io.parapet.tests.intg.scheduler.{EventDiff, SchedulerCorrectnessSpec, SchedulerStressSpec}
import io.parapet.testutils.EventStore
import io.parapet.{Envelope, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*
import scala.util.Using

import scala.concurrent.duration.*

/** Correctness suite for the scheduler.
  */
abstract class SchedulerCorrectnessSpec[F[_]] extends AnyFunSuite with SchedulerTestRunner[F] {

  import SchedulerCorrectnessSpec.*

  test("scheduler correctness under normal conditions") {
    run(correctnessMatrix)
  }

  test("RoundRobin task gen") {
    val numberOfEvents    = 5
    val numberOfProcesses = 5
    val processes         = (0 until numberOfProcesses).map(_ => TestProcesses.dummy[F]).toArray
    val actualTasks       = WorkDistributionStrategy.RoundRobin.createTasks[F](numberOfEvents, processes, 1)

    actualTasks.size shouldBe numberOfEvents

    val actualEvents =
      groupEventsByProcess(actualTasks).values.flatten.map(e => TestEvent.cast(e).seqNumber).toList.sorted
    actualEvents shouldBe (1 to numberOfEvents)
  }

  test("Random task gen is repeatable for a fixed seed") {
    val numberOfEvents    = 20
    val numberOfProcesses = 5
    val processes         = (0 until numberOfProcesses).map(_ => TestProcesses.dummy[F]).toArray
    val strategy          = WorkDistributionStrategy.Random.seeded(42L)

    val left  = strategy.createTasks[F](numberOfEvents, processes, 1)
    val right = strategy.createTasks[F](numberOfEvents, processes, 1)

    left.map(_.envelope.receiver) shouldBe right.map(_.envelope.receiver)
    left.map(t => TestEvent.cast(t.envelope.event)) shouldBe right.map(t => TestEvent.cast(t.envelope.event))
  }

  test("Batch task gen") {
    val batchSize         = 5
    val numberOfProcesses = 5
    val totalTasks        = batchSize * numberOfProcesses
    val processes         = (0 until numberOfProcesses).map(_ => TestProcesses.dummy[F]).toArray
    val actualTasks       = WorkDistributionStrategy.Batch.createTasks[F](batchSize, processes, 1)

    actualTasks.size shouldBe totalTasks

    val groupedByEvents = groupEventsByProcess(actualTasks)

    (0 until numberOfProcesses).foreach { i =>
      val expectedRange = (i * batchSize + 1) to ((i + 1) * batchSize)
      groupedByEvents(processes(i).ref).map(e => TestEvent.cast(e).seqNumber) shouldBe expectedRange
    }
  }

  test("Random task gen - multi submitter round-robins ids") {
    val numberOfEvents     = 12
    val numberOfProcesses  = 3
    val numberOfSubmitters = 4
    val processes          = (0 until numberOfProcesses).map(_ => TestProcesses.dummy[F]).toArray
    val tasks              =
      WorkDistributionStrategy.Random.seeded(99L).createTasks[F](numberOfEvents, processes, numberOfSubmitters)

    tasks.size shouldBe numberOfEvents
    val events = tasks.map(t => TestEvent.cast(t.envelope.event))
    events.map(_.seqNumber).toSet shouldBe (1 to numberOfEvents).toSet
    events.groupBy(_.submitterId).view.mapValues(_.size).toMap shouldBe
      (0 until numberOfSubmitters).map(i => i -> numberOfEvents / numberOfSubmitters).toMap
  }

  test("create processes - size matches n for all fastFraction values") {
    val fractions = Seq(0.0, 0.25, 0.5, 0.75, 1.0)
    fractions.foreach { f =>
      val n          = 5
      val eventStore = new EventStore[F, TestEvent]
      val workload   = WorkloadProfile.TwoGroup(
        fraction = f,
        first = TaskProcessingTime.instant,
        second = TaskProcessingTime.instant
      )
      val processes = TestProcesses.createAll(n, workload, eventStore)
      processes.length shouldBe n
    }
  }

  test("events order assertion") {
    assertEventsOrder(Seq(TestEvent(0, 1), TestEvent(0, 2), TestEvent(0, 3)))
    assertThrows[IllegalArgumentException] {
      assertEventsOrder(Seq(TestEvent(0, 2), TestEvent(0, 1), TestEvent(0, 3)))
    }
    assertThrows[IllegalArgumentException] {
      assertEventsOrder(Seq(TestEvent(0, 1), TestEvent(0, 3), TestEvent(0, 2)))
    }
  }

  test("verifyEvents fails on loss") {
    val process = TestProcesses.dummy[F]
    val tasks   = Seq(
      Deliver[F](Envelope(ProcessRef.SystemRef, TestEvent(0, 1), process.ref), ExecutionTrace.Dummy),
      Deliver[F](Envelope(ProcessRef.SystemRef, TestEvent(0, 2), process.ref), ExecutionTrace.Dummy)
    )
    val store = new EventStore[F, TestEvent]
    store.add(process.ref, TestEvent(0, 1))
    assertThrows[org.scalatest.exceptions.TestFailedException] {
      SchedulerCorrectnessSpec.verifyEvents(StabilitySpec.default, tasks, store)
    }
  }

  test("verifyEvents fails on out-of-order per submitter") {
    val process = TestProcesses.dummy[F]
    val tasks   = Seq(
      Deliver[F](Envelope(ProcessRef.SystemRef, TestEvent(0, 1), process.ref), ExecutionTrace.Dummy),
      Deliver[F](Envelope(ProcessRef.SystemRef, TestEvent(0, 2), process.ref), ExecutionTrace.Dummy)
    )
    val store = new EventStore[F, TestEvent]
    store.add(process.ref, TestEvent(0, 2))
    store.add(process.ref, TestEvent(0, 1))
    assertThrows[org.scalatest.exceptions.TestFailedException] {
      SchedulerCorrectnessSpec.verifyEvents(StabilitySpec.default, tasks, store)
    }
  }

  test("writeFailureReport produces a readable report on missing + reordered events") {
    val receiver = ProcessRef("diagnostic-target")
    val _        = Process.builder[F](_ => { case _: TestEvent => dsl.unit }).ref(receiver).build
    val tasks    = Seq(
      Deliver[F](Envelope(ProcessRef.SystemRef, TestEvent(0, 1), receiver), ExecutionTrace.Dummy),
      Deliver[F](Envelope(ProcessRef.SystemRef, TestEvent(0, 2), receiver), ExecutionTrace.Dummy),
      Deliver[F](Envelope(ProcessRef.SystemRef, TestEvent(1, 3), receiver), ExecutionTrace.Dummy),
      Deliver[F](Envelope(ProcessRef.SystemRef, TestEvent(1, 4), receiver), ExecutionTrace.Dummy)
    )
    val store = new EventStore[F, TestEvent]
    store.add(receiver, TestEvent(0, 2))
    store.add(receiver, TestEvent(0, 1))
    store.add(receiver, TestEvent(1, 3))
    store.add(receiver, TestEvent(1, 3))

    val spec = StabilitySpec.default.copy(name = "self-check-report")
    val path = SchedulerTestRunner.writeFailureReport[F](spec, 1, tasks, store, None)

    val file = new java.io.File(path)
    file.exists() shouldBe true
    val contents =
      Using.resource(scala.io.Source.fromFile(file)) { source =>
        source.getLines().mkString("\n")
      }
    contents should include("parapet scheduler failure report")
    contents should include("spec: self-check-report")
    contents should include("missing=1")
    contents should include("duplicates=1")
    contents should include("orderingBreaks=2")
    contents should include("TestEvent(1,4)")
    contents should include("TestEvent(1,3) (x2)")
    contents should include("submitter=0")
    contents should include("submitter=1")
    file.delete()
    ()
  }

  /** The full correctness matrix. Defined in the subclass body so individual scenarios can be disabled (via `.disable`)
    * or filtered without touching the test block.
    *
    * Each [[StabilitySpec]] is a declarative description; [[SchedulerTestRunner.run]] interprets it.
    */
  def correctnessMatrix: Seq[StabilitySpec] = SchedulerCorrectnessSpec.correctnessMatrix

  private def groupEventsByProcess(tasks: Seq[Deliver[F]]): Map[ProcessRef, Seq[io.parapet.Event]] =
    tasks.groupBy(t => t.envelope.receiver).view.mapValues(_.map(_.envelope.event)).toMap
}

object SchedulerCorrectnessSpec {

  /** Baseline correctness matrix. Organised into sections by the invariant they stress.
    */
  private val correctnessMatrix: Seq[StabilitySpec] = {
    val fastTime = instant
    val slowTime = range(50.millis, 100.millis)

    val balancedSplit   = WorkloadProfile.TwoGroup(fraction = 0.5, first = fastTime, second = slowTime)
    val asymmetricSplit = WorkloadProfile.TwoGroup(fraction = 0.75, first = fastTime, second = slowTime)

    val workerVariants                          = Seq(1, 5, 10)
    val workersDistribution: Seq[StabilitySpec] = workerVariants.flatMap { w =>
      Seq(
        StabilitySpec(
          name = s"workers-$w-round-robin",
          samples = 5,
          config = SchedulerConfig(numberOfWorkers = w),
          wds = WorkDistributionStrategy.RoundRobin,
          numberOfEvents = 50,
          numberOfProcesses = 5,
          workload = balancedSplit
        ),
        StabilitySpec(
          name = s"workers-$w-random",
          samples = 5,
          config = SchedulerConfig(numberOfWorkers = w),
          wds = WorkDistributionStrategy.Random(),
          numberOfEvents = 50,
          numberOfProcesses = 5,
          workload = balancedSplit
        ),
        StabilitySpec(
          name = s"workers-$w-batch",
          samples = 5,
          config = SchedulerConfig(numberOfWorkers = w),
          wds = WorkDistributionStrategy.Batch,
          numberOfEvents = 10,
          numberOfProcesses = 5,
          workload = balancedSplit
        )
      )
    }

    val asymmetricWorkload: Seq[StabilitySpec] = Seq(
      StabilitySpec(
        name = "asymmetric-split-round-robin",
        samples = 5,
        config = SchedulerConfig(numberOfWorkers = 5),
        wds = WorkDistributionStrategy.RoundRobin,
        numberOfEvents = 50,
        numberOfProcesses = 5,
        workload = asymmetricSplit
      ),
      StabilitySpec(
        name = "asymmetric-split-random",
        samples = 5,
        config = SchedulerConfig(numberOfWorkers = 5),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 50,
        numberOfProcesses = 5,
        workload = asymmetricSplit
      ),
      StabilitySpec(
        name = "asymmetric-split-batch",
        samples = 5,
        config = SchedulerConfig(numberOfWorkers = 5),
        wds = WorkDistributionStrategy.Batch,
        numberOfEvents = 10,
        numberOfProcesses = 5,
        workload = asymmetricSplit
      )
    )

    val concurrentSubmitters = Seq(
      StabilitySpec(
        name = "concurrent-submitters-round-robin",
        samples = 3,
        config = SchedulerConfig(numberOfWorkers = 8),
        wds = WorkDistributionStrategy.RoundRobin,
        numberOfEvents = 500,
        numberOfProcesses = 10,
        workload = WorkloadProfile.instant,
        numberOfSubmitters = 4
      ),
      StabilitySpec(
        name = "concurrent-submitters-random",
        samples = 3,
        config = SchedulerConfig(numberOfWorkers = 8),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 500,
        numberOfProcesses = 10,
        workload = WorkloadProfile.instant,
        numberOfSubmitters = 4
      ),
      StabilitySpec(
        name = "concurrent-submitters-batch",
        samples = 3,
        config = SchedulerConfig(numberOfWorkers = 4),
        wds = WorkDistributionStrategy.Batch,
        numberOfEvents = 50,
        numberOfProcesses = 10,
        workload = WorkloadProfile.TwoGroup(
          fraction = 0.5,
          first = instant,
          second = range(5.millis, 20.millis)
        ),
        numberOfSubmitters = 3
      )
    )

    val blockingMix = Seq(
      StabilitySpec(
        name = "blocking-mix",
        samples = 3,
        config = SchedulerConfig(numberOfWorkers = 5),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 200,
        numberOfProcesses = 10,
        workload = WorkloadProfile.instant,
        blockingRatio = 0.5
      ),
      StabilitySpec(
        name = "blocking-plus-concurrent",
        samples = 3,
        config = SchedulerConfig(numberOfWorkers = 8),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 300,
        numberOfProcesses = 10,
        workload = WorkloadProfile.instant,
        numberOfSubmitters = 4,
        blockingRatio = 0.3
      )
    )

    val scale = Seq(
      StabilitySpec(
        name = "scale-fan-out",
        samples = 2,
        config = SchedulerConfig(numberOfWorkers = 8),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 2000,
        numberOfProcesses = 500,
        workload = WorkloadProfile.instant
      ),
      StabilitySpec(
        name = "scale-concurrent",
        samples = 1,
        config = SchedulerConfig(numberOfWorkers = 8),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 2000,
        numberOfProcesses = 200,
        workload = WorkloadProfile.instant,
        numberOfSubmitters = 6
      )
    )

    /** Mailbox slicing variants. Force the slice low enough that a typical scenario crosses the slice boundary multiple
      * times per process, then assert the per-(submitter, receiver) FIFO invariant still holds across release/notify
      * boundaries. `mailboxSlice = 1` is the worst case (yield after every event).
      */
    val mailboxSlicing = Seq(
      StabilitySpec(
        name = "slicing-1-event",
        samples = 3,
        config = SchedulerConfig(numberOfWorkers = 4, mailboxSlice = 1),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 200,
        numberOfProcesses = 5,
        workload = WorkloadProfile.instant,
        numberOfSubmitters = 3
      ),
      StabilitySpec(
        name = "slicing-8-events",
        samples = 3,
        config = SchedulerConfig(numberOfWorkers = 4, mailboxSlice = 8),
        wds = WorkDistributionStrategy.Batch,
        numberOfEvents = 50,
        numberOfProcesses = 8,
        workload = WorkloadProfile.TwoGroup(
          fraction = 0.5,
          first = instant,
          second = range(2.millis, 8.millis)
        ),
        numberOfSubmitters = 2
      ),
      StabilitySpec(
        name = "slicing-with-multiqueue",
        samples = 2,
        config = SchedulerConfig(numberOfWorkers = 8, numberOfSignalQueues = 4, mailboxSlice = 16),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 500,
        numberOfProcesses = 10,
        workload = WorkloadProfile.instant,
        numberOfSubmitters = 4
      ),
      StabilitySpec(
        name = "slicing-with-blocking",
        samples = 2,
        config = SchedulerConfig(numberOfWorkers = 5, mailboxSlice = 4),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 200,
        numberOfProcesses = 10,
        workload = WorkloadProfile.instant,
        blockingRatio = 0.4,
        numberOfSubmitters = 3
      )
    )

    /** Multi-queue + work-stealing variants. Reuses the most demanding scenarios (concurrent submitters, blocking mix,
      * scale) and runs them under `numberOfSignalQueues > 1`.
      */
    val multiQueue = Seq(
      StabilitySpec(
        name = "multiqueue-workers-8-q-4",
        samples = 3,
        config = SchedulerConfig(numberOfWorkers = 8, numberOfSignalQueues = 4),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 500,
        numberOfProcesses = 10,
        workload = WorkloadProfile.instant,
        numberOfSubmitters = 4
      ),
      StabilitySpec(
        name = "multiqueue-workers-8-q-8",
        samples = 3,
        config = SchedulerConfig(numberOfWorkers = 8, numberOfSignalQueues = 8),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 500,
        numberOfProcesses = 10,
        workload = WorkloadProfile.instant,
        numberOfSubmitters = 4
      ),
      StabilitySpec(
        name = "multiqueue-blocking-mix-q-8",
        samples = 2,
        config = SchedulerConfig(numberOfWorkers = 8, numberOfSignalQueues = 8),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 200,
        numberOfProcesses = 10,
        workload = WorkloadProfile.instant,
        blockingRatio = 0.4,
        numberOfSubmitters = 3
      ),
      StabilitySpec(
        name = "multiqueue-more-queues-than-workers",
        samples = 2,
        config = SchedulerConfig(numberOfWorkers = 4, numberOfSignalQueues = 8),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 300,
        numberOfProcesses = 10,
        workload = WorkloadProfile.instant,
        numberOfSubmitters = 4
      ),
      // samples=5 because this is the highest-contention scenario in the matrix (8 workers × 8 queues × 6 submitters ×
      // 200 processes × 2000 events) and the one that surfaced a missed-wakeup race in the scheduler notify path
      // before the always-notify fix in `SchedulerImpl.submit`. Running it five times per CI build acts as a regression
      // guard.
      StabilitySpec(
        name = "multiqueue-scale-q-8",
        samples = 5,
        config = SchedulerConfig(numberOfWorkers = 8, numberOfSignalQueues = 8),
        wds = WorkDistributionStrategy.Random(),
        numberOfEvents = 2000,
        numberOfProcesses = 200,
        workload = WorkloadProfile.instant,
        numberOfSubmitters = 6
      )
    )

    val edgeCases = Seq(
      StabilitySpec(
        name = "hot-mailbox",
        samples = 3,
        config = SchedulerConfig(numberOfWorkers = 4),
        wds = WorkDistributionStrategy.Random.seeded(11L),
        numberOfEvents = 1000,
        numberOfProcesses = 2,
        workload = WorkloadProfile.instant,
        numberOfSubmitters = 8
      ),
      StabilitySpec(
        name = "single-worker-worst-case",
        samples = 3,
        config = SchedulerConfig(numberOfWorkers = 1, mailboxSlice = 1),
        wds = WorkDistributionStrategy.Random.seeded(29L),
        numberOfEvents = 300,
        numberOfProcesses = 10,
        workload = WorkloadProfile.instant,
        numberOfSubmitters = 4,
        blockingRatio = 0.4
      ),
      StabilitySpec(
        name = "slow-tail-multiqueue",
        samples = 2,
        config = SchedulerConfig(numberOfWorkers = 8, numberOfSignalQueues = 8),
        wds = WorkDistributionStrategy.Random.seeded(41L),
        numberOfEvents = 1000,
        numberOfProcesses = 50,
        workload = WorkloadProfile.TwoGroup(
          fraction = 0.9,
          first = instant,
          second = range(50.millis, 100.millis)
        ),
        numberOfSubmitters = 6,
        blockingRatio = 0.2
      )
    )

    workersDistribution ++ asymmetricWorkload ++ concurrentSubmitters ++ blockingMix ++ scale ++ mailboxSlicing ++ multiQueue ++ edgeCases
  }

  /** Verifies the invariants listed at the top of this file. On violation produces a compact summary; the full
    * structured diff is written to a file by [[SchedulerTestRunner.writeFailureReport]].
    */
  def verifyEvents[F[_]](
      spec: StabilitySpec,
      submittedTasks: Seq[Deliver[F]],
      eventStore: EventStore[F, TestEvent]
  ): Unit = {
    val MaxExamplesPerCategory = 3
    val diff                   = EventDiff.compute(submittedTasks, eventStore)
    if (!diff.isClean) {
      val preview = (
        diff.missing.take(MaxExamplesPerCategory).map(e => s"missing=$e") ++
          diff.duplicates.take(MaxExamplesPerCategory).map { case (e, n) => s"dup=$e x$n" } ++
          diff.unexpected.take(MaxExamplesPerCategory).map(e => s"unexpected=$e") ++
          diff.orderingBreaks
            .take(MaxExamplesPerCategory)
            .map { b =>
              val exp = b.expected.fold("<none>")(_.toString)
              val act = b.actual.fold("<none>")(_.toString)
              s"reorder at ${b.receiver}[${b.submitterId}][${b.index}]: expected=$exp actual=$act"
            }
      ).mkString(" | ")
      org.scalatest.Assertions.fail(
        s"spec=${spec.name} invariant violated: ${diff.summary}. First issues: $preview"
      )
    }
  }

}
