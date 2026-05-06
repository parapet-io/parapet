package io.parapet.tests.intg.scheduler

import io.parapet.ProcessRef
import io.parapet.core.Scheduler.*
import io.parapet.core.{Context, EventTransformers, Parapet, Scheduler}
import io.parapet.syntax.logger.*
import io.parapet.tests.intg.scheduler.*
import io.parapet.tests.intg.scheduler.TaskSubmitter.submitAll
import io.parapet.tests.intg.*
import io.parapet.testutils.EventStore

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

/** Shared driver that runs a [[StabilitySpec]].
  *
  * When a sample fails the driver writes a standalone failure report to
  * `target/scheduler-failures/<spec>-<sample>-<timestamp>.log` containing the spec, the diff between submitted and
  * delivered events, and - if tracing is on - the trace-id from the scheduler debug log.
  */
trait SchedulerTestRunner[F[_]] extends IntegrationSpec[F] {

  def run(specs: Seq[StabilitySpec]): Unit =
    specs.filter(_.enabled).foreach(run)

  /** Executes `spec` for `spec.samples` iterations.
    */
  def run(spec: StabilitySpec): Unit =
    (1 to spec.samples).foreach { i =>
      val mdcFields: MDCFields = Map(
        "name"                       -> spec.name,
        "sample"                     -> i,
        "number_of_workers"          -> spec.config.numberOfWorkers,
        "number_of_processes"        -> spec.numberOfProcesses,
        "number_of_events"           -> spec.numberOfEvents,
        "number_of_submitters"       -> spec.numberOfSubmitters,
        "blocking_ratio"             -> spec.blockingRatio,
        "workload"                   -> spec.workload.name,
        "work_distribution_strategy" -> spec.wds.name,
        "dev_mode"                   -> spec.devMode,
        "tracing_enabled"            -> spec.tracingEnabled,
        "event_log_enabled"          -> spec.eventLogEnabled
      )

      val eventStore = new EventStore[F, TestEvent]
      val processes  =
        TestProcesses.createAll(spec.numberOfProcesses, spec.workload, eventStore, spec.blockingRatio)
      val tasks = spec.wds.createTasks(spec.numberOfEvents, processes, spec.numberOfSubmitters)

      require(tasks.size >= spec.numberOfEvents, "number of tasks must be gte number of events")

      val parConfig = Parapet.ParConfig(
        processBufferSize = -1,
        schedulerConfig = spec.config,
        devMode = spec.devMode,
        tracingEnabled = spec.tracingEnabled,
        eventLogEnabled = spec.eventLogEnabled
      )

      val program = for {
        context <- Context[F](
          parConfig,
          io.parapet.core.EventStore.stub,
          EventTransformers.empty
        )
        scheduler <- Scheduler[F](spec.config, context, interpreter(context))
        fiber     <- ct.start(scheduler.start)
        _         <- context.start(scheduler)
        _         <- context.registerAll(ProcessRef.SystemRef, processes.toList)
        _         <- submitAll(scheduler, tasks, spec.numberOfSubmitters)
        _         <- eventStore.await0(tasks.size, fiber)
      } yield ()

      logger.mdc(mdcFields)(_ => logger.info("test is starting"))
      val start = System.nanoTime()
      try unsafeRun(program)
      catch {
        case t: Throwable =>
          val report = SchedulerTestRunner.writeFailureReport(spec, i, tasks, eventStore, Some(t))
          logger.mdc(mdcFields)(_ => logger.error(s"test failed: ${t.getMessage}. report: $report"))
          throw new AssertionError(
            s"spec=${spec.name} sample=$i failed during run: ${t.getMessage}. report: $report",
            t
          )
      }
      val end         = System.nanoTime()
      val elapsedTime = TimeUnit.NANOSECONDS.toMillis(end - start)
      logger.mdc(mdcFields)(_ => logger.info(s"test completed in $elapsedTime ms"))

      try SchedulerCorrectnessSpec.verifyEvents(spec, tasks, eventStore)
      catch {
        case t: Throwable =>
          val report = SchedulerTestRunner.writeFailureReport(spec, i, tasks, eventStore, Some(t))
          logger.mdc(mdcFields)(_ => logger.error(s"verification failed: ${t.getMessage}. report: $report"))
          throw new AssertionError(
            s"spec=${spec.name} sample=$i verification failed: ${t.getMessage}. report: $report",
            t
          )
      }
    }
}

object SchedulerTestRunner {

  private val Timestamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS").withZone(java.time.ZoneOffset.UTC)

  private val MaxRowsPerSection     = 200
  private val MaxOrderingBreaksRows = 50

  /** Writes a self-contained diagnostic report for a failed sample to
    * `<cwd>/target/scheduler-failures/<spec>-<sample>-<timestamp>.log`. Returns the absolute path of the file.
    */
  def writeFailureReport[F[_]](
      spec: StabilitySpec,
      sample: Int,
      tasks: Seq[Deliver[F]],
      eventStore: EventStore[F, TestEvent],
      cause: Option[Throwable]
  ): String = {
    val dir = Paths.get("target/scheduler-failures")
    Files.createDirectories(dir)
    val file = dir.resolve(s"${spec.name}-s$sample-${Timestamp.format(Instant.now())}.log")
    val sb   = new StringBuilder

    sb.append("=== parapet scheduler failure report ===\n")
    sb.append(s"timestamp: ${Instant.now()}\n")
    sb.append(s"spec: ${spec.name}\n")
    sb.append(s"sample: $sample\n")
    sb.append(s"workers: ${spec.config.numberOfWorkers}\n")
    sb.append(s"processes: ${spec.numberOfProcesses}\n")
    sb.append(s"events: ${spec.numberOfEvents}\n")
    sb.append(s"submitters: ${spec.numberOfSubmitters}\n")
    sb.append(s"workload: ${spec.workload.name}\n")
    sb.append(s"blockingRatio: ${spec.blockingRatio}\n")
    sb.append(s"wds: ${spec.wds.name}\n")
    sb.append(
      s"devMode: ${spec.devMode}  tracingEnabled: ${spec.tracingEnabled}  eventLogEnabled: ${spec.eventLogEnabled}\n"
    )
    cause.foreach { t =>
      sb.append(s"\n=== error ===\n")
      sb.append(s"${t.getClass.getName}: ${t.getMessage}\n")
      val sw = new java.io.StringWriter
      t.printStackTrace(new java.io.PrintWriter(sw))
      sb.append(sw.toString).append('\n')
    }

    val diff = EventDiff.compute(tasks, eventStore)
    sb.append(s"\n=== event diff ===\n")
    sb.append(diff.summary).append('\n')

    if (!diff.isClean) {
      def appendTruncated[A](section: String, rows: Seq[A], cap: Int)(render: A => String): Unit = {
        sb.append(s"\n=== $section ===\n")
        rows.take(cap).foreach(r => sb.append(s"  ${render(r)}\n"))
        if (rows.size > cap) sb.append(s"  ... ${rows.size - cap} more\n")
      }

      appendTruncated("missing events (submitted but never delivered)", diff.missing, MaxRowsPerSection)(_.toString)
      appendTruncated("duplicated events (delivered more than once)", diff.duplicates, MaxRowsPerSection) {
        case (e, n) => s"$e (x$n)"
      }
      appendTruncated("unexpected events (delivered but never submitted)", diff.unexpected, MaxRowsPerSection)(
        _.toString
      )

      sb.append(s"\n=== ordering breaks (first mismatch per (receiver, submitter)) ===\n")
      diff.orderingBreaks.take(MaxOrderingBreaksRows).foreach { break =>
        sb.append(s"  receiver=${break.receiver} submitter=${break.submitterId}\n")
        sb.append(s"    expected[${break.index}]=${break.expected.fold("<none>")(_.toString)}\n")
        sb.append(s"    actual  [${break.index}]=${break.actual.fold("<none>")(_.toString)}\n")
      }
      if (diff.orderingBreaks.size > MaxOrderingBreaksRows)
        sb.append(s"  ... ${diff.orderingBreaks.size - MaxOrderingBreaksRows} more\n")
    }

    sb.append(s"\n=== per-receiver delivery counts (expected vs actual) ===\n")
    diff.perReceiver.toSeq.sortBy(_._1.value).foreach { case (ref, pr) =>
      val ok = if (pr.expected == pr.actual) "OK " else "!! "
      sb.append(f"  $ok$ref%-32s expected=${pr.expected}%5d actual=${pr.actual}%5d\n")
    }

    if (spec.devMode)
      sb.append(
        "\nScheduler DEBUG log is at target/scheduler-test.log (append mode). Grep for " +
          "`traceId:` or the envelope ids above to reconstruct the causal chain.\n"
      )
    else
      sb.append(
        "\ndevMode was OFF - no scheduler DEBUG log was emitted. Re-run with " +
          "`.withDiagnostics` on the StabilitySpec to capture trace ids next time.\n"
      )

    Files.writeString(file, sb.toString, StandardCharsets.UTF_8)
    file.toAbsolutePath.toString
  }
}
