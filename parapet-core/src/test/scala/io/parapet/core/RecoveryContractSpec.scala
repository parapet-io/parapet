package io.parapet.core

import io.parapet.core.Dsl.WithDsl
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.{Ok, SubmissionResult, Task}
import io.parapet.core.exceptions.RecoveryContractViolation
import io.parapet.effect.Monad.*
import io.parapet.journal.{EventCodecRegistry, JournalConfig, JournalStoreLocal}
import io.parapet.{Event, ProcessRef, Scope}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.file.Files
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import scala.jdk.CollectionConverters.*

class RecoveryContractSpec extends AnyFunSuite with WithDsl[TestUtils.TestIO]:
  import TestUtils.*
  import TestUtils.given
  import dsl.*

  test("concurrent registration installs one process and records one marker") {
    val fixture = newFixture()
    val parent  = process(ProcessRef.root[Event]("parent"))
    fixture.context.register(ProcessRef.SystemRef, parent).unsafeRun()

    val childRef = parent.ref.child[Event]("worker")
    val children = Vector(process(childRef), process(childRef))
    val ready    = new CountDownLatch(children.size)
    val start    = new CountDownLatch(1)
    val results  = new ConcurrentLinkedQueue[Either[Throwable, ProcessRef.Unknown]]()

    val threads = children.map { child =>
      new Thread(() => {
        ready.countDown()
        start.await()
        try results.add(Right(fixture.context.register(parent.ref, child).unsafeRun()))
        catch case error: Throwable => results.add(Left(error))
        ()
      })
    }

    threads.foreach(_.start())
    ready.await()
    start.countDown()
    threads.foreach(_.join())

    val outcomes = results.iterator().asScala.toVector
    outcomes.count(_.isRight) shouldBe 1
    outcomes.collect { case Left(error) => error }.head shouldBe a[IllegalStateException]

    fixture.context.stopJournal.unsafeRun()
    val entries = fixture.store.read(0L).unsafeRun()

    entries.count(_.receiver == parent.ref) shouldBe 1
    children.exists(_ eq fixture.context.getProcessState(childRef).get.process) shouldBe true
  }

  test("recovery-enabled processes fail before executing Suspend unless they are a boundary") {
    val fixture  = newFixture()
    var executed = false

    val regular = process(ProcessRef.root[Event]("regular"))
    fixture.context.register(ProcessRef.SystemRef, regular).unsafeRun()

    val interpreter = DslInterpreter[TestIO](fixture.context)
    val regularFlow = suspend(TestIO.delay { executed = true })

    assertThrows[RecoveryContractViolation] {
      regularFlow
        .foldMap(
          interpreter.interpret(ProcessRef.SystemRef, fixture.context.getProcessState(regular.ref).get, Scope.empty)
        )
        .unsafeRun()
    }
    executed shouldBe false

    assertThrows[RecoveryContractViolation] {
      fork(eval { executed = true })
        .foldMap(
          interpreter.interpret(ProcessRef.SystemRef, fixture.context.getProcessState(regular.ref).get, Scope.empty)
        )
        .unsafeRun()
    }
    executed shouldBe false

    assertThrows[RecoveryContractViolation] {
      race(eval { executed = true }, eval { executed = true })
        .foldMap(
          interpreter.interpret(ProcessRef.SystemRef, fixture.context.getProcessState(regular.ref).get, Scope.empty)
        )
        .unsafeRun()
    }
    executed shouldBe false

    val boundary = new Process[TestIO, Event, Event] with ReplayBoundary:
      override val ref: ProcessRef[Event] = ProcessRef.root[Event]("storage")
      def handle: Receive                 = PartialFunction.empty

    fixture.context.register(ProcessRef.SystemRef, boundary).unsafeRun()
    suspend(TestIO.delay { executed = true })
      .foldMap(
        interpreter.interpret(ProcessRef.SystemRef, fixture.context.getProcessState(boundary.ref).get, Scope.empty)
      )
      .unsafeRun()

    executed shouldBe true
    fixture.context.stopJournal.unsafeRun()
  }

  test("Suspend remains available when recovery is disabled") {
    val fixture  = new RuntimeFixture
    var executed = false

    fixture.run(suspend(TestIO.delay { executed = true }))

    executed shouldBe true
  }

  final private case class Fixture(
      context: Context[TestIO],
      store: JournalStoreLocal[TestIO]
  )

  private def newFixture(): Fixture =
    val dir    = Files.createTempDirectory("recovery-contract")
    val config = ParConfig.default.copy(
      journal = JournalConfig(enabled = true, dataDir = dir.toString, batchSize = 1)
    )
    val store   = new JournalStoreLocal[TestIO](JournalStoreLocal.Config(dir))
    val context = Context[TestIO](
      config,
      EventTransformers.empty,
      journalStorage = Some(store),
      codecRegistry = EventCodecRegistry.empty
    ).unsafeRun()
    val scheduler = new Scheduler[TestIO]:
      def start: TestIO[Unit]                                  = TestIO.unit
      def submit(task: Task[TestIO]): TestIO[SubmissionResult] = TestIO.pure(Ok)

    context.bind(scheduler).unsafeRun()
    Fixture(context, store)

  private def process(processRef: ProcessRef[Event]): Process[TestIO, Event, Event] =
    new Process[TestIO, Event, Event]:
      override val ref: ProcessRef[Event] = processRef
      def handle: Receive                 = PartialFunction.empty
