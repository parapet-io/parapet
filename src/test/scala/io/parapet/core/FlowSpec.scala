package io.parapet.core

import cats.effect.{ContextShift, IO, Timer}
import io.parapet.core.Parapet.ProcessRef._
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatest.OptionValues._

import scala.collection.mutable.{ListBuffer, Queue => SQueue}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class FlowSpec extends FlatSpec {
  import FlowSpec._
  // m1 ~> p1
  "Flow send" should "enqueue an event" in {
    val program = TestEvent("1") ~> blackhole
    val env = run_(program)
    env.taskQueue.size shouldBe 1
    env.taskQueue.peek.value.evalEvent shouldBe TestEvent("1")
  }

  // m1 ~> p1 ++ m2 ~> p1
  "Flow composition of two seq flows" should "send events in program order" in {
    val program = TestEvent("1") ~> blackhole ++ TestEvent("2") ~> blackhole
    val env = run_(program)
    env.taskQueue.size shouldBe 2
    env.taskQueue.pull.value.evalEvent shouldBe TestEvent("1")
    env.taskQueue.pull.value.evalEvent shouldBe TestEvent("2")
  }

  // par(m1 ~> p1, m2 ~> p1)
  "Par flow" should "send events in parallel" in {
    val program =
      par(
        delay(3.seconds) ++ TestEvent("1") ~> blackhole, // <- delay first event
        TestEvent("2") ~> blackhole
      )

    val env = run_(program)
    env.taskQueue.size shouldBe 2
    env.taskQueue.pull.value.evalEvent shouldBe TestEvent("2")
    env.taskQueue.pull.value.evalEvent shouldBe TestEvent("1")
  }

  // m1 ~> p1 ++ par(m2 ~> p1 ++ m3 ~> p1) ++ m4 ~> p1
  // valid executions:
  // (1) m1 -> m2 -> m3 -> m4
  // (2) m1 -> m3 -> m2 -> m4
  "Composition of seq and par flow" should "send events in program order" in {
    val program =
      TestEvent("1") ~> blackhole ++
        par(delay(3.seconds) ++ TestEvent("2") ~> blackhole, TestEvent("3") ~> blackhole) ++
        TestEvent("4") ~> blackhole

    val env = run_(program)

    env.taskQueue.size shouldBe 4
    env.taskQueue.pull.value.evalEvent shouldBe TestEvent("1")
    env.taskQueue.pull.value.evalEvent shouldBe TestEvent("3")
    env.taskQueue.pull.value.evalEvent shouldBe TestEvent("2")
    env.taskQueue.pull.value.evalEvent shouldBe TestEvent("4")
  }

  "Send" should "create evaluate event lazily" in {
    val env = new TestEnv()
    var i = 0
    val events = new ListBuffer[Event]()

    val program =
      eval { i = i + 1 } ++ TestEvent(i.toString) ~> blackhole ++
      eval { events += env.taskQueue.pull.value.evalEvent } ++
      suspend(IO { i = i + 1 }) ++ TestEvent(i.toString) ~> blackhole ++
      eval { events += env.taskQueue.pull.value.evalEvent }

    run_(program, env)
    env.taskQueue.size shouldBe 0
    events.size shouldBe 2
    events.head shouldBe TestEvent("1")
    events.last shouldBe TestEvent("2")
  }

}

object FlowSpec {

  import cats.effect.IO._
  import io.parapet.core.catsInstances.flow._
  val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val ioTimer: Timer[IO] = IO.timer(ec)

  def run_(program: FlowF[IO, Unit], env: TestEnv): TestEnv = {
    val interpreter = ioFlowInterpreter(env) or ioEffectInterpreter
    interpret_(program, interpreter, FlowState(SystemRef, SystemRef)).map(_ => env).unsafeRunSync()
  }

  def run_(f: FlowF[IO, Unit]): TestEnv = {
    run_(f, new TestEnv())
  }

  class TestEnv extends TaskQueueModule[IO] with CatsModule {
    override val taskQueue: IOQueue[Task[IO]] = new IOQueue[Task[IO]]()
    override val ctx: ContextShift[IO] = IO.contextShift(ec)
    override val timer: Timer[IO] = IO.timer(ec)
  }

  class IOQueue[A] extends Queue[IO, A] {
    val queue: SQueue[A] = new SQueue()

    override def enqueue(e: => A): IO[Unit] = IO(queue.enqueue(e))

    override def dequeue: IO[A] = IO(queue.dequeue())

    def peek: Option[A] = queue.headOption
    def peekLast: Option[A] = queue.lastOption

    def pull: Option[A] = Option(queue.dequeue())
    def size: Int = queue.size
  }

  case class TestEvent(id: String) extends Event

  val blackhole = Process[IO] {
    case _ => empty
  }

  implicit class TaskOps[F[_]](task: Task[F]) {
    def evalEvent: Event = task.asInstanceOf[Deliver[IO]].envelope.event()
  }

}