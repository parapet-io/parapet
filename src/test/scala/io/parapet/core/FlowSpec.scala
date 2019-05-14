package io.parapet.core

import cats.effect.{ContextShift, IO, Timer}
import io.parapet.core.FlowSpec._
import io.parapet.core.Parapet._
import io.parapet.core.Parapet.Process
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow._
import org.scalatest.{FlatSpec, _}

import scala.collection.mutable.{ListBuffer, Queue => SQueue}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class FlowSpec extends FlatSpec with Matchers with OptionValues {

  // m1 ~> p1
  "Flow send" should "enqueue an event" in {
    val program = TestEvent("1") ~> blackhole
    val env = interpret(program).unsafeRunSync()
    env.taskQueue.size shouldBe 1
    env.taskQueue.peek.value.evalEvent shouldBe TestEvent("1")
  }

  // m1 ~> p1 ++ m2 ~> p1
  "Flow composition of two seq flows" should "send events in program order" in {
    val program = TestEvent("1") ~> blackhole ++ TestEvent("2") ~> blackhole
    val env = interpret(program).unsafeRunSync()
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

    val env = interpret(program).unsafeRunSync()
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

    val env = interpret(program).unsafeRunSync()

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

    interpret(program, env).unsafeRunSync()
    env.taskQueue.size shouldBe 0
    events.size shouldBe 2
    events.head shouldBe TestEvent("1")
    events.last shouldBe TestEvent("2")
  }

}

object FlowSpec {

  import cats.effect.IO._
  import cats.implicits._

  val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val ioTimer: Timer[IO] = IO.timer(ec)

  def interpret(f: FlowF[IO, Unit], env: TestEnv): IO[TestEnv] = {
    val interpreter = ioFlowInterpreter(env) or ioEffectInterpreter
    f.foldMap[FlowState[IO, ?]](interpreter).runS(ListBuffer()).value.toList.sequence_.map(_ => env)
  }

  def interpret(f: FlowF[IO, Unit]): IO[TestEnv] = {
    interpret(f, new TestEnv())
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
    def evalEvent: Event = task.asInstanceOf[Deliver[IO]].event()
  }

}