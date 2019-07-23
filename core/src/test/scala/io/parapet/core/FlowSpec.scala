package io.parapet.core

import cats.effect.{ContextShift, IO, Timer}
import io.parapet.core.ProcessRef._
import io.parapet.core.Scheduler._
import io.parapet.implicits._
import io.parapet.instances.DslInterpreterInstances.dslInterpreterForCatsIO._
import org.scalatest.FlatSpec
import org.scalatest.Matchers.{empty => _, _}
import org.scalatest.OptionValues._
import io.parapet.core.Dsl._

import scala.collection.mutable.{ListBuffer, Queue => SQueue}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


class FlowSpec extends FlatSpec with  WithDsl [IO]{
//  import flowDsl._
//  import effectDsl._
//
//  import FlowSpec._
//  // m1 ~> p1
//  "Flow send" should "enqueue an event" in {
//    val program = TestEvent("1") ~> blackhole
//    val taskQueue = run_(program)
//    taskQueue.getSize shouldBe 1
//    taskQueue.peek.value.event shouldBe TestEvent("1")
//  }
//
//  // m1 ~> p1 ++ m2 ~> p1
//  "Flow composition of two seq flows" should "send events in program order" in {
//    val program = TestEvent("1") ~> blackhole ++ TestEvent("2") ~> blackhole
//    val taskQueue = run_(program)
//    taskQueue.getSize shouldBe 2
//    taskQueue.pull.value.event shouldBe TestEvent("1")
//    taskQueue.pull.value.event shouldBe TestEvent("2")
//  }
//
//  // par(m1 ~> p1, m2 ~> p1)
//  "Par flow" should "send events in parallel" in {
//    val program =
//      par(
//        delay(3.seconds) ++ TestEvent("1") ~> blackhole, // <- delay first event
//        TestEvent("2") ~> blackhole
//      )
//
//    val taskQueue = run_(program)
//    taskQueue.getSize shouldBe 2
//    taskQueue.pull.value.event shouldBe TestEvent("2")
//    taskQueue.pull.value.event shouldBe TestEvent("1")
//  }
//
//  // m1 ~> p1 ++ par(m2 ~> p1 ++ m3 ~> p1) ++ m4 ~> p1
//  // valid executions:
//  // (1) m1 -> m2 -> m3 -> m4
//  // (2) m1 -> m3 -> m2 -> m4
//  "Composition of seq and par flow" should "send events in program order" in {
//    val program =
//      TestEvent("1") ~> blackhole ++
//        par(delay(3.seconds) ++ TestEvent("2") ~> blackhole, TestEvent("3") ~> blackhole) ++
//        TestEvent("4") ~> blackhole
//
//    val taskQueue = run_(program)
//
//    taskQueue.getSize shouldBe 4
//    taskQueue.pull.value.event shouldBe TestEvent("1")
//    taskQueue.pull.value.event shouldBe TestEvent("3")
//    taskQueue.pull.value.event shouldBe TestEvent("2")
//    taskQueue.pull.value.event shouldBe TestEvent("4")
//  }
//
//  "Send" should "create evaluate event lazily" in {
//
//    var i = 0
//    val events = new ListBuffer[Event]()
//    val taskQueue = new IOQueue[Task[IO]]
//    val program =
//      eval {
//        println("first")
//        i = i + 1
//      } ++ use(i){ i1 => TestEvent(i1.toString) ~> blackhole } ++
//      eval { events += taskQueue.pull.value.event } ++
//      flow(suspend(IO {
//        println("second")
//        i = i + 1 })) ++ use(i){ i1 => TestEvent(i1.toString) ~> blackhole } ++
//      eval { events += taskQueue.pull.value.event }
//
//    run_(program, taskQueue)
//    taskQueue.getSize shouldBe 0
//    events.size shouldBe 2
//    events.head shouldBe TestEvent("1")
//    events.last shouldBe TestEvent("2")
//  }
//
//  // proposal use `use` to capture state
//  "Events sent using 'use'" should  "be evaluated before enqueue" in {
//    var x, y = 0
//    val program =
//      eval {
//        x = x + 1
//        y = y + 2
//      } ++
//        use((x, y)) { case (x1, y1) => TestEvent((x1 + y1).toString) ~> blackhole }
//
//    val taskQueue = run_(program)
//    taskQueue.getSize shouldBe 1
//    taskQueue.pull.value.event shouldBe TestEvent("3")
//  }
//
//}
//
//object FlowSpec extends WithDsl [IO]{
//  import flowDsl._
//  import effectDsl._
//
//  val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
//  implicit val contextShift: ContextShift[IO] = IO.contextShift(ec)
//  implicit val ioTimer: Timer[IO] = IO.timer(ec)
//
//  def run_(program: DslF[IO, Unit], taskQueue: IOQueue[Task[IO]]): IOQueue[Task[IO]]  = {
//    val ctx = new Context[IO](Parapet.defaultConfig, taskQueue)
//    val interpreter = ioFlowInterpreter(ctx) or ioEffectInterpreter(ctx)
//    interpret_(program, interpreter, FlowState(SystemRef, SystemRef))
//      .map(_ => taskQueue).unsafeRunSync()
//  }
//
//  def run_(f: DslF[IO, Unit]): IOQueue[Task[IO]] = {
//    run_(f, new IOUnboundedQueue)
//  }
//
//
//  type IOQueue[A] = IOUnboundedQueue[A]
//  class IOUnboundedQueue[A] extends Queue[IO, A] {
//    val queue: SQueue[A] = new SQueue()
//
//    override def enqueue(a: A): IO[Unit] = IO(queue.enqueue(a))
//
//    override def dequeue: IO[A] = IO(queue.dequeue())
//
//    def peek: IO[Option[A]] = queue.headOption
//    def peekLast: Option[A] = queue.lastOption
//
//    def pull: Option[A] = Option(queue.dequeue())
//    override def size: IO[Int] = IO.pure(queue.size)
//    def getSize: Int = queue.size
//
//    override def tryDequeue: IO[Option[A]] = IO(Option(queue.dequeue()))
//
//    override def tryEnqueue(a: A): IO[Boolean] = IO {
//      queue.enqueue(a)
//      true
//    }
//
//  }
//
//  case class TestEvent(body: String) extends Event
//
//  val blackhole = new Process[IO] {
//    val handle: Receive = {
//      case _ => empty
//    }
//  }
//
//  implicit class TaskOps[F[_]](task: Task[F]) {
//    def event: Event = task.asInstanceOf[Deliver[IO]].envelope.event
//  }

}