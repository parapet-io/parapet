package io.parapet.benchmark.catsio

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import io.parapet.core.Queue
import io.parapet.core.Queue.{ChannelType, MonixBasedQueue}

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

object Benchmark extends IOApp {

  trait Event

  case object Start extends Event

  case object Request extends Event

  case object Stop extends Event

  class EventQueue {
    private val queue = new ConcurrentLinkedQueue[Event]()

    def enqueue(e: Event): Unit = queue.add(e)

    def deque(): Option[Event] = Option(queue.poll())
  }

  abstract class Actor {
    private val _active: AtomicBoolean = new AtomicBoolean()
    private val _more: AtomicBoolean = new AtomicBoolean()
    val queue = new EventQueue()

    def activate(): Boolean = _active.compareAndSet(false, true)
    def active(): Boolean = _active.get()
    def deactivate(): Boolean = _active.compareAndSet(true, false)
    def setMore(): Unit = _more.set(true)
    def resetMore(): Boolean = _more.compareAndSet(true, false)
    val handle: PartialFunction[Event, IO[Unit]]
  }

  class Worker(val name: String, scheduler: Scheduler) {

    def run: IO[Unit] = {
      def loop: IO[Unit] = {
        scheduler.queue.dequeue >>= (actor => process(actor) >> loop)
      }

      loop
    }

    def process(actor: Actor): IO[Unit] = {
      //println(s"$name -- process")
      for {
        activated <- IO(actor.activate())
        _ <- if (activated) process0(actor) else IO.unit
      } yield ()
    }

    def process0(actor: Actor): IO[Unit] = {
      for {
        _ <- drain(actor)
        _ <- IO(actor.deactivate())
        hasMore <- IO(actor.resetMore())
        _ <- if (hasMore) scheduler.queue.enqueue(actor) else IO.unit
      } yield ()
    }

    def drain(actor: Actor): IO[Unit] = {
      IO(actor.queue.deque()).flatMap {
        case Some(e) =>
          actor.handle(e) >> drain(actor)
        case None => IO.unit
      }
    }
  }

  class Scheduler(val queue: Queue[IO, Actor], workersN: Int) {
    self =>

    private val workers = (0 until workersN).map(i => new Worker(s"worker-$i", self).run).toList.parSequence_

    def run: IO[Unit] = workers

    def schedule(actor: Actor): IO[Unit] = {
      for {
        active <- IO {
          actor.setMore()
          actor.active()
        }
        _ <- if (!active) queue.enqueue(actor) else IO.unit
      } yield ()
    }
  }

  def send(event: Event, actor: Actor, scheduler: Scheduler): IO[Unit] = {
    IO(actor.queue.enqueue(event)) >> scheduler.schedule(actor)
  }

  class Sender(actor: Actor, scheduler: Scheduler, events: Int) {
    def run: IO[Unit] = {
      send(Start, actor, scheduler) >>
        (0 until events).map { _ =>
          send(Request, actor, scheduler)
        }.toList.sequence_ >> send(Stop, actor, scheduler)
    }
  }

  class BenchmarkActor extends Actor {
    var _startTime = 0L
    var _time = 0L
    var _count = 0

    override val handle: PartialFunction[Event, IO[Unit]] = {
      case Start => IO {
        _startTime = System.nanoTime()
      }.void
      case Request => IO {
        _count = _count + 1
      }.void
      case Stop => IO {
        _time = System.nanoTime() - _startTime
        println(s"count: ${_count}, time: ${TimeUnit.MILLISECONDS.convert(_time, TimeUnit.NANOSECONDS)}ms")
      }.void
    }
  }

  val benchmarkActor = new BenchmarkActor()
  val workersCount = 1
  val events = 1000000

  override def run(args: List[String]): IO[ExitCode] = {
    for {
      queue <- MonixBasedQueue.unbounded[IO, Actor](ChannelType.MPMC)
      scheduler <- IO.pure(new Scheduler(queue, workersCount))
      fiber <- scheduler.run.start
      _ <- new Sender(benchmarkActor, scheduler, events).run
      _ <- fiber.join
    } yield ExitCode.Success
  }
}
