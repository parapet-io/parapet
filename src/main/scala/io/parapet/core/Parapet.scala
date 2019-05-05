package io.parapet.core

import cats.data.{EitherK, State}
import cats.effect.{ContextShift, IO, Timer}
import cats.free.Free
import cats.{InjectK, Monad}
import io.parapet.core.Parapet.CatsProcess

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, implicitConversions, reflectiveCalls}

object Parapet {

  trait Event

  case class Message(sender: String, payload: String) extends Event

  implicit class EventOps[F[_]](e: => Event) {
    type ProcessFlow = Free[EitherK[FlowOp, Effect[F, ?], ?], Unit]

    def ~>(process: Process[F])(implicit FL: Flow[EitherK[FlowOp, Effect[F, ?], ?]]): ProcessFlow = FL.send(e, "")
  }

  trait Queue[F[_]] {
    def enqueue(e: => Event): F[Unit]

    def dequeue: F[Event]
  }

  trait QueueModule[F[_]] {
    def queue: Queue[F]
  }

  // <----------- Flow ADT ----------->
  trait FlowOp[A]


  object Empty extends FlowOp[Unit]

  case class Send(f: () => Event, receivers: Seq[String]) extends FlowOp[Unit]

  case class Par[F[_]](flow: Free[F, Unit]) extends FlowOp[Unit]

  case class Delay[F[_]](duration: FiniteDuration, flow: Free[F, Unit]) extends FlowOp[Unit]


  class Flow[F[_]](implicit I: InjectK[FlowOp, F]) {
    val empty: Free[F, Unit] = Free.inject[FlowOp, F](Empty)

    // sends event `e` to the list of receivers
    def send(e: => Event, receiver: String, other: String*): Free[F, Unit] = Free.inject[FlowOp, F](Send(() => e, receiver +: other))

    // changes sequential execution to parallel
    def par(flow: Free[F, Unit]): Free[F, Unit] = Free.inject[FlowOp, F](Par(flow))

    // delays execution of the given `flow`
    def delay(duration: FiniteDuration, flow: Free[F, Unit]): Free[F, Unit] = Free.inject[FlowOp, F](Delay(duration, flow))
  }

  object Flow {
    implicit def flow[F[_]](implicit I: InjectK[FlowOp, F]): Flow[F] = new Flow[F]
  }

  // <----------- Effect ADT ----------->
  // Allows to use some other effect directly outside of Flow ADT, e.g cats IO, Future, Task and etc.
  trait Effect[F[_], A]

  case class Suspend[F[_]](thunk: () => F[Unit]) extends Effect[F, Unit]

  case class Eval[F[_]](thunk: () => Unit) extends Effect[F, Unit]

  class Effects[F[_], G[_]](implicit I: InjectK[Effect[F, ?], G]) {
    // suspends an effect which produces `F`
    def suspend(thunk: => F[Unit]): Free[G, Unit] =
      Free.inject[Effect[F, ?], G](Suspend(() => thunk))

    // suspends a side effect in `F`
    def eval(thunk: => Unit): Free[G, Unit] = {
      Free.inject[Effect[F, ?], G](Eval(() => thunk))
    }
  }


  class IOEffects[F[_]](implicit I: InjectK[IOEffects.IOEffect, F]) extends Effects[IO, F]

  object IOEffects {
    type IOEffect[A] = Effect[IO, A]

    implicit def effects[F[_]](implicit I: InjectK[IOEffect, F]): IOEffects[F] = new IOEffects[F]
  }


  implicit class FreeOps[F[_], A](fa: Free[F, A]) {
    // alias for Free flatMap
    def ++[B](fb: Free[F, B]): Free[F, B] = fa.flatMap(_ => fb)
  }

  // Interpreters for ADTs
  import cats.effect.IO._
  import cats.implicits._
  import cats.~>


  type FlowState[F[_], A] = State[Seq[F[_]], A]
  type IOFlowState[A] = FlowState[IO, A]
  type CatsFlow[A] = EitherK[FlowOp, IOEffects.IOEffect, A]

  implicit def ioFlowInterpreter[Env <: QueueModule[IO] with CatsModule](env: Env): FlowOp ~> IOFlowState = new (FlowOp ~> IOFlowState) {

    def run[A](flow: Free[CatsFlow, A], interpreter: CatsFlow ~> IOFlowState): Seq[IO[_]] = {
      flow.foldMap(interpreter).runS(ListBuffer()).value
    }

    override def apply[A](fa: FlowOp[A]): IOFlowState[A] = {
      implicit val ctx: ContextShift[IO] = env.ctx
      implicit val ioTimer: Timer[IO] = env.timer
      val interpreter: CatsFlow ~> IOFlowState = ioFlowInterpreter(env) or ioEffectInterpreter
      fa match {
        case Empty => State.set(ListBuffer.empty)
        case Send(thunk, receivers) =>
          val ops = receivers.map(receiver =>  env.queue.enqueue(thunk()))
          State[Seq[IO[_]], Unit] { s => (s ++ ops, ()) }
        case Par(flow) =>
          val res = flow.asInstanceOf[Free[CatsFlow, A]]
            .foldMap(interpreter).runS(ListBuffer()).value.toList.parSequence_
          State[Seq[IO[_]], Unit] { s => (s :+ res, ()) }
        // todo: behavior needs to be determined for par / seq flow
        case Delay(duration, flow) =>
          val delayIO = IO.sleep(duration)
          val res = run(flow.asInstanceOf[Free[CatsFlow, A]], interpreter).map(op => delayIO *> op)
          State[Seq[IO[_]], Unit] { s => (s ++ res, ()) }
      }
    }
  }

  implicit def ioEffectInterpreter: IOEffects.IOEffect ~> IOFlowState = new (IOEffects.IOEffect ~> IOFlowState) {
    override def apply[A](fa: IOEffects.IOEffect[A]): IOFlowState[A] = fa match {
      case Suspend(thunk) => State.modify[Seq[IO[_]]](s => s ++ Seq(thunk()))
      case Eval(thunk) => State.modify[Seq[IO[_]]](s => s ++ Seq(IO(thunk())))
    }
  }

  // <-------------- Process -------------->
  trait Process[F[_]] {
    self =>
    type ProcessFlow = Free[EitherK[FlowOp, Effect[F, ?], ?], Unit]
    type Receive = PartialFunction[Event, ProcessFlow]

    val handle: Receive

    // composition of this and `pb` process
    def ++(pb: Process[F]): Process[F] = new Process[F] {
      override val handle: Receive = {
        case e => self.handle(e) ++ pb.handle(e)
      }
    }
  }

  trait CatsProcess extends Process[IO]

  // replace with Queue from cats effects
  def ioQueue: IO[Queue[IO]] = IO {
    new Queue[IO] {
      override def enqueue(e: => Event): IO[Unit] = IO(println(s"enqueue: $e"))

      override def dequeue: IO[Event] = IO.pure(Message("", ""))
    }
  }

  trait CatsModule {
    val ctx: ContextShift[IO]
    val timer: Timer[IO]
  }

  case class CatsAppEnv(queue: Queue[IO], ctx: ContextShift[IO], timer: Timer[IO]) extends QueueModule[IO] with CatsModule

  object CatsAppEnv {
    def apply(): IO[CatsAppEnv] =
      for {
        queue <- ioQueue
      } yield CatsAppEnv(queue,
        IO.contextShift(ExecutionContext.global),
        IO.timer(ExecutionContext.global)
      )
  }

  abstract class ParApp[F[+ _], Env](implicit M: Monad[F]) {
    type EffectF[A] = Effect[F, A]
    type ProcessFlow = Free[EitherK[FlowOp, EffectF, ?], Unit]
    type FlowStateF[A] = FlowState[F, A]

    val env: F[Env]

    def flowInterpreter(e: Env): FlowOp ~> FlowStateF

    def effectInterpreter(e: Env): EffectF ~> FlowStateF

    def program: ProcessFlow

    def run: F[Unit] => Unit

    def main(args: Array[String]): Unit = {
      val p = for {
        e <- env
        interpreter <- M.pure(flowInterpreter(e) or effectInterpreter(e))
        _ <- program.foldMap[FlowStateF](interpreter)
          .runS(ListBuffer()).value.toList.sequence_
      } yield ()
      run(p)
    }
  }

  abstract class CatsApp extends ParApp[IO, CatsAppEnv] {
    override val env: IO[CatsAppEnv] = CatsAppEnv()

    override def flowInterpreter(e: CatsAppEnv): FlowOp ~> FlowStateF = ioFlowInterpreter(e)

    override def effectInterpreter(e: CatsAppEnv): EffectF ~> FlowStateF = ioEffectInterpreter

    override def run: IO[Unit] => Unit = _.unsafeRunSync()
  }

}

import io.parapet.core.Parapet.{CatsApp, Event}

object MyApp extends CatsApp {

  import CounterProcess._
  import io.parapet.core.catsInstances.flow._ // for Flow DSL
  import io.parapet.core.catsInstances.effect._ // for Effect DSL

  // Client program
  class CounterProcess extends CatsProcess /* Process[IO] */ {
    // state
    var counter = 0

    val handle: Receive = {
      case Inc =>
        eval {
          counter = counter + 1
        } ++ eval(println(s"counter=$counter"))
    }
  }

  object CounterProcess {

    // API
    object Inc extends Event

  }

  override def program: ProcessFlow = {
    val counter = new CounterProcess()
    Inc ~> counter ++ Inc ~> counter ++
      Inc ~> (counter ++ counter) ++ // compose two processes
      eval(println("===============")) ++ // print line
      par(eval(println("a")) ++ eval(println("b")) ++ eval(println("c")))
  }

  // possible outputs:
  //  counter=1
  //  counter=2
  //  counter=3
  //  counter=4
  //  ===============
  //  a
  //  b
  //  c
  //  ^ any permutation of a,b,c

}