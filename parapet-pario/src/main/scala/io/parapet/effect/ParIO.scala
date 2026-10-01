package io.parapet.effect

import scala.concurrent.duration.FiniteDuration

/** Reference effect type for Parapet. ParIO is not the recommended production backend.
  */
sealed trait ParIO[+A]:
  /** Maps the result. */
  final def map[B](f: A => B): ParIO[B] =
    flatMap(a => ParIO.pure(f(a)))

  /** Sequences the next computation. */
  final def flatMap[B](f: A => ParIO[B]): ParIO[B] =
    ParIO.FlatMap(this, f)

  /** Recovers from an exception via `f`. */
  final def handleErrorWith[B >: A](f: Throwable => ParIO[B]): ParIO[B] =
    ParIO.HandleError(this, f)

  /** Synchronously runs the program on the calling thread, blocking until it completes (or rethrowing any uncaught
    * error).
    */
  final def unsafeRunSync(): A =
    ParIO.runtime.unsafeRun(this)

  /** Runs `finalizer` if this computation is canceled. */
  final def onCancel(finalizer: ParIO[Unit]): ParIO[A] =
    ParIO.OnCancel(this, finalizer)

  /** Runs `finalizer` after this computation regardless of success, failure, or cancellation. */
  final def guarantee(finalizer: ParIO[Unit]): ParIO[A] =
    ParIO.Guarantee(this, finalizer)

/** [[ParIO]] constructors and the default runtime-backed type-class instances for the reference runtime. */
object ParIO:
  /** Wraps an already-known value. */
  final case class Pure[A](value: A) extends ParIO[A]

  /** A pure but lazy computation. */
  final case class Delay[A](thunk: () => A) extends ParIO[A]

  /** A computation that must be shifted to the runtime's blocking context before it runs. */
  final case class Blocking[A](thunk: () => A) extends ParIO[A]

  /** Defers construction of a `ParIO`. */
  final case class Suspend[A](thunk: () => ParIO[A]) extends ParIO[A]

  /** Races two computations. A canceled participant does not win the race. */
  final case class Race[A, B](left: ParIO[A], right: ParIO[B]) extends ParIO[Either[A, B]]

  /** Sequencing constructor used by [[ParIO.flatMap]]. */
  final case class FlatMap[A, B](source: ParIO[A], bind: A => ParIO[B]) extends ParIO[B]

  /** Error-recovery constructor used by [[ParIO.handleErrorWith]]. */
  final case class HandleError[A](source: ParIO[A], handler: Throwable => ParIO[A]) extends ParIO[A]

  /** Requests cancellation of the current fiber. */
  case object Canceled extends ParIO[Unit]

  /** Describes a duration delay. How this is scheduled depends on the active [[ParIORuntime]]. */
  final case class Sleep(duration: FiniteDuration) extends ParIO[Unit]

  /** Registers a finalizer for cancellation of `source`. */
  final case class OnCancel[+A](source: ParIO[A], finalizer: ParIO[Unit]) extends ParIO[A]

  /** Registers a finalizer for every outcome of `source`. */
  final case class Guarantee[+A](source: ParIO[A], finalizer: ParIO[Unit]) extends ParIO[A]

  /** Materializes the terminal outcome of `source` in an isolated cancellation scope. */
  final case class OutcomeOf[+A](source: ParIO[A]) extends ParIO[Outcome[A]]

  /** Lifts a pure value. */
  def pure[A](value: A): ParIO[A] =
    Pure(value)

  /** The `pure(())` constant. */
  def unit: ParIO[Unit] =
    pure(())

  /** Defers a pure but lazy computation. */
  def delay[A](thunk: => A): ParIO[A] =
    Delay(() => thunk)

  /** Defers a blocking computation. */
  def blocking[A](thunk: => A): ParIO[A] =
    Blocking(() => thunk)

  /** Defers construction of a `ParIO`. */
  def suspend[A](thunk: => ParIO[A]): ParIO[A] =
    Suspend(() => thunk)

  /** Aborts with `error` when interpreted. */
  def raiseError[A](error: Throwable): ParIO[A] =
    Delay(() => throw error)

  /** Requests cancellation of the current fiber. */
  def canceled: ParIO[Unit] =
    Canceled

  /** Describes a duration delay. */
  def sleep(duration: FiniteDuration): ParIO[Unit] =
    Sleep(duration)

  private lazy val defaultRuntime = ParIORuntime.default

  /** The default singleton runtime used by [[unsafeRunSync]] and by code that relies on [[ParIO.effect]] /
    * [[ParIO.parallel]] directly.
    */
  def runtime: ParIORuntime =
    defaultRuntime

  given effect: Effect[ParIO] = runtime.effect

  given parallel: Parallel[ParIO] = runtime.parallel
