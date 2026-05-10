package io.parapet.effect

import io.parapet.core.Parallel

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

  /** Sequencing constructor used by [[ParIO.flatMap]]. */
  final case class FlatMap[A, B](source: ParIO[A], bind: A => ParIO[B]) extends ParIO[B]

  /** Error-recovery constructor used by [[ParIO.handleErrorWith]]. */
  final case class HandleError[A](source: ParIO[A], handler: Throwable => ParIO[A]) extends ParIO[A]

  /** Describes a duration delay. How this is scheduled depends on the active [[ParIORuntime]]. */
  final case class Sleep(duration: FiniteDuration) extends ParIO[Unit]

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
