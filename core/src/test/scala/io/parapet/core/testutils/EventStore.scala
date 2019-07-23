package io.parapet.core.testutils

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{ContextShift, Fiber, IO, Timer}
import io.parapet.core.{Event, ProcessRef}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, _}
import cats.syntax.flatMap._
import cats.syntax.functor._

class EventStore[A <: Event] {
  type EventList = ListBuffer[A]
  private val eventMap: java.util.Map[ProcessRef, EventList] =
    new java.util.concurrent.ConcurrentHashMap[ProcessRef, EventList]()

  private val sizeRef = new AtomicInteger()

  def add(pRef: ProcessRef, event: A): Unit = {
    sizeRef.incrementAndGet()
    eventMap.computeIfAbsent(pRef, _ => ListBuffer())
    eventMap.computeIfPresent(pRef, (_: ProcessRef, events: EventList) => events += event)
  }

  def get(pRef: ProcessRef): Seq[A] = eventMap.getOrDefault(pRef, ListBuffer.empty)

  def allEvents: Seq[A] = eventMap.values().asScala.flatten.toSeq

  def print(): Unit = {
    println("===== Event store ====")
    eventMap.forEach { (ref: ProcessRef, events: EventList) =>
      println(s"$ref  -> $events")
    }
  }

  def size: Int = sizeRef.get()

  def awaitSizeOld(expectedSize: Int, delay: FiniteDuration = 100.millis,
                   timeout: FiniteDuration = 3.minutes)
                  (implicit timer: Timer[IO], ctx: ContextShift[IO]): IO[Unit] = {
    def step: IO[Unit] = {
      if (size >= expectedSize) IO.unit
      else IO.sleep(delay) >> step
    }

    step.timeout(timeout)
  }

  def awaitSize(expectedSize: Int, op: IO[_], delay: FiniteDuration = 100.millis,
                timeout: FiniteDuration = 1.minutes)
               (implicit timer: Timer[IO], ctx: ContextShift[IO]): IO[Unit] = {
    for {
      fiber <- op.start
      _ <- IO.race(
        awaitSizeOld(expectedSize, delay, timeout).guaranteeCase(_ => fiber.cancel),
        fiber.join).void
    } yield ()

  }
}

