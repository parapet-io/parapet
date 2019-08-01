package io.parapet.testutils

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{Concurrent, ContextShift, Timer}
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.parapet.core.{Event, ProcessRef}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.TimeoutException
import scala.concurrent.duration.{FiniteDuration, _}

class EventStore[F[_] : Concurrent, A <: Event] {

  type EventList = ListBuffer[A]

  private val ct = implicitly[Concurrent[F]]

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

  def await(expectedSize: Int, op: F[_], delay: FiniteDuration = 100.millis,
            timeout: FiniteDuration = 1.minutes)
           (implicit timer: Timer[F]): F[Unit] = {

    def step: F[Unit] = {
      if (size >= expectedSize) ct.unit
      else timer.sleep(delay) >> step
    }

    for {
      fiber <- ct.start(op)
      _ <- ct.race(
        ct.guarantee(
          Concurrent.timeoutTo[F, Unit](step, timeout, ct.raiseError(new TimeoutException(timeout.toString))))
        (fiber.cancel),
        fiber.join)
    } yield ()

  }
}