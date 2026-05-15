package io.parapet.testutils

import io.parapet.effect.Effect
import io.parapet.effect.Effect.*
import io.parapet.effect.Monad.*
import io.parapet.{Event, ProcessRef}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{FiniteDuration, _}
import scala.jdk.CollectionConverters.*

class EventStore[F[_], A <: Event](using effect: Effect[F]) {
  type EventList = ListBuffer[A]

  private val eventMap = new ConcurrentHashMap[ProcessRef[?], EventList]()
  private val sizeRef  = new AtomicInteger()

  def receivers: Set[ProcessRef[?]] = eventMap.keySet().asScala.toSet

  def add(pRef: ProcessRef[?], event: A): Unit = {
    sizeRef.incrementAndGet()
    eventMap.computeIfAbsent(pRef, _ => ListBuffer.empty)
    eventMap.computeIfPresent(pRef, (_: ProcessRef[?], events: EventList) => events += event)
  }

  def get(pRef: ProcessRef[?]): Seq[A] =
    eventMap.getOrDefault(pRef, ListBuffer.empty).toSeq

  def allEvents: Seq[A] =
    eventMap.values().asScala.flatten.toSeq

  def groupBy[K](groupKeyOf: (ProcessRef[?], A) => K): Map[K, Seq[A]] = {
    final case class Entry(groupKey: K, event: A)

    eventMap.asScala.iterator
      .flatMap { case (ref, buf) =>
        buf.toSeq.iterator.map { event =>
          Entry(groupKeyOf(ref, event), event)
        }
      }
      .toSeq
      .groupMap(_.groupKey)(_.event)
  }

  def count(pRef: ProcessRef[?]): Int = get(pRef).size

  def size: Int =
    sizeRef.get()

  def await(
      expectedSize: Int,
      op: F[Unit],
      delay: FiniteDuration = 100.millis,
      timeout: FiniteDuration = 1.minute
  ): F[Unit] =
    for
      fiber <- effect.start(op)
      _     <- await0(expectedSize, fiber, delay, timeout)
    yield ()

  def await0(
      expectedSize: Int,
      fiber: io.parapet.effect.EffectFiber[F, Unit],
      delay: FiniteDuration = 100.millis,
      timeout: FiniteDuration = 1.minute
  ): F[Unit] = {
    def cancelAndWait: F[Unit] =
      effect
        .race(
          fiber.cancel >> fiber.join.void.handleErrorWith(_ => effect.pure(())),
          effect.sleep(5.seconds)
        )
        .void

    def waitForEvents: F[Unit] =
      if size >= expectedSize then effect.pure(())
      else effect.sleep(delay) >> waitForEvents

    val waitWithTimeout =
      effect
        .race(
          waitForEvents,
          effect.sleep(timeout) >> effect.raiseError[Unit](new TimeoutException(timeout.toString))
        )
        .void

    effect.guarantee(effect.race(waitWithTimeout, fiber.join).void)(cancelAndWait)
  }
}

object EventStore {
  case object Dummy extends Event
}
