package io.parapet.core


import java.util
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import cats.effect.concurrent.Deferred
import io.parapet.core.EventDeliveryHooks._

import scala.collection.JavaConverters._

class EventDeliveryHooks[F[_]] {

  private val hooks = new util.concurrent.ConcurrentHashMap[ProcessRef, Hook[F]]()

  def add(ref: ProcessRef,
          selector: PartialFunction[Event, Unit], d: Deferred[F, Unit]): String = {

    hooks.computeIfAbsent(ref, _ => new Hook)
    hooks.get(ref).add(OnEvent(selector, d))
  }

  def removeFirstMatch(ref: ProcessRef, event: Event): Option[OnEvent[F]] = {
    Option(hooks.get(ref)).flatMap(hs => hs.removeFirstMatch(event))
  }

  def remove(ref: ProcessRef, token: String): Option[OnEvent[F]] = {
    Option(hooks.get(ref)).flatMap(hs => hs.remove(token))
  }

  def size(ref: ProcessRef): Int = Option(hooks.get(ref)).map(_.size).getOrElse(0)

}

object EventDeliveryHooks {

  case class OnEvent[F[_]](selector: PartialFunction[Event, Unit], d: Deferred[F, Unit])

  class Hook[F[_]] {
    private[this] val lock = new ReentrantLock()
    private[this] val hooks = new util.LinkedHashMap[String, OnEvent[F]]()

    def add(hook: OnEvent[F]): String = {
      try {
        lock.lock()
        val token = UUID.randomUUID().toString
        hooks.put(token, hook)
        token
      } finally {
        lock.unlock()
      }
    }

    def remove(token: String): Option[OnEvent[F]] = {
      try {
        lock.lock()
        Option(hooks.remove(token))
      } finally {
        lock.unlock()
      }
    }

    def removeFirstMatch(event: Event): Option[OnEvent[F]] = {
      try {
        lock.lock()
        hooks.asScala.find {
          case (_, OnEvent(selector, _)) => selector.isDefinedAt(event)
        }.map {
          case (k, _) => hooks.remove(k)
        }
      } finally {
        lock.unlock()
      }
    }

    def size: Int = hooks.size()
  }

}

