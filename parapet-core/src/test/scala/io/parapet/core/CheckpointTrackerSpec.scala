package io.parapet.core

import io.parapet.effect.Clock
import io.parapet.runtime.Context.CheckpointTracker
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import scala.concurrent.duration.*

class CheckpointTrackerSpec extends AnyFunSuite:

  private def tracker(clock: Clock = new Clock.Mock(0.millis)) = new CheckpointTracker(clock)

  test("due on the event-count ceiling") {
    val t = tracker()
    t.onDelivered(seq = 1L)
    t.onDelivered(seq = 2L)
    t.due(maxEvents = 3, maxIntervalMillis = 0) shouldBe false
    t.onDelivered(seq = 3L)
    t.due(maxEvents = 3, maxIntervalMillis = 0) shouldBe true
  }

  test("time trigger disabled (0) never fires no matter how much time passes") {
    val clock = new Clock.Mock(0.millis)
    val t     = tracker(clock)
    t.onDelivered(seq = 1L)
    clock.update(10.seconds)
    t.due(maxEvents = 100, maxIntervalMillis = 0) shouldBe false
  }

  test("time trigger fires once the interval elapses while dirty") {
    val clock = new Clock.Mock(0.millis)
    val t     = tracker(clock)
    t.onDelivered(seq = 1L) // anchors the window at 0
    clock.update(49.millis)
    t.due(maxEvents = 100, maxIntervalMillis = 50) shouldBe false
    clock.update(50.millis)
    t.due(maxEvents = 100, maxIntervalMillis = 50) shouldBe true
  }

  test("time trigger does not fire when clean (no unsnapshotted deliveries)") {
    val clock = new Clock.Mock(0.millis)
    val t     = tracker(clock)
    clock.update(10.seconds)
    t.due(maxEvents = 100, maxIntervalMillis = 50) shouldBe false
  }

  test("onSnapshot resets the event count and the time window (measured from the snapshot)") {
    val clock = new Clock.Mock(0.millis)
    val t     = tracker(clock)
    (1 to 5).foreach(i => t.onDelivered(seq = i.toLong))
    clock.update(100.millis)
    t.onSnapshot()
    t.dirty shouldBe false
    t.due(maxEvents = 3, maxIntervalMillis = 50) shouldBe false

    clock.update(120.millis)
    t.onDelivered(seq = 6L) // dirty again; window still anchored at the snapshot (100ms)
    clock.update(149.millis)
    t.due(maxEvents = 100, maxIntervalMillis = 50) shouldBe false
    clock.update(150.millis)
    t.due(maxEvents = 100, maxIntervalMillis = 50) shouldBe true
  }
