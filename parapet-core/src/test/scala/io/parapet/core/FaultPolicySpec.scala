package io.parapet.core

import io.parapet.core.Dsl.UnitFlow
import io.parapet.core.TestUtils.TestIO
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.util.Random

class FaultPolicySpec extends AnyFunSuite:
  import FaultPolicySpec.*

  private val a      = ProcessRef.SystemRef
  private val b      = ProcessRef.DeadLetterRef
  private val sender = ProcessRef.UndefinedRef

  test("none proceeds on everything") {
    val policy = FaultPolicy.none[TestIO]
    val rng    = new Random(1L)
    assert(policy.onSend(Sent(sender, a, Tick), rng) == Fault.Proceed)
    assert(policy.onOp(sender, UnitFlow[TestIO](), rng) == Fault.Proceed)
  }

  test("dropSends drops the matching receiver and leaves others alone") {
    val policy = FaultPolicy[TestIO]().dropSends(prob = 1.0, when = _.to == a)
    val rng    = new Random(1L)
    assert(policy.onSend(Sent(sender, a, Tick), rng) == Fault.Drop)
    assert(policy.onSend(Sent(sender, b, Tick), rng) == Fault.Proceed)
  }

  test("onSend matches on the event value") {
    val policy = FaultPolicy[TestIO]().onSend { case Sent(_, _, Heartbeat) =>
      Fault.Drop
    }
    val rng = new Random(1L)
    assert(policy.onSend(Sent(sender, a, Heartbeat), rng) == Fault.Drop)
    assert(policy.onSend(Sent(sender, a, Tick), rng) == Fault.Proceed)
  }

  test("failOps fails operations with the given error") {
    val boom   = new RuntimeException("boom")
    val policy = FaultPolicy[TestIO]().failOps(prob = 1.0, boom)
    val rng    = new Random(1L)
    assert(policy.onOp(sender, UnitFlow[TestIO](), rng) == Fault.Fail(boom))
  }

  test("send rule and op rule coexist; each layer fires independently") {
    val boom   = new RuntimeException("boom")
    val policy = FaultPolicy[TestIO]().dropSends(prob = 1.0, when = _.to == a).failOps(prob = 1.0, boom)
    val rng    = new Random(1L)
    assert(policy.onSend(Sent(sender, a, Tick), rng) == Fault.Drop)
    assert(policy.onOp(sender, UnitFlow[TestIO](), rng) == Fault.Fail(boom))
  }

  test("same seed yields the same sequence of decisions") {
    def draws(): List[Fault] =
      val policy = FaultPolicy[TestIO]().dropSends(prob = 0.5, when = _.to == a)
      val rng    = new Random(42L)
      List.fill(20)(policy.onSend(Sent(sender, a, Tick), rng))

    assert(draws() == draws())
  }

  test("onOp escape hatch can target operations directly") {
    val boom   = new RuntimeException("op")
    val policy = FaultPolicy[TestIO]().onOp((_, _, _) => Fault.Fail(boom))
    val rng    = new Random(1L)
    assert(policy.onOp(sender, UnitFlow[TestIO](), rng) == Fault.Fail(boom))
  }

  test("rejects invalid probabilities and reversed delay ranges") {
    assertThrows[IllegalArgumentException](FaultPolicy[TestIO]().dropSends(prob = 1.5))
    assertThrows[IllegalArgumentException](FaultPolicy[TestIO]().dropSends(prob = -0.1))
    assertThrows[IllegalArgumentException](FaultPolicy[TestIO]().dropSends(prob = Double.NaN))
    assertThrows[IllegalArgumentException](FaultPolicy[TestIO]().delaySends(0.5, 2.seconds -> 1.second))
  }

object FaultPolicySpec:
  case object Heartbeat extends Event
  case object Tick      extends Event
