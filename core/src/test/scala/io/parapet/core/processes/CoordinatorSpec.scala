package io.parapet.core.processes

import cats.Id
import io.parapet.ProcessRef
import io.parapet.core.TestUtils.{Execution, IdInterpreter, Message}
import io.parapet.core.api.Cmd.coordinator._
import io.parapet.core.processes.Coordinator.Generator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class CoordinatorSpec extends AnyFunSuite {

  test("a node satisfying the launch condition") {

    // given
    val coordinatorRef = ProcessRef("coordinator")
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val execution = new Execution()
    val coordinator = new Coordinator[Id](coordinatorRef, "p1", p1,
      peers = Map("p2" -> p2), 0.5, generator(1.0))

    // when
    coordinator(Start).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Propose("p1", 1.0), p2),
      Message(Coordinator.Timeout, coordinatorRef))
  }

  test("a node receives propose with higher number") {
    // given
    val coordinatorRef = ProcessRef("coordinator")
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val execution = new Execution()
    val coordinator = new Coordinator[Id](coordinatorRef, "p1", p1,
      peers = Map("p2" -> p2), 0.80, generator(0.5))

    // when
    coordinator(Start).foldMap(IdInterpreter(execution))
    coordinator(Propose("p2", 0.86)).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    execution.trace.exists {
      case Message(Ack("p1", 0.86, AckCode.Ok), p2) => true
      case _ => false
    } shouldBe true

  }

  test("a node with higher number receives propose") {
    // given
    val coordinatorRef = ProcessRef("coordinator")
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val execution = new Execution()
    val coordinator = new Coordinator[Id](coordinatorRef, "p1", p1,
      peers = Map("p2" -> p2), 0.5, generator(1.0))

    // when
    coordinator(Start).foldMap(IdInterpreter(execution))
    coordinator(Propose("p2", 0.86)).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    execution.trace.exists {
      case Message(Ack("p1", 1.0, AckCode.Voted), p2) => true
      case _ => false
    } shouldBe true
  }

  test("a node has the majority of positive responses") {
    // given
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val coordinatorRef = ProcessRef("coordinator")
    val execution = new Execution()
    val coordinator = new Coordinator[Id](coordinatorRef, "p1", p1,
      peers = Map("p2" -> p2, "p3" -> p3), 0.80, generator(1))

    // when
    coordinator(Start).foldMap(IdInterpreter(execution))
    coordinator(Ack("p2", 0.5, AckCode.Ok)).foldMap(IdInterpreter(execution))
    coordinator(Ack("p3", 0.3, AckCode.Ok)).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Propose("p1", 1), p2),
      Message(Propose("p1", 1), p3),
      Message(Coordinator.Timeout, coordinatorRef),
      Message(Elected("p1"), p2),
      Message(Elected("p1"), p3),
      Message(Elected("p1"), p1))
  }

  test("a coordinator node receives proposal") {
    // given
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val coordinatorRef = ProcessRef("coordinator")
    val execution = new Execution()
    val coordinator = new Coordinator[Id](coordinatorRef, "p1", p1,
      peers = Map("p2" -> p2, "p3" -> p3), 0.80, generator(1))

    coordinator(Start).foldMap(IdInterpreter(execution))
    coordinator(Ack("p2", 0.5, AckCode.Ok)).foldMap(IdInterpreter(execution))
    coordinator(Propose("p3", 0.85)).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Propose("p1", 1.0), p2),
      Message(Propose("p1", 1.0), p3),
      Message(Coordinator.Timeout, coordinatorRef),
      Message(Elected("p1"), p2),
      Message(Elected("p1"), p3),
      Message(Elected("p1"), p1),
      Message(Ack("p1", 1.0, AckCode.Elected), p3),
      Message(Elected("p1"), p3))
  }

  test("a node that already voted receives proposal") {
    // given
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val coordinatorRef = ProcessRef("coordinator")
    val execution = new Execution()

    val coordinator = new Coordinator[Id](coordinatorRef, "p1", p1,
      peers = Map("p2" -> p2, "p3" -> p3), 0.80, generator(0.5))


    coordinator(Propose("p2", 0.85)).foldMap(IdInterpreter(execution))
    coordinator(Propose("p3", 0.86)).foldMap(IdInterpreter(execution))

    execution.print()

    execution.trace shouldBe Seq(
      Message(Ack("p1", 0.85, AckCode.Ok), p2),
      Message(Ack("p1", 0.85, AckCode.Voted), p3))

  }

  test("a candidate node waiting for majority acks receives timeout") {
    // given
    val coordinatorRef = ProcessRef("coordinator")
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val execution = new Execution()
    val coordinator = new Coordinator[Id](coordinatorRef, "p1", p1,
      peers = Map("p2" -> p2), 0.5, generator(1.0))

    // when
    coordinator(Start).foldMap(IdInterpreter(execution))
    coordinator(Coordinator.Timeout).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Propose("p1", 1.0), p2),
      Message(Coordinator.Timeout, coordinatorRef),
      Message(Start, coordinatorRef))
  }

  private def generator(value: Double): Generator = {
    new Generator {
      override def generate: Coordinator.MinMax = Coordinator.MinMax(value, value)
    }
  }
}
