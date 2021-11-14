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
      peers = Map("p2" -> p2), 0.5, generator(0.1))

    // when
    coordinator(Propose("p2", 0.86)).foldMap(IdInterpreter(execution))

    // then
    execution.print()
    
    execution.trace.exists {
      case Message(Ack("p1", 0.86, AckCode.Ok), p2) => true
      case _ => false
    } shouldBe true

  }

  private def generator(value: Double): Generator = {
    new Generator {
      override def generate: Coordinator.MinMax = Coordinator.MinMax(value, value)
    }
  }
}
