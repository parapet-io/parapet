package io.parapet.demo.coloring

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

class GraphColoringSimulationSpec extends AnyFunSuite:
  test("simulation eventually colors the sample graph") {
    val simulation = new GraphColoringSimulation(seed = 11)

    var state = simulation.snapshot()
    var guard = 0
    while !state.completed && guard < 20 do
      state = simulation.stepRound()
      guard = guard + 1

    state.completed shouldBe true
    state.nodes.forall(_.color.nonEmpty) shouldBe true
  }

  test("reset clears coloring progress") {
    val simulation = new GraphColoringSimulation(seed = 11)

    simulation.stepRound()
    val reset = simulation.reset()

    reset.round shouldBe 0
    reset.completed shouldBe false
    reset.nodes.forall(node => node.color.isEmpty && node.proposedColor.isEmpty) shouldBe true
  }

  test("configure regenerates the graph with requested size and palette") {
    val simulation = new GraphColoringSimulation(seed = 11)

    val configured = simulation.configure(nodeCount = 100, paletteSize = 6)

    configured.nodeCount shouldBe 100
    configured.paletteSize shouldBe 6
    configured.nodes should have size 100
    configured.cluster should have size 100
    configured.canvasWidth should be > 900
  }
