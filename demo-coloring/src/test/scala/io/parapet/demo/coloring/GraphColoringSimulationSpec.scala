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

  test("burst appends a connected cluster tagged with a fresh clusterId") {
    val simulation = new GraphColoringSimulation(seed = 11)
    val before = simulation.snapshot()

    val after = simulation.burst(size = 10, bridges = 2)

    after.nodeCount shouldBe before.nodeCount + 10
    after.nodes should have size (before.nodeCount + 10)

    val newNodes = after.nodes.filter(n => !before.nodes.exists(_.id == n.id))
    newNodes should have size 10
    newNodes.map(_.clusterId).distinct should contain only 1
    newNodes.foreach { node =>
      node.neighbors should not be empty
    }
    after.events.map(_.kind) should contain("burst")
    after.clusters.map(_.id) should contain(0)
    after.clusters.map(_.id) should contain(1)
  }

  test("burst creates bridge edges between the new cluster and the existing graph") {
    val simulation = new GraphColoringSimulation(seed = 11)
    simulation.configure(nodeCount = 20, paletteSize = 4)
    val after = simulation.burst(size = 8, bridges = 2)

    val byId = after.nodes.map(node => node.id -> node).toMap
    val bridgeEdges = after.nodes.flatMap { node =>
      node.neighbors.collect {
        case other if node.id < other && byId.get(other).exists(_.clusterId != node.clusterId) =>
          (node.id, other)
      }
    }
    bridgeEdges should not be empty
    bridgeEdges.size should be >= 2
  }

  test("burst followed by step rounds colors the new cluster") {
    val simulation = new GraphColoringSimulation(seed = 11)
    var state = simulation.snapshot()
    var guard = 0
    while !state.completed && guard < 20 do
      state = simulation.stepRound()
      guard += 1
    state.completed shouldBe true

    simulation.burst(size = 6, bridges = 1)
    guard = 0
    state = simulation.snapshot()
    while !state.completed && guard < 40 do
      state = simulation.stepRound()
      guard += 1
    state.completed shouldBe true
    state.nodes.forall(_.color.nonEmpty) shouldBe true
  }

  test("burst respects node capacity cap") {
    val simulation = new GraphColoringSimulation(seed = 11)
    simulation.configure(nodeCount = GraphColoringSimulation.MaxNodes, paletteSize = 4)

    val after = simulation.burst(size = 20)

    after.nodeCount shouldBe GraphColoringSimulation.MaxNodes
  }
