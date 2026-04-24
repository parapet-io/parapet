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
    val before     = simulation.snapshot()

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

    val byId        = after.nodes.map(node => node.id -> node).toMap
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
    var state      = simulation.snapshot()
    var guard      = 0
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

  test("startBattle assigns every node a color and switches mode") {
    val simulation = new GraphColoringSimulation(seed = 11)
    simulation.configure(nodeCount = 16, paletteSize = 4)

    val battle = simulation.startBattle()

    battle.mode shouldBe GameMode.Battle
    battle.nodes.forall(_.color.nonEmpty) shouldBe true
    battle.races.map(_.size).sum shouldBe battle.nodeCount
  }

  test("step() dispatches to battle round when in battle mode") {
    val simulation = new GraphColoringSimulation(seed = 11)
    simulation.configure(nodeCount = 16, paletteSize = 4)
    val before = simulation.startBattle()

    val after = simulation.step()

    after.mode shouldBe GameMode.Battle
    after.round shouldBe (before.round + 1)
  }

  test("battle eventually completes with a declared victor") {
    val simulation = new GraphColoringSimulation(seed = 11)
    simulation.configure(nodeCount = 24, paletteSize = 3)
    simulation.startBattle()

    var state = simulation.snapshot()
    var guard = 0
    while !state.completed && guard < 400 do
      state = simulation.battleRound()
      guard += 1

    state.completed shouldBe true
    state.victor shouldBe defined
  }

  test("under-strength attackers fail to break a fortified node") {
    // Rule: leader_attackers must be strictly greater than same-color defenders to conquer.
    val simulation = new GraphColoringSimulation(seed = 42)
    simulation.configure(nodeCount = 16, paletteSize = 3)
    val state = simulation.startBattle()

    val afterRound = simulation.battleRound()
    state.nodes.foreach { victim =>
      val victimColor = victim.color.get
      val defenders   = victim.neighbors.count { nid =>
        state.nodes.find(_.id == nid).flatMap(_.color).contains(victimColor)
      }
      val attackerColors = victim.neighbors.flatMap { nid =>
        state.nodes.find(_.id == nid).flatMap(_.color).filterNot(_ == victimColor)
      }
      if attackerColors.nonEmpty then
        val leaderCount = attackerColors.groupBy(identity).values.map(_.size).max
        if leaderCount <= defenders then
          val after = afterRound.nodes.find(_.id == victim.id).get
          after.color shouldBe Some(victimColor)
    }
  }

  test("a failed siege still wounds the defender (guarantees progress)") {
    val simulation = new GraphColoringSimulation(seed = 17)
    simulation.configure(nodeCount = 24, paletteSize = 3)
    simulation.startBattle()

    val before = simulation.snapshot()
    val after  = simulation.battleRound()

    val beforeWounds = before.nodes.map(n => n.id -> n.conquests).toMap
    val afterWounds  = after.nodes.map(n => n.id -> n.conquests).toMap
    val rose         = afterWounds.exists { case (id, w) => w > beforeWounds.getOrElse(id, 0) }
    rose shouldBe true
  }

  test("no node is ever wounded more than MaxConquests times") {
    val simulation = new GraphColoringSimulation(seed = 23)
    simulation.configure(nodeCount = 36, paletteSize = 4)
    simulation.startBattle()

    var state = simulation.snapshot()
    var guard = 0
    while !state.completed && guard < 400 do
      state = simulation.battleRound()
      guard += 1

    state.nodes.foreach { n =>
      n.conquests should be <= GraphColoringSimulation.MaxConquests
    }
  }

  test("battle always terminates within a bounded number of rounds") {
    Seq(7, 11, 13, 17, 23).foreach { s =>
      val simulation = new GraphColoringSimulation(seed = s)
      simulation.configure(nodeCount = 40, paletteSize = 4)
      simulation.startBattle()

      var state = simulation.snapshot()
      var guard = 0
      val cap   = 4 * state.nodeCount + GraphColoringSimulation.StalemateLimit + 20
      while !state.completed && guard < cap do
        state = simulation.battleRound()
        guard += 1
      state.completed shouldBe true
      state.victor shouldBe defined
    }
  }

  test("stopBattle restores coloring mode") {
    val simulation = new GraphColoringSimulation(seed = 11)
    simulation.startBattle()
    val stopped = simulation.stopBattle()
    stopped.mode shouldBe GameMode.Coloring
  }
