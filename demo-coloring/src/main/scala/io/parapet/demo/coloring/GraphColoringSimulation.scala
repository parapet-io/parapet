package io.parapet.demo.coloring

import scala.collection.mutable
import scala.util.Random

final class GraphColoringSimulation(
    graphId: String = "sample-12",
    paletteSize: Int = 4,
    nodeCount: Int = 12,
    seed: Int = 7
):
  private final case class GraphTemplate(
      graphId: String,
      nodeCount: Int,
      paletteSize: Int,
      canvasWidth: Int,
      canvasHeight: Int,
      nodes: Vector[ColoringNodeState],
      cluster: Vector[ClusterNodeState]
  )

  private var proposalRandom = new Random(seed)
  private val nodesById = mutable.LinkedHashMap.empty[String, ColoringNodeState]
  private var template = emptyTemplate
  private var round = 0
  private var tick = 0L
  private var running = false
  private var events = Vector.empty[DemoEvent]
  private var cluster = Vector.empty[ClusterNodeState]
  private var clusterCounter = 0

  configureGraph(nodeCount, paletteSize, graphId, initialization = true)

  def snapshot(): DemoState = synchronized {
    val clusterSizes = nodesById.values
      .groupBy(_.clusterId)
      .view
      .map { case (id, ns) => ClusterSummary(id, ns.size) }
      .toVector
      .sortBy(_.id)
    DemoState(
      graphId = template.graphId,
      round = round,
      tick = tick,
      nodeCount = nodesById.size,
      paletteSize = template.paletteSize,
      canvasWidth = template.canvasWidth,
      canvasHeight = template.canvasHeight,
      running = running,
      completed = nodesById.nonEmpty && nodesById.values.forall(_.status == ColoringNodeStatus.Locked),
      nodes = nodesById.values.toVector,
      cluster = cluster,
      events = events.takeRight(36),
      clusters = clusterSizes
    )
  }

  def setRunning(value: Boolean): DemoState = synchronized {
    running = value
    pushEvent("control", None, if value then "simulation started" else "simulation paused")
    snapshot()
  }

  def configure(nodeCount: Int, paletteSize: Int): DemoState = synchronized {
    val sanitizedNodes = sanitizeNodeCount(nodeCount)
    val sanitizedPalette = sanitizePaletteSize(paletteSize)
    configureGraph(sanitizedNodes, sanitizedPalette, s"generated-$sanitizedNodes-$sanitizedPalette", initialization = false)
    snapshot()
  }

  def reset(): DemoState = synchronized {
    resetState("simulation reset")
    snapshot()
  }

  def burst(size: Int, bridges: Int = 1): DemoState = synchronized {
    val requested = size.max(2).min(60)
    val room = (GraphColoringSimulation.MaxNodes - nodesById.size).max(0)
    val amount = requested.min(room)
    if amount <= 0 then
      pushEvent("burst", None, "capacity reached, skipping burst")
    else
      clusterCounter = clusterCounter + 1
      val clusterId = clusterCounter
      val random = new Random(seed + tick.toInt * 131 + amount * 29 + nodesById.size * 17 + clusterId * 997)

      val existingIds = nodesById.keys.toVector
      val bridgeCount = bridges.max(1).min(existingIds.size.max(0)).min(amount)
      val anchors: Vector[String] =
        if existingIds.isEmpty then Vector.empty
        else
          val picks = mutable.LinkedHashSet.empty[String]
          while picks.size < bridgeCount do
            picks += existingIds(random.nextInt(existingIds.size))
          picks.toVector

      val anchorNode = anchors.headOption.flatMap(nodesById.get)
      val centerX = anchorNode.map(_.x).getOrElse(template.canvasWidth.toDouble / 2.0)
      val centerY = anchorNode.map(_.y).getOrElse(template.canvasHeight.toDouble / 2.0)
      val spread = 40.0 + math.sqrt(amount.toDouble) * 18.0

      val nextIndex = maxNodeIndex() + 1
      val newIds = (0 until amount).toVector.map(i => s"n${nextIndex + i}")

      val positions = newIds.map { id =>
        val angle = random.nextDouble() * 2.0 * math.Pi
        val radius = random.nextDouble() * spread + 25.0
        (id, centerX + math.cos(angle) * radius, centerY + math.sin(angle) * radius)
      }

      val neighborMap = mutable.Map.empty[String, mutable.Set[String]]
      newIds.foreach(id => neighborMap(id) = mutable.Set.empty[String])

      def connect(a: String, b: String): Unit =
        if a != b then
          neighborMap.getOrElseUpdate(a, mutable.Set.empty) += b
          neighborMap.getOrElseUpdate(b, mutable.Set.empty) += a

      // Random spanning tree over the new nodes so the cluster is connected.
      (1 until amount).foreach { i =>
        val parent = random.nextInt(i)
        connect(newIds(i), newIds(parent))
      }

      // Add internal extra edges to give the cluster a bit of density (capped degree).
      val degreeCap = math.min(math.max(3, template.paletteSize - 1), 6)
      val extraEdges = math.max(amount / 2, amount / 3 + 1)
      var attempts = 0
      var added = 0
      while added < extraEdges && attempts < extraEdges * 6 do
        attempts += 1
        val a = newIds(random.nextInt(amount))
        val b = newIds(random.nextInt(amount))
        if a != b && !neighborMap(a).contains(b)
          && neighborMap(a).size < degreeCap && neighborMap(b).size < degreeCap
        then
          connect(a, b)
          added += 1

      // Bridge edges back to the existing graph via the anchors.
      val bridgeEdges = mutable.Set.empty[(String, String)]
      anchors.foreach { anchorId =>
        val attach = newIds(random.nextInt(amount))
        neighborMap.getOrElseUpdate(attach, mutable.Set.empty) += anchorId
        val ordered = if anchorId < attach then (anchorId, attach) else (attach, anchorId)
        bridgeEdges += ordered
      }

      val newStates = positions.map { case (id, x, y) =>
        ColoringNodeState(
          id = id,
          x = x,
          y = y,
          neighbors = neighborMap(id).toVector.sorted,
          clusterId = clusterId
        )
      }
      newStates.foreach(n => nodesById.put(n.id, n))

      anchors.foreach { anchorId =>
        nodesById.get(anchorId).foreach { anchor =>
          val newLinks = bridgeEdges.collect {
            case (a, b) if a == anchorId => b
            case (a, b) if b == anchorId => a
          }
          val merged = anchor.copy(
            neighbors = (anchor.neighbors ++ newLinks).distinct.sorted,
            status = ColoringNodeStatus.Uncolored,
            color = None,
            proposedColor = None,
            conflict = true
          )
          nodesById.update(anchorId, merged)
        }
      }

      val anchorLabel =
        if anchors.isEmpty then ""
        else s" via ${anchors.mkString(", ")}"
      pushEvent(
        "burst",
        anchors.headOption,
        s"cluster #$clusterId joined ($amount processes$anchorLabel)"
      )
    snapshot()
  }

  def stepRound(): DemoState = synchronized {
    if nodesById.values.forall(_.status == ColoringNodeStatus.Locked) then
      running = false
      pushEvent("round", None, "all nodes are colored")
      return snapshot()

    round = round + 1
    electControlLeader()
    pushEvent("round", None, s"starting round $round")

    val proposals = mutable.Map.empty[String, Int]
    nodesById.keys.foreach { nodeId =>
      val node = nodesById(nodeId)
      if node.status != ColoringNodeStatus.Locked then
        val proposed = chooseColor(node)
        proposals.put(nodeId, proposed)
        nodesById.update(
          nodeId,
          node.copy(
            status = ColoringNodeStatus.Proposing,
            proposedColor = Some(proposed),
            conflict = false
          )
        )
        pushEvent("proposal", Some(nodeId), s"proposes color $proposed")
    }

    proposals.foreach { case (nodeId, proposed) =>
      val hasConflict = nodesById(nodeId).neighbors.exists { neighborId =>
        proposals.get(neighborId).contains(proposed) && losesTie(nodeId, neighborId)
      }

      if hasConflict then
        val node = nodesById(nodeId)
        nodesById.update(
          nodeId,
          node.copy(
            status = ColoringNodeStatus.Uncolored,
            proposedColor = None,
            conflict = true
          )
        )
        pushEvent("conflict", Some(nodeId), s"lost color $proposed to a neighbor")
      else
        val node = nodesById(nodeId)
        nodesById.update(
          nodeId,
          node.copy(
            status = ColoringNodeStatus.Locked,
            color = Some(proposed),
            proposedColor = None,
            conflict = false
          )
        )
        pushEvent("lock", Some(nodeId), s"locked color $proposed")
    }

    if nodesById.values.forall(_.status == ColoringNodeStatus.Locked) then
      running = false
      pushEvent("complete", None, s"graph colored in $round rounds")

    snapshot()
  }

  private def configureGraph(nodeCount: Int, paletteSize: Int, graphId: String, initialization: Boolean): Unit =
    val sanitizedNodes = sanitizeNodeCount(nodeCount)
    val sanitizedPalette = sanitizePaletteSize(paletteSize)
    template = generateTemplate(sanitizedNodes, sanitizedPalette, graphId)
    resetState(
      if initialization then "simulation ready"
      else s"configured ${template.nodeCount} processes and ${template.paletteSize} colors"
    )

  private def resetState(reason: String): Unit =
    nodesById.clear()
    template.nodes.foreach(node => nodesById.put(node.id, node))
    round = 0
    tick = 0L
    running = false
    events = Vector.empty
    cluster = template.cluster
    clusterCounter = 0
    proposalRandom = new Random(seed + template.nodeCount * 17 + template.paletteSize * 31)
    pushEvent("control", None, reason)

  private def generateTemplate(nodeCount: Int, paletteSize: Int, graphId: String): GraphTemplate =
    val graphRandom = new Random(seed + nodeCount * 97 + paletteSize * 53)
    val cols = math.max(2, math.ceil(math.sqrt(nodeCount.toDouble)).toInt)
    val rows = math.ceil(nodeCount.toDouble / cols.toDouble).toInt
    val canvasWidth = math.max(900, cols * 130 + 180)
    val canvasHeight = math.max(680, rows * 120 + 180)
    val xStep =
      if cols <= 1 then 0.0 else (canvasWidth - 180).toDouble / (cols - 1)
    val yStep =
      if rows <= 1 then 0.0 else (canvasHeight - 180).toDouble / (rows - 1)

    val buckets = shuffledBuckets(nodeCount, paletteSize, graphRandom)
    val positions = Vector.tabulate(nodeCount) { index =>
      val col = index % cols
      val row = index / cols
      val jitterX = boundedJitter(graphRandom, if nodeCount <= 24 then 28.0 else 14.0)
      val jitterY = boundedJitter(graphRandom, if nodeCount <= 24 then 22.0 else 12.0)
      val x = 90.0 + col * xStep + jitterX
      val y = 90.0 + row * yStep + jitterY
      (x, y)
    }

    val adjacency = Array.fill(nodeCount)(mutable.Set.empty[Int])

    def connect(left: Int, right: Int): Unit =
      if left != right then
        adjacency(left).add(right)
        adjacency(right).add(left)

    def distance(left: Int, right: Int): Double =
      val (x1, y1) = positions(left)
      val (x2, y2) = positions(right)
      math.hypot(x2 - x1, y2 - y1)

    (1 until nodeCount).foreach { index =>
      val candidates = (0 until index).filter(other => buckets(other) != buckets(index))
      val parent = candidates.minByOption(other => distance(index, other)).getOrElse(index - 1)
      connect(index, parent)
    }

    val nearestWindow = math.max(3, math.min(8, paletteSize + 2))
    val threshold = math.min(xStep.max(120.0), yStep.max(110.0)) * 1.7 + 45.0

    (0 until nodeCount).foreach { index =>
      val nearest = (0 until nodeCount)
        .filter(other => other != index && buckets(other) != buckets(index) && !adjacency(index).contains(other))
        .sortBy(other => distance(index, other))
        .take(nearestWindow)

      nearest.foreach { other =>
        val shouldConnect =
          distance(index, other) <= threshold || graphRandom.nextDouble() < 0.16
        if shouldConnect && adjacency(index).size < nearestWindow + 2 then
          connect(index, other)
      }
    }

    val nodes =
      Vector.tabulate(nodeCount) { index =>
        val (x, y) = positions(index)
        ColoringNodeState(
          id = s"n${index + 1}",
          x = x,
          y = y,
          neighbors = adjacency(index).toVector.sorted.map(other => s"n${other + 1}")
        )
      }

    val cluster =
      Vector.tabulate(nodeCount) { index =>
        ClusterNodeState(
          id = s"worker-${index + 1}",
          address = s"tcp://127.0.0.1:${9000 + index}",
          online = true,
          role = if index == 0 then "leader" else "follower",
          term = 1L
        )
      }

    GraphTemplate(graphId, nodeCount, paletteSize, canvasWidth, canvasHeight, nodes, cluster)

  private def chooseColor(node: ColoringNodeState): Int =
    val neighborColors = node.neighbors.flatMap(neighborId => nodesById.get(neighborId).flatMap(_.color)).toSet
    val available = (0 until template.paletteSize).filterNot(neighborColors.contains)
    if available.nonEmpty then available(proposalRandom.nextInt(available.size))
    else proposalRandom.nextInt(template.paletteSize)

  private def losesTie(nodeId: String, neighborId: String): Boolean =
    nodeId > neighborId

  private def electControlLeader(): Unit =
    val onlineIds = cluster.filter(_.online).map(_.id)
    val leaderId = if onlineIds.isEmpty then "" else onlineIds.min
    val nextTerm = cluster.map(_.term).maxOption.getOrElse(0L) + 1L
    cluster = cluster.map { node =>
      if node.id == leaderId then node.copy(role = "leader", term = nextTerm)
      else node.copy(role = "follower", term = nextTerm)
    }
    if leaderId.nonEmpty then
      pushEvent("raft", Some(leaderId), s"control leader for round $round is $leaderId in term $nextTerm")

  private def pushEvent(kind: String, nodeId: Option[String], detail: String): Unit =
    tick = tick + 1
    events = (events :+ DemoEvent(tick = tick, kind = kind, nodeId = nodeId, detail = detail))
      .takeRight(GraphColoringSimulation.EventHistoryLimit)

  private def maxNodeIndex(): Int =
    nodesById.keys.flatMap(id => id.stripPrefix("n").toIntOption).maxOption.getOrElse(0)

  private def shuffledBuckets(nodeCount: Int, paletteSize: Int, random: Random): Vector[Int] =
    val values = Array.tabulate(nodeCount)(index => index % paletteSize)
    var i = values.length - 1
    while i > 0 do
      val j = random.nextInt(i + 1)
      val tmp = values(i)
      values(i) = values(j)
      values(j) = tmp
      i = i - 1
    values.toVector

  private def boundedJitter(random: Random, magnitude: Double): Double =
    (random.nextDouble() - 0.5) * 2.0 * magnitude

  private def sanitizeNodeCount(value: Int): Int =
    value.max(4).min(GraphColoringSimulation.MaxNodes)

  private def sanitizePaletteSize(value: Int): Int =
    value.max(2).min(12)

  private def emptyTemplate: GraphTemplate =
    GraphTemplate("empty", 0, 0, 0, 0, Vector.empty, Vector.empty)

object GraphColoringSimulation:
  val MaxNodes: Int = 2000
  val EventHistoryLimit: Int = 512
