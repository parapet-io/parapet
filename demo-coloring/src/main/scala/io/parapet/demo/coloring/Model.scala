package io.parapet.demo.coloring

/** Lifecycle of a node in the distributed graph-coloring simulation. */
enum ColoringNodeStatus derives CanEqual:
  /** No color proposed yet. */
  case Uncolored

  /** Has tentatively proposed a color and is waiting for neighbour acks. */
  case Proposing

  /** Successfully picked a color (or, in [[GameMode.Battle]], reached the conquest cap). */
  case Locked

/** Top-level mode for the demo. The simulation switches between modes on user request. */
enum GameMode derives CanEqual:
  /** Cooperative graph coloring - neighbours negotiate non-conflicting colors. */
  case Coloring

  /** Adversarial "alien race" battle - colors compete to conquer territory. */
  case Battle

/** Aggregate population of a single color (race) in [[GameMode.Battle]]. */
final case class RaceStat(color: Int, size: Int)

/** Per-node state surfaced over the JSON API to the web UI.
  *
  * @param x,y
  *   initial layout hints; the front-end uses these to seed the 3D force layout.
  * @param neighbors
  *   ids of adjacent nodes (graph topology).
  * @param status
  *   node lifecycle, see [[ColoringNodeStatus]].
  * @param color
  *   current color (when locked or proposing successfully).
  * @param proposedColor
  *   color tentatively proposed during a coloring round.
  * @param conflict
  *   whether the most recent proposal collided with a neighbour.
  * @param clusterId
  *   cluster the node belongs to (clusters spawn over time).
  * @param conquests
  *   number of times the node has been wounded/flipped in battle; reaching `MaxConquests` permanently locks it.
  */
final case class ColoringNodeState(
    id: String,
    x: Double,
    y: Double,
    neighbors: Vector[String],
    status: ColoringNodeStatus = ColoringNodeStatus.Uncolored,
    color: Option[Int] = None,
    proposedColor: Option[Int] = None,
    conflict: Boolean = false,
    clusterId: Int = 0,
    conquests: Int = 0
)

/** UI-friendly summary of a cluster (spatially-grouped sub-graph). */
final case class ClusterSummary(id: Int, size: Int)

/** Snapshot of a single node in the optional Raft cluster overlay. */
final case class ClusterNodeState(
    id: String,
    address: String,
    online: Boolean,
    role: String,
    term: Long
)

/** Event emitted by the simulation; consumed by the front-end event log.
  *
  * @param tick
  *   monotonically-increasing simulation tick.
  * @param kind
  *   event category (e.g. `"propose"`, `"conquest"`, `"siege"`).
  * @param nodeId
  *   optional subject node id.
  * @param detail
  *   human-readable description.
  */
final case class DemoEvent(
    tick: Long,
    kind: String,
    nodeId: Option[String],
    detail: String
)

/** Complete simulation state; the JSON serialisation of this object drives the UI.
  *
  * @param graphId
  *   identifier of the current graph generation; bumped when the user resets/regenerates so the front-end can reset its
  *   caches.
  * @param round
  *   coloring/battle round counter.
  * @param tick
  *   monotonic event tick (separate from `round`).
  * @param nodeCount
  *   total nodes currently in the graph.
  * @param paletteSize
  *   number of available colors.
  * @param canvasWidth
  *   layout viewport width in pixels.
  * @param canvasHeight
  *   layout viewport height in pixels.
  * @param running
  *   whether the background simulation loop is active.
  * @param completed
  *   whether the current mode has terminated.
  * @param nodes
  *   per-node state.
  * @param cluster
  *   Raft cluster overlay (may be empty).
  * @param events
  *   recent simulation events (capped to a sliding window).
  * @param clusters
  *   summary metadata about node clusters.
  * @param mode
  *   active game mode.
  * @param races
  *   per-color population stats in [[GameMode.Battle]].
  * @param victor
  *   winning color when a battle has terminated.
  */
final case class DemoState(
    graphId: String,
    round: Int,
    tick: Long,
    nodeCount: Int,
    paletteSize: Int,
    canvasWidth: Int,
    canvasHeight: Int,
    running: Boolean,
    completed: Boolean,
    nodes: Vector[ColoringNodeState],
    cluster: Vector[ClusterNodeState],
    events: Vector[DemoEvent],
    clusters: Vector[ClusterSummary] = Vector.empty,
    mode: GameMode = GameMode.Coloring,
    races: Vector[RaceStat] = Vector.empty,
    victor: Option[Int] = None
)
