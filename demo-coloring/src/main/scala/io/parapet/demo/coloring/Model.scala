package io.parapet.demo.coloring

enum ColoringNodeStatus derives CanEqual:
  case Uncolored
  case Proposing
  case Locked

final case class ColoringNodeState(
    id: String,
    x: Double,
    y: Double,
    neighbors: Vector[String],
    status: ColoringNodeStatus = ColoringNodeStatus.Uncolored,
    color: Option[Int] = None,
    proposedColor: Option[Int] = None,
    conflict: Boolean = false
)

final case class ClusterNodeState(
    id: String,
    address: String,
    online: Boolean,
    role: String,
    term: Long
)

final case class DemoEvent(
    tick: Long,
    kind: String,
    nodeId: Option[String],
    detail: String
)

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
    events: Vector[DemoEvent]
)
