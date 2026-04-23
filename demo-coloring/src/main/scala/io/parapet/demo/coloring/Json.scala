package io.parapet.demo.coloring

object Json:
  def render(state: DemoState): String =
    obj(
      "graphId" -> string(state.graphId),
      "round" -> num(state.round),
      "tick" -> num(state.tick),
      "nodeCount" -> num(state.nodeCount),
      "paletteSize" -> num(state.paletteSize),
      "canvasWidth" -> num(state.canvasWidth),
      "canvasHeight" -> num(state.canvasHeight),
      "running" -> bool(state.running),
      "completed" -> bool(state.completed),
      "nodes" -> arr(state.nodes.map(renderNode)),
      "cluster" -> arr(state.cluster.map(renderClusterNode)),
      "events" -> arr(state.events.map(renderEvent)),
      "clusters" -> arr(state.clusters.map(renderClusterSummary))
    )

  private def renderNode(node: ColoringNodeState): String =
    obj(
      "id" -> string(node.id),
      "x" -> num(node.x),
      "y" -> num(node.y),
      "neighbors" -> arr(node.neighbors.map(string)),
      "status" -> string(node.status.toString),
      "color" -> optNum(node.color),
      "proposedColor" -> optNum(node.proposedColor),
      "conflict" -> bool(node.conflict),
      "clusterId" -> num(node.clusterId)
    )

  private def renderClusterSummary(summary: ClusterSummary): String =
    obj(
      "id" -> num(summary.id),
      "size" -> num(summary.size)
    )

  private def renderClusterNode(node: ClusterNodeState): String =
    obj(
      "id" -> string(node.id),
      "address" -> string(node.address),
      "online" -> bool(node.online),
      "role" -> string(node.role),
      "term" -> num(node.term)
    )

  private def renderEvent(event: DemoEvent): String =
    obj(
      "tick" -> num(event.tick),
      "kind" -> string(event.kind),
      "nodeId" -> optString(event.nodeId),
      "detail" -> string(event.detail)
    )

  private def obj(fields: (String, String)*): String =
    fields.map { case (name, value) => s"${string(name)}:$value" }.mkString("{", ",", "}")

  private def arr(values: Seq[String]): String =
    values.mkString("[", ",", "]")

  private def string(value: String): String =
    s""""${escape(value)}""""

  private def optString(value: Option[String]): String =
    value.map(string).getOrElse("null")

  private def num(value: Int): String =
    value.toString

  private def num(value: Long): String =
    value.toString

  private def num(value: Double): String =
    f"$value%.2f"

  private def optNum(value: Option[Int]): String =
    value.map(_.toString).getOrElse("null")

  private def bool(value: Boolean): String =
    if value then "true" else "false"

  private def escape(value: String): String =
    value.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case c    => c.toString
    }
