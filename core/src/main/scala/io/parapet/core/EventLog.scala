package io.parapet.core



import io.parapet.core.EventLog._
import io.parapet.{Envelope, Event, ProcessRef}

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ConcurrentHashMap => JMap}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class EventLog {

  private val graph: JMap[ProcessRef, ListBuffer[Node]] = new JMap()
  private val edges: AtomicReference[List[Edge]] = new AtomicReference(List.empty)

  def add(envelope: Envelope): Unit = {
    // todo envelope.id
    add(envelope.sender, envelope.event, envelope.receiver)
  }

  def add(source: ProcessRef, e: Event, target: ProcessRef): Unit = {
    val sourceNode = EventNode(UUID.randomUUID().toString, in = false)
    val targetNode = EventNode(UUID.randomUUID().toString, e.toString, in = true)
    addNode(source, sourceNode)
    addNode(target, targetNode)
    edges.updateAndGet(xs => xs :+ Edge(sourceNode.id, targetNode.id))
  }

  private def addNode(ref: ProcessRef, n: Node): Unit =
    graph.compute(
      ref,
      (_, v) => {
        var l = v
        if (l == null) {
          l = new ListBuffer[Node]()
          l += PNode(ref.value, ref.value, start = true)
        }
        l += n
      },
    )

  def close(): Unit =
    // append final nodes
    graph.entrySet().forEach(entry => entry.getValue += PNode(entry.getKey.toString + "-end", "", start = false))

}

object EventLog {

  sealed class Node(val id: String, val name: String)

  case class EventNode(override val id: String, override val name: String = "", in: Boolean) extends Node(id, name)

  case class PNode(override val id: String, override val name: String = "", start: Boolean) extends Node(id, name)

  case class Edge(source: String, target: String)

  object Cytoscape {

    import play.api.libs.json.{JsObject, Json}

    def toJson(t: EventLog): String = {
      var y = 100
      val xStep = 100
      val yStep = 30
      val data = ListBuffer.empty[JsObject]

      t.graph.asScala.foreach { case (p, nodes) =>
        var x = 100
        val parent = p.value + "-parent"
        data += Json.obj("data" -> Json.obj("id" -> parent))

        data ++= nodes.map { n =>
          val obj = n match {
            case PNode(id, name, true) =>
              Json.obj(
                "data" -> Json.obj("id" -> id, "label" -> name, "parent" -> parent),
                "position" -> Json.obj("x" -> x, "y" -> y),
              )
            case PNode(id, name, false) =>
              Json.obj(
                "data" -> Json.obj("id" -> id, "label" -> name, "type" -> "rectangle", "parent" -> parent),
                "position" -> Json.obj("x" -> x, "y" -> y),
              )
            case EventNode(id, name, false) =>
              Json.obj(
                "data" -> Json.obj("id" -> id, "label" -> name, "type" -> "star", "parent" -> parent),
                "position" -> Json.obj("x" -> x, "y" -> y),
              )
            case EventNode(id, name, true) =>
              Json.obj(
                "data" -> Json.obj("id" -> id, "label" -> name, "type" -> "diamond", "parent" -> parent),
                "position" -> Json.obj("x" -> x, "y" -> y),
              )
          }

          x = x + xStep
          obj
        }

        def group(in: List[Node], out: List[(Node, Node)]): List[(Node, Node)] =
          in match {
            case f :: s :: Nil => out :+ (f, s)
            case f :: s :: xs => group(s +: xs, out :+ (f, s))
            case Nil => out
          }

        val pairs = group(nodes.toList, List.empty)
        data ++= pairs.slice(0, pairs.size - 1).map { pair =>
          val (n0, n1) = pair
          Json.obj(
            "data" ->
              Json.obj(
                "id" -> UUID.randomUUID().toString,
                "source" -> n0.id,
                "target" -> n1.id,
              ),
          )
        }

        val (n0, n1) = pairs.last

        // final edge
        data += Json.obj(
          "data" ->
            Json.obj(
              "id" -> UUID.randomUUID().toString,
              "source" -> n0.id,
              "target" -> n1.id,
              "arrow" -> "tee",
            ),
        )

        y = y + yStep

      }
      var arr = Json.arr()
      data.foreach(v => arr = arr.append(v))
      t.edges.get().foreach { edge =>
        arr = arr.append(
          Json.obj(
            "data" ->
              Json.obj(
                "id" -> UUID.randomUUID().toString,
                "source" -> edge.source,
                "target" -> edge.target,
                "arrow" -> "triangle",
              ),
          ),
        )
      }
      Json.stringify(arr)
    }
  }

}
