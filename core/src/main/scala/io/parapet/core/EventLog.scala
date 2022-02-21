package io.parapet.core

import io.parapet.core.EventLog._
import io.parapet.{Envelope, Event, ProcessRef}

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

// todo it should be Execution trace
class EventLog {

  private val graph = new ConcurrentHashMap[ProcessRef, List[Node]]()
  private val edges = new CopyOnWriteArrayList[Edge]()

  def add(envelope: Envelope): Unit = {
    add(envelope.sender, envelope.event, envelope.receiver)
  }

  def add(source: ProcessRef, e: Event, target: ProcessRef): Unit = {
    val sourceNode = EventNode(e, UUID.randomUUID().toString, in = false)
    val targetNode = EventNode(e, UUID.randomUUID().toString, e.toString, in = true)
    addNode(source, sourceNode)
    addNode(target, targetNode)
    edges.add(Edge(sourceNode.id, targetNode.id))
  }

  def incoming(ref: ProcessRef): List[Event] = {
    val t  = graph.getOrDefault(ref, List.empty[Node])
    t.collect {
      case n: EventNode if n.in => n.event
    }
  }

  private def addNode(ref: ProcessRef, n: Node): Unit =
    graph.compute(ref, (_, v) => Option(v).getOrElse(List(PNode(ref.value, ref.value, start = true))) :+ n)

  def close(): Unit = graph.keySet().forEach(close)

  private def close(ref: ProcessRef): Unit = {
    graph.computeIfPresent(ref, (key, v) => v :+ PNode(key.toString + "-end", "", start = false))
  }

}

object EventLog {

  sealed class Node(val id: String, val name: String)

  case class EventNode(event: Event, override val id: String, override val name: String = "", in: Boolean) extends Node(id, name)

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
            case EventNode(_, id, name, false) =>
              Json.obj(
                "data" -> Json.obj("id" -> id, "label" -> name, "type" -> "star", "parent" -> parent),
                "position" -> Json.obj("x" -> x, "y" -> y),
              )
            case EventNode(_, id, name, true) =>
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
      t.edges.asScala.foreach { edge =>
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
