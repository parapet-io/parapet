package io.parapet.core

import io.parapet.{Envelope, Event, ProcessRef}

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}

trait EventLog {
  def add(envelope: Envelope): Unit

  def add(source: ProcessRef, e: Event, target: ProcessRef): Unit

  def close(): Unit
}

// todo it should be Execution trace
object EventLog {

  def apply(): EventLog = new Impl

  class Impl extends EventLog {

    import Impl._

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
      val t = graph.getOrDefault(ref, List.empty[Node])
      t.collect {
        case n: EventNode if n.in => n.event
      }
    }

    private def addNode(ref: ProcessRef, n: Node): Unit =
      graph.compute(ref, (_, v) => Option(v).getOrElse(List(PNode(ref.value, ref.value, start = true))) :+ n)

    override def close(): Unit = graph.keySet().forEach(close)

    private def close(ref: ProcessRef): Unit = {
      graph.computeIfPresent(ref, (key, v) => v :+ PNode(key.toString + "-end", "", start = false))
    }
  }

  object Impl {
    sealed class Node(val id: String, val name: String)

    case class EventNode(event: Event, override val id: String, override val name: String = "", in: Boolean) extends Node(id, name)

    case class PNode(override val id: String, override val name: String = "", start: Boolean) extends Node(id, name)

    case class Edge(source: String, target: String)
  }

  val Mock: EventLog = new EventLog {
    override def add(envelope: Envelope): Unit = ()

    override def add(source: ProcessRef, e: Event, target: ProcessRef): Unit = ()

    override def close(): Unit = ()
  }
}
