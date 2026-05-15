package io.parapet.core

import io.parapet.{Envelope, Event, ProcessRef}

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}

/** In-memory record of every envelope delivered through the runtime. Activated when
  * [[Parapet.ParConfig.eventLogEnabled]] is `true`.
  *
  * Maintains a graph of per-process event nodes connected by directed [[EventLog.Impl.Edge]]s suitable for
  * visualization or replay.
  */
trait EventLog {

  /** Records the envelope (sender, event, receiver). */
  def add(envelope: Envelope): Unit

  /** Records the (source, event, target) tuple in long form. */
  def add(source: ProcessRef.Unknown, e: Event, target: ProcessRef.Unknown): Unit

  /** Marks the log as closed by appending a terminal node per process. */
  def close(): Unit
}

/** Default in-memory [[EventLog]] implementation and a no-op [[Mock]]. */
// todo it should be Execution trace
object EventLog {

  /** Returns a fresh in-memory log. */
  def apply(): EventLog = new Impl

  /** Default thread-safe in-memory implementation backed by `ConcurrentHashMap` and a `CopyOnWriteArrayList` of edges.
    */
  class Impl extends EventLog {

    import Impl._

    private val graph = new ConcurrentHashMap[ProcessRef.Unknown, List[Node]]()
    private val edges = new CopyOnWriteArrayList[Edge]()

    def add(envelope: Envelope): Unit =
      add(envelope.sender, envelope.event, envelope.receiver)

    def add(source: ProcessRef.Unknown, e: Event, target: ProcessRef.Unknown): Unit = {
      val sourceNode = EventNode(e, UUID.randomUUID().toString, in = false)
      val targetNode = EventNode(e, UUID.randomUUID().toString, e.toString, in = true)
      addNode(source, sourceNode)
      addNode(target, targetNode)
      edges.add(Edge(sourceNode.id, targetNode.id))
    }

    def incoming(ref: ProcessRef.Unknown): List[Event] = {
      val t = graph.getOrDefault(ref, List.empty[Node])
      t.collect {
        case n: EventNode if n.in => n.event
      }
    }

    private def addNode(ref: ProcessRef.Unknown, n: Node): Unit =
      graph.compute(ref, (_, v) => Option(v).getOrElse(List(PNode(ref.value, ref.value, start = true))) :+ n)

    override def close(): Unit = graph.keySet().forEach(close)

    private def close(ref: ProcessRef.Unknown): Unit =
      graph.computeIfPresent(ref, (key, v) => v :+ PNode(key.toString + "-end", "", start = false))
  }

  /** Node and edge ADT used by the in-memory graph. */
  object Impl {

    /** Common parent for every recorded node. */
    sealed class Node(val id: String, val name: String)

    /** A single event observation; `in` distinguishes incoming vs outgoing. */
    case class EventNode(event: Event, override val id: String, override val name: String = "", in: Boolean)
        extends Node(id, name)

    /** A process boundary marker (start/end). */
    case class PNode(override val id: String, override val name: String = "", start: Boolean) extends Node(id, name)

    /** Directed edge connecting two [[Node]]s by id. */
    case class Edge(source: String, target: String)
  }

  /** A no-op log; used by tests and for production deployments where logging is off. */
  val Mock: EventLog = new EventLog {
    override def add(envelope: Envelope): Unit = ()

    override def add(source: ProcessRef.Unknown, e: Event, target: ProcessRef.Unknown): Unit = ()

    override def close(): Unit = ()
  }
}
