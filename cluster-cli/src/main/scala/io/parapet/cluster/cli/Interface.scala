package io.parapet.cluster.cli

import scala.util.Try

trait Interface {
  def connect():Unit
  // sends its unique id and network address
  def join(group: String): Try[Unit]
  // leaves the group
  def leave(group: String): Try[Unit]
  // send a message to the node
  def whisper(node: String, data: Array[Byte]): Try[Unit]
  // send a message to all nodes in the group
  def broadcast(group: String, data: Array[Byte]): Try[Unit]
  // get the current leader
  def leader: Option[String]
  // returns all registered nodes
  def getNodes: Seq[String]

  def close(): Unit
}
