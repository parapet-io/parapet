package io.parapet.cluster.cli

import scala.util.Try

trait Interface {

  /** Connects to the leader.
    */
  def connect(): Unit

  /** Joins a cluster.
    * @param group the node group
    * @return result
    */
  def join(group: String): Try[Unit]

  /** Leaves the given node group.
    * @param group the node group
    * @return result
    */
  def leave(group: String): Try[Unit]

  /** Sends a request.
    *
    * @param req the request
    * @return result
    */
  def send(req: Req): Try[Unit]

  /**
    * Sends a reply.
    *
    * @param rep the reply
    * @return result
    */
  def send(rep: Rep): Try[Unit]

  /** Sends a message to all nodes in the group.
    *
    * @param group the node group
    * @param data the data to send
    * @return result
    */
  def broadcast(group: String, data: Array[Byte]): Try[Unit]

  /** Gets current leader.
    *
    * @return leader
    */
  def leader: Option[String]

  /** Gets all registered nodes.
    * @return registered nodes
    */
  def getNodes: Try[Seq[String]]

  /** Disconnects from the cluster and closes open network connections.
    */
  def close(): Unit
}
