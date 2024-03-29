package io.parapet.net

import org.zeromq.ZMQ.Socket

import scala.util.Try

/**
  * Basic class that represents a network node.
  *
  * @param id unique node id
  * @param _address physical node address
  * @param socket zmq socket
  */
class Node(val id: String,
           private var _address: String,
           private val socket: Socket,
           val protocol: String = "tcp") {

  // todo socket.setReceiveTimeOut
  def address: String = _address

  def send(data: Array[Byte]): Unit = socket.send(data)

  def receive(): Option[Array[Byte]] = Option(socket.recv())

  def reconnect(): Try[Unit] = Try {
    socket.disconnect(protocol + "://" + _address)
    socket.connect(protocol + "://" + _address)
  }

  def reconnect(newAddress: String): Try[Boolean] = {
    Try {
      if (_address != newAddress) {
        socket.disconnect(protocol + "://" + _address)
        socket.connect(protocol + "://" + newAddress)
        _address = newAddress
        true
      } else {
        false
      }
    }
  }

  def close(): Try[Unit] = Try(socket.close())

  override def toString: String = s"id=$id, address=$address"
}