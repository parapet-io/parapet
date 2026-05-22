package io.parapet.net.transport

sealed trait ReceiveResult[+A]

object ReceiveResult:
  final case class Received[A](value: A)         extends ReceiveResult[A]
  case object Idle                               extends ReceiveResult[Nothing]
  final case class Failed(error: TransportError) extends ReceiveResult[Nothing]
