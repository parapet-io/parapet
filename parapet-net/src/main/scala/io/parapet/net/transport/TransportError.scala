package io.parapet.net.transport

/** Transport-level errors that callers may handle explicitly.
  */
sealed trait TransportError extends Product with Serializable

object TransportError:
  final case class TimedOut(operation: String)                       extends TransportError
  final case class Closed(operation: String)                         extends TransportError
  final case class SendFailed(operation: String, message: String)    extends TransportError
  final case class ReceiveFailed(operation: String, message: String) extends TransportError
  final case class UnknownRoute(routingId: RoutingId)                extends TransportError
  final case class ProtocolViolation(message: String)                extends TransportError
  final case class Unexpected(cause: Throwable)                      extends TransportError
