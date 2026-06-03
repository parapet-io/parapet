package io.parapet.net.transport

/** A correlated transport message used by request/reply-style transports.
  *
  * `correlationId` identifies one logical request/response exchange. It is distinct from socket/router identity.
  * `payload` is one application-level payload, encoded by higher-level protocol code when structure is needed.
  *
  * Mutability note: `Array[Byte]` is mutable by the JVM. By convention, `payload` is treated as effectively immutable
  * once the `Message` leaves the transport layer.
  */
final case class Message(correlationId: String, payload: Array[Byte])
