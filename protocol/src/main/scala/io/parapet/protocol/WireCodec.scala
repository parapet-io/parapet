package io.parapet.protocol

import java.nio.charset.StandardCharsets

/** Type class describing how to serialize values of `A` to and from raw bytes for over-the-wire transport.
  *
  * Provided here as a thin abstraction so transport code can be polymorphic over the payload type. The companion ships
  * defaults for `Array[Byte]` (passthrough with defensive clone) and `String` (UTF-8 round trip).
  *
  * @tparam A
  *   the application-level value type.
  */
trait WireCodec[A]:
  /** Encodes `value` to a byte buffer suitable for transport. */
  def encode(value: A): Array[Byte]

  /** Decodes `bytes` back to a value, returning a human-readable error on the `Left`. */
  def decode(bytes: Array[Byte]): Either[String, A]

/** Built-in [[WireCodec]] instances. */
object WireCodec:
  /** Identity codec for raw bytes. Defensive clones on both sides protect against downstream mutation.
    */
  given byteArrayCodec: WireCodec[Array[Byte]] with
    def encode(value: Array[Byte]): Array[Byte] =
      value.clone()

    def decode(bytes: Array[Byte]): Either[String, Array[Byte]] =
      Right(bytes.clone())

  /** UTF-8 codec for [[String]]. */
  given utf8StringCodec: WireCodec[String] with
    def encode(value: String): Array[Byte] =
      value.getBytes(StandardCharsets.UTF_8)

    def decode(bytes: Array[Byte]): Either[String, String] =
      Right(String(bytes, StandardCharsets.UTF_8))
