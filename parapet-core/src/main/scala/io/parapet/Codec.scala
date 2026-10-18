package io.parapet

import scala.util.Try

/** A bidirectional encoding for values of `A`.
  *
  * A successful encoding must be decodable by the same codec into an equivalent value. Failures are returned as
  * [[scala.util.Failure]].
  */
trait Codec[A] {

  /** Encodes `value` into its binary representation. */
  def encode(value: A): Try[Array[Byte]]

  /** Decodes a value from bytes produced by this codec. */
  def decode(bytes: Array[Byte]): Try[A]

}
