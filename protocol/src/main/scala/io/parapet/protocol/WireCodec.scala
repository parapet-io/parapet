package io.parapet.protocol

import java.nio.charset.StandardCharsets

trait WireCodec[A]:
  def encode(value: A): Array[Byte]
  def decode(bytes: Array[Byte]): Either[String, A]

object WireCodec:
  given byteArrayCodec: WireCodec[Array[Byte]] with
    def encode(value: Array[Byte]): Array[Byte] =
      value.clone()

    def decode(bytes: Array[Byte]): Either[String, Array[Byte]] =
      Right(bytes.clone())

  given utf8StringCodec: WireCodec[String] with
    def encode(value: String): Array[Byte] =
      value.getBytes(StandardCharsets.UTF_8)

    def decode(bytes: Array[Byte]): Either[String, String] =
      Right(String(bytes, StandardCharsets.UTF_8))
