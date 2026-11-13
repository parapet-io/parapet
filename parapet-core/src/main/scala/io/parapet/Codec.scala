package io.parapet

import io.parapet.journal.log.*

import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.nio.{ByteBuffer, CharBuffer}
import scala.compiletime.{constValue, constValueTuple, erasedValue, summonInline}
import scala.deriving.Mirror
import scala.util.Try

trait Codec[A]:
  def encode(value: A): Try[Array[Byte]]
  def decode(buff: ByteBuffer): Try[A]
  final def decode(bytes: Array[Byte]): Try[A] = Try {
    val buffer = ByteBuffer.wrap(bytes)
    val value  = decode(buffer).get
    if (buffer.hasRemaining) throw new CorruptLogException("unexpected bytes after encoded value")
    value
  }

private[parapet] object IntCodec extends Codec[Int]:
  def encode(value: Int): Try[Array[Byte]] =
    Try(ByteBuffer.allocate(Integer.BYTES).putInt(value).array())
  def decode(buff: ByteBuffer): Try[Int] = Try(buff.getInt())

private[parapet] object LongCodec extends Codec[Long]:
  def encode(value: Long): Try[Array[Byte]] =
    Try(ByteBuffer.allocate(java.lang.Long.BYTES).putLong(value).array())
  def decode(buff: ByteBuffer): Try[Long] = Try(buff.getLong())

private[parapet] object BytesCodec extends Codec[Array[Byte]]:
  def decode(buff: ByteBuffer): Try[Array[Byte]] = Try {
    val length = buff.getInt()
    if (length < 0 || length > buff.remaining())
      throw new CorruptLogException("invalid byte-array length")
    val bytes = new Array[Byte](length)
    buff.get(bytes)
    bytes
  }
  def encode(value: Array[Byte]): Try[Array[Byte]] = Try {
    ByteBuffer
      .allocate(Math.addExact(Integer.BYTES, value.length))
      .putInt(value.length)
      .put(value)
      .array()
  }

private[parapet] object StringCodec extends Codec[String]:
  def decode(buff: ByteBuffer): Try[String] = Try {
    val bytes = BytesCodec.decode(buff).get
    try
      StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(bytes))
        .toString
    catch {
      case error: java.nio.charset.CharacterCodingException =>
        throw new CorruptLogException("invalid UTF-8", error)
    }
  }
  def encode(value: String): Try[Array[Byte]] = Try {
    val bytes = StandardCharsets.UTF_8
      .newEncoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
      .encode(CharBuffer.wrap(value))
    ByteBuffer
      .allocate(Math.addExact(Integer.BYTES, bytes.remaining()))
      .putInt(bytes.remaining())
      .put(bytes)
      .array()
  }

/** Each field is encoded once using its supplied codec. Field codecs own their wire representation. */
final private[parapet] class RecordCodec[A](
    fields: Vector[Codec[?]],
    mirror: Mirror.ProductOf[A]
) extends Codec[A]:
  def decode(buffer: ByteBuffer): Try[A] = Try {
    val values = fields.map(_.decode(buffer).get)
    mirror.fromProduct(Tuple.fromArray(values.toArray))
  }
  def encode(value: A): Try[Array[Byte]] = Try {
    val product = value.asInstanceOf[Product]
    val parts   = fields.zipWithIndex.map { (codec, index) =>
      codec.asInstanceOf[Codec[Any]].encode(product.productElement(index)).get
    }
    val size   = parts.foldLeft(0)((total, bytes) => Math.addExact(total, bytes.length))
    val buffer = ByteBuffer.allocate(size)
    parts.foreach(bytes => buffer.put(bytes))
    buffer.array()
  }

object Codec:
  given Codec[Int]           = IntCodec
  given Codec[Long]          = LongCodec
  given Codec[String]        = StringCodec
  given Codec[Array[Byte]]   = BytesCodec
  given Codec[ProcessRef[?]] = new Codec[ProcessRef[_]] {
    override def encode(value: ProcessRef[_]): Try[Array[Byte]] = StringCodec.encode(value.value)

    override def decode(buff: ByteBuffer): Try[ProcessRef[_]] = StringCodec.decode(buff).map(ProcessRef(_))
  }

  inline def derived[A](using m: Mirror.ProductOf[A]): Codec[A] = {
    val codecs = fields[m.MirroredElemTypes]
    val names  = constValueTuple[m.MirroredElemLabels].toList.asInstanceOf[List[String]]
    new RecordCodec[A](codecs.toVector, m)
  }

  private inline def fields[T <: Tuple]: List[Codec[?]] =
    inline erasedValue[T] match {
      case _: EmptyTuple     => Nil
      case _: (head *: tail) => summonInline[Codec[head]] :: fields[tail]
    }
