package io.parapet

import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.nio.{ByteBuffer, CharBuffer}
import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror
import scala.util.Try

trait Codec[A]:

  /** Encodes `value`. The caller may keep the returned array, as the journal does until the entry is written, so the
    * array must not change after `encode` returns.
    */
  def encode(value: A): Try[Array[Byte]]
  def decode(buff: ByteBuffer): Try[A]
  final def decode(bytes: Array[Byte]): Try[A] = Try {
    val buffer = ByteBuffer.wrap(bytes)
    val value  = decode(buffer).get
    if (buffer.hasRemaining) throw new DecodeException("unexpected bytes after encoded value")
    value
  }

/** Thrown by a decoder when its input is not a valid encoding of the value it decodes. */
final class DecodeException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

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
      throw new DecodeException("invalid byte-array length")
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
        throw new DecodeException("invalid UTF-8", error)
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

/** Each field is encoded once using its supplied codec, in declaration order. Field codecs own their wire
  * representation.
  */
final private[parapet] class RecordCodec[A](
    fields: () => Vector[Codec[?]],
    mirror: Mirror.ProductOf[A]
) extends Codec[A]:

  // Resolved on first use, so a field can refer to the codec being defined, as `FailureData.cause` does.
  private lazy val codecs = fields()

  def decode(buffer: ByteBuffer): Try[A] = Try {
    val values = codecs.map(_.decode(buffer).get)
    mirror.fromProduct(Tuple.fromArray(values.toArray))
  }
  def encode(value: A): Try[Array[Byte]] = Try {
    val product = value.asInstanceOf[Product]
    Codec.concat(codecs.zipWithIndex.map { (codec, index) =>
      codec.asInstanceOf[Codec[Any]].encode(product.productElement(index)).get
    })
  }

/** Encodes a value of a sealed trait or an enum as the ordinal of its case, then the fields of that case. Cases are
  * numbered in declaration order, so inserting or reordering cases changes the ordinals of values already stored.
  */
final private[parapet] class SumCodec[A](cases: Vector[Codec[?]], mirror: Mirror.SumOf[A]) extends Codec[A]:
  def encode(value: A): Try[Array[Byte]] = Try {
    val ordinal = mirror.ordinal(value)
    Codec.concat(Vector(IntCodec.encode(ordinal).get, cases(ordinal).asInstanceOf[Codec[A]].encode(value).get))
  }

  def decode(buff: ByteBuffer): Try[A] = Try {
    val ordinal = buff.getInt()
    if ordinal < 0 || ordinal >= cases.size then throw new DecodeException(s"invalid case ordinal $ordinal")
    cases(ordinal).decode(buff).get.asInstanceOf[A]
  }

/** Encodes the element count, then each element. */
final private[parapet] class ListCodec[A: Codec]() extends Codec[List[A]]:
  private val codec = summon[Codec[A]]

  def encode(list: List[A]): Try[Array[Byte]] = Try {
    Codec.concat(IntCodec.encode(list.size).get +: list.map(codec.encode(_).get))
  }

  def decode(buff: ByteBuffer): Try[List[A]] = Try {
    val size = buff.getInt()
    if size < 0 then throw new DecodeException(s"invalid list size $size")
    List.fill(size)(codec.decode(buff).get)
  }

/** Encodes `None` as tag 0, and `Some` as tag 1 followed by the value. */
final private[parapet] class OptionCodec[A: Codec] extends Codec[Option[A]]:
  private val codec         = summon[Codec[A]]
  private val NoneTag: Byte = 0x00
  private val SomeTag: Byte = 0x01

  def encode(value: Option[A]): Try[Array[Byte]] = Try {
    value match
      case None        => Array(NoneTag)
      case Some(value) => Codec.concat(Vector(Array(SomeTag), codec.encode(value).get))
  }

  def decode(buff: ByteBuffer): Try[Option[A]] = Try {
    buff.get() match
      case NoneTag => None
      case SomeTag => Some(codec.decode(buff).get)
      case tag     => throw new DecodeException(s"invalid option tag $tag")
  }

/** Encodes a stack frame field by field. The class loader, module, module version and file name may be absent. */
private[parapet] object StackTraceElementCodec extends Codec[StackTraceElement]:
  private val optional = OptionCodec[String]()

  def encode(frame: StackTraceElement): Try[Array[Byte]] = Try {
    Codec.concat(
      Vector(
        optional.encode(Option(frame.getClassLoaderName)).get,
        optional.encode(Option(frame.getModuleName)).get,
        optional.encode(Option(frame.getModuleVersion)).get,
        StringCodec.encode(frame.getClassName).get,
        StringCodec.encode(frame.getMethodName).get,
        optional.encode(Option(frame.getFileName)).get,
        IntCodec.encode(frame.getLineNumber).get
      )
    )
  }

  def decode(buff: ByteBuffer): Try[StackTraceElement] = Try {
    new StackTraceElement(
      optional.decode(buff).get.orNull,
      optional.decode(buff).get.orNull,
      optional.decode(buff).get.orNull,
      StringCodec.decode(buff).get,
      StringCodec.decode(buff).get,
      optional.decode(buff).get.orNull,
      IntCodec.decode(buff).get
    )
  }

object Codec:
  given Codec[Int]               = IntCodec
  given Codec[Long]              = LongCodec
  given Codec[String]            = StringCodec
  given Codec[Array[Byte]]       = BytesCodec
  given Codec[StackTraceElement] = StackTraceElementCodec

  given [A: Codec]: Codec[List[A]]   = ListCodec()
  given [A: Codec]: Codec[Option[A]] = OptionCodec()

  given Codec[ProcessRef[?]] = new Codec[ProcessRef[_]] {
    override def encode(value: ProcessRef[_]): Try[Array[Byte]] = StringCodec.encode(value.value)

    override def decode(buff: ByteBuffer): Try[ProcessRef[_]] = StringCodec.decode(buff).map(ProcessRef(_))
  }

  /** Derives a codec for a case class, a sealed trait or an enum.
    *
    * Fields are encoded in declaration order and cases by ordinal, so reordering fields or cases changes the format of
    * values already stored. Every case of a sealed trait or an enum must be a case class or a case object; its codec is
    * derived here. Every field type needs its own codec, a given or `derives Codec`; a recursive type refers to its own
    * given.
    */
  inline def derived[A](using m: Mirror.Of[A]): Codec[A] =
    inline m match
      case p: Mirror.ProductOf[A] => product(p)
      case s: Mirror.SumOf[A]     => new SumCodec[A](cases[s.MirroredElemTypes], s)

  /** Concatenates encoded parts in order. */
  private[parapet] def concat(parts: Seq[Array[Byte]]): Array[Byte] =
    val buffer = ByteBuffer.allocate(parts.foldLeft(0)((total, bytes) => Math.addExact(total, bytes.length)))
    parts.foreach(bytes => buffer.put(bytes))
    buffer.array()

  private inline def product[A](p: Mirror.ProductOf[A]): Codec[A] =
    new RecordCodec[A](() => fields[p.MirroredElemTypes], p)

  private inline def fields[T <: Tuple]: Vector[Codec[?]] =
    inline erasedValue[T] match
      case _: EmptyTuple     => Vector.empty
      case _: (head *: tail) => summonInline[Codec[head]] +: fields[tail]

  private inline def cases[T <: Tuple]: Vector[Codec[?]] =
    inline erasedValue[T] match
      case _: EmptyTuple     => Vector.empty
      case _: (head *: tail) => product[head](summonInline[Mirror.ProductOf[head]]) +: cases[tail]
