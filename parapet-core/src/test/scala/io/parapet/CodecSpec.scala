package io.parapet

import io.parapet.journal.effect.FailureData
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.ByteBuffer
import scala.compiletime.testing.typeCheckErrors

class CodecSpec extends AnyFunSuite:
  import CodecSpec.*

  private def roundTrip[A](value: A)(using codec: Codec[A]): A =
    codec.decode(codec.encode(value).get).get

  private inline def typeErrors(inline code: String): String =
    typeCheckErrors(code).map(_.message).mkString("\n")

  test("options and lists round-trip, empty and non-empty") {
    roundTrip(Option.empty[String]) shouldBe None
    roundTrip(Option("a")) shouldBe Some("a")
    roundTrip(List.empty[Int]) shouldBe Nil
    roundTrip(List(1, 2, 3)) shouldBe List(1, 2, 3)
  }

  test("a stack frame keeps every field, including absent ones") {
    val full    = new StackTraceElement("app", "my.module", "1.0", "a.B", "run", "B.scala", 42)
    val minimal = new StackTraceElement(null, null, null, "a.B", "run", null, -2)

    roundTrip(full) shouldBe full
    roundTrip(full).getModuleVersion shouldBe "1.0"
    roundTrip(minimal) shouldBe minimal
    roundTrip(minimal).getFileName shouldBe null
  }

  test("failure data with a chain of causes round-trips") {
    val trace = new RuntimeException().getStackTrace.toList
    val data  = FailureData(
      "java.io.IOException",
      Some("disk full"),
      trace,
      Some(FailureData("java.lang.IllegalStateException", None, trace.take(1), None))
    )

    roundTrip(data) shouldBe data
  }

  test("an enum value keeps the fields of its case") {
    roundTrip[Shape](Shape.Point) shouldBe Shape.Point
    roundTrip[Shape](Shape.Circle(7)) shouldBe Shape.Circle(7)
    roundTrip[Shape](Shape.Square(2)) shouldBe Shape.Square(2)
  }

  test("a sealed trait value keeps its case, including a case object and a case that refers to the trait") {
    val expr: Expr = Expr.Add(Expr.Num(1), Expr.Add(Expr.Zero, Expr.Num(2)))
    roundTrip(expr) shouldBe expr
  }

  test("a value of an enum or a sealed trait is stored as the ordinal of its case followed by its fields") {
    def ints(values: Int*): Array[Byte] =
      values.foldLeft(ByteBuffer.allocate(values.size * Integer.BYTES))(_.putInt(_)).array()

    summon[Codec[Shape]].encode(Shape.Circle(7)).get shouldBe ints(1, 7)
    summon[Codec[Expr]].encode(Expr.Num(5)).get shouldBe ints(0, 5)
    summon[Codec[Expr]].encode(Expr.Zero).get shouldBe ints(2)
  }

  test("corrupt tags and sizes fail inside the Try") {
    def decodeError[A](bytes: Array[Byte])(using codec: Codec[A]): Throwable =
      codec.decode(ByteBuffer.wrap(bytes)).failed.get

    decodeError[Option[Int]](Array[Byte](7)) shouldBe a[DecodeException]
    decodeError[Shape](ByteBuffer.allocate(4).putInt(3).array()) shouldBe a[DecodeException]
    decodeError[Expr](ByteBuffer.allocate(4).putInt(-1).array()) shouldBe a[DecodeException]
    decodeError[List[Int]](ByteBuffer.allocate(4).putInt(-1).array()) shouldBe a[DecodeException]
    decodeError[List[Int]](Array[Byte](0, 0)) shouldBe a[java.nio.BufferUnderflowException]
  }

  test("derivation rejects a field type without a codec") {
    typeErrors("Codec.derived[CodecSpec.Wrapper]") should include("CodecSpec.Inner")
  }

object CodecSpec:
  enum Shape derives Codec:
    case Point
    case Circle(radius: Int)
    case Square(side: Int)

  sealed trait Expr derives Codec

  object Expr:
    final case class Num(value: Int)              extends Expr
    final case class Add(left: Expr, right: Expr) extends Expr
    case object Zero                              extends Expr

  final case class Inner(value: Int)
  final case class Wrapper(inner: Inner)
