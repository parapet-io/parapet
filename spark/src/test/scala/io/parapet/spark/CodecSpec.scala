package io.parapet.spark

import org.scalatest.funsuite.AnyFunSuite
import io.parapet.spark.SparkType._
import io.parapet.spark.Codec._
import org.scalatest.matchers.should.Matchers._

class CodecSpec extends AnyFunSuite {

  test("encode row") {
    val sparkSchema = SparkSchema(
      Seq(SchemaField("id", IntType),
        SchemaField("name", StringType)))
    val data = encodeDataframe(Seq(Row.of(1, "Jeff"),
      Row.of(2, "Ben")), sparkSchema)

    val (schema, rows) = decodeDataframe(data)

    schema shouldBe sparkSchema
    rows shouldBe Seq(Row.of(1, "Jeff"), Row.of(2, "Ben"))

  }

}
