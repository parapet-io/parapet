package io.parapet.spark

import cats.effect.IO
import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.spark.Spark.SparkContext

class Dataframe(rows: Seq[Row], schema: SparkSchema, ctx: SparkContext) extends WithDsl[IO] {
  def map(f: Row => Row): DslF[IO, Dataframe] =
    dsl.eval(new Dataframe(rows.map(f), schema, ctx))

  def show: DslF[IO, Unit] = dsl.eval {
    rows.foreach { row =>
      println(row.values.mkString(","))
    }
  }

}
