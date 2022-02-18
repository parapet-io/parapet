package io.parapet.spark

import io.parapet.core.Dsl.DslF

class Dataframe[F[_]](rows: Seq[Row], schema: SparkSchema,
                      ctx: SparkContext[F]) extends io.parapet.core.Process[F] {

  import dsl._


  def map(f: Row => Row): DslF[F, Dataframe] = {
    ctx.mapDataframe(rows, schema, f)
  }

  def show: DslF[F, Unit] = dsl.eval {
    rows.foreach { row =>
      println(row.values.mkString(","))
    }
  }

  override def handle: Receive = {
    case _ => unit
  }
}
