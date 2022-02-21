package io.parapet.spark

import com.github.freva.asciitable.AsciiTable
import io.parapet.core.Dsl.DslF

class Dataframe[F[_]](rows: Seq[Row], schema: SparkSchema,
                      ctx: SparkContext[F]) extends io.parapet.core.Process[F] {

  import dsl._

  def map(f: Row => Row): DslF[F, Dataframe[F]] = {
    ctx.mapDataframe(rows, schema, f)
  }

  def sortBy[B](f: Row => B)(implicit ord: Ordering[B]): DslF[F, Dataframe[F]] = {
    ctx.createDataframe(rows.sortBy(f), schema)
  }

  def show: DslF[F, Unit] = dsl.eval {
    val headers = schema.fields.map(_.name).toArray
    val data =
      rows.map(_.values.map(_.asInstanceOf[AnyRef]).toArray).toArray
    println(AsciiTable.getTable(headers, data))
  }

  override def handle: Receive = {
    case _ => unit
  }
}
