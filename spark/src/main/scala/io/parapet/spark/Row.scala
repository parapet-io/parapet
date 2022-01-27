package io.parapet.spark

case class Row(values: Vector[Any]) {
  def getAs[T](i: Int): T = values(i).asInstanceOf[T]
}

object Row {
  def of(data: Any*): Row = new Row(data.toVector)
}