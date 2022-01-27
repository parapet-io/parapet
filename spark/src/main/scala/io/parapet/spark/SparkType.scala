package io.parapet.spark

sealed trait SparkType

object SparkType {
  case object StringType extends SparkType

  case object IntType extends SparkType
}
