package io.parapet.spark

import io.parapet.spark.SparkType._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

object Codec {

  trait Encoder[-A] {
    def encode(a: A): Array[Byte]
  }

  trait Decoder[+T] {
    def decode(data: Array[Byte]): T
  }


  def encode[T](a: T)(implicit encoder: Encoder[T]): Unit = {
    encoder.encode(a)
  }

  def decode[T](data: Array[Byte])(implicit decoder: Decoder[T]): T = {
    decoder.decode(data)
  }

  def encode(row: Row, sparkSchema: SparkSchema): Array[Byte] = {
    val buf = ByteBuffer.allocate(1000) // some random value, todo precalculate size
    for ((f, i) <- sparkSchema.fields.view.zipWithIndex) {
      f.t match {
        case StringType =>
          val strBytes = row.getAs[String](i).getBytes()
          buf.putInt(strBytes.length).put(strBytes)
        case IntType => buf.putInt(row.getAs[Int](i))
      }
    }
    buf.flip()
    val res = new Array[Byte](buf.remaining())
    buf.get(res)
    res
  }

  def decodeRow(data: Array[Byte], sparkSchema: SparkSchema): Row = {
    decodeRow(ByteBuffer.wrap(data), sparkSchema)
  }

  def decodeRow(buf: ByteBuffer, sparkSchema: SparkSchema): Row = {
    val cols = new Array[Any](sparkSchema.fields.size)
    for ((f, i) <- sparkSchema.fields.view.zipWithIndex) {
      f.t match {
        case StringType =>
          val strLen = buf.getInt
          val strBytes = new Array[Byte](strLen)
          buf.get(strBytes)
          cols(i) = new String(strBytes)
        case IntType =>
          cols(i) = buf.getInt()
        case _ => throw new UnsupportedOperationException(s"unsupported type: ${f.t}")
      }
    }
    Row(cols.toVector)
  }


  def encodeSchema(schema: SparkSchema): Array[Byte] = {
    AvroUtils.encode(schema)
  }

  def decodeSchema(bytes: Array[Byte]): SparkSchema = {
    AvroUtils.decode[SparkSchema](bytes)
  }

  // format:
  // 4 bytes schema length
  // n bytes schema
  // 4 bytes number of rows
  // n rows
  def encodeDataframe(rows: Seq[Row], schema: SparkSchema): Array[Byte] = {
    val buf = ByteBuffer.allocate(1000) // random value
    val schemaBytes = encodeSchema(schema)
    buf.putInt(schemaBytes.length)
    buf.put(schemaBytes)
    buf.putInt(rows.size)
    encodeRows(rows, schema, buf)
    buf.flip()
    val res = new Array[Byte](buf.remaining())
    buf.get(res)
    res
  }

  // number of rows should be put into buffer before calling the method
  def encodeRows(rows: Seq[Row], schema: SparkSchema, buf: ByteBuffer): Unit = {
    rows.foreach { row =>
      buf.put(encode(row, schema))
    }
  }

  def decodeDataframe(data: Array[Byte]): (SparkSchema, Array[Row]) = {
    decodeDataframe(ByteBuffer.wrap(data))
  }

  def decodeDataframe(buf: ByteBuffer): (SparkSchema, Array[Row]) = {
    val schemaSize = buf.getInt
    val schemaBytes = new Array[Byte](schemaSize)
    buf.get(schemaBytes)
    val schema = decodeSchema(schemaBytes)
    (schema, decodeRows(buf, buf.getInt, schema))
  }

  def decodeRows(buf: ByteBuffer, n: Int, schema: SparkSchema): Array[Row] = {
    val rows = new Array[Row](n)
    for (i <- rows.indices) {
      rows(i) = decodeRow(buf, schema)
    }
    rows
  }

  def encodeObj(obj: Any): Array[Byte] = {
    val bios = new ByteArrayOutputStream()
    val objectOutputStream = new ObjectOutputStream(bios)
    objectOutputStream.writeObject(obj)
    bios.toByteArray
  }

  def decodeObj[A](bytes: Array[Byte]): A = {
    val bios = new ByteArrayInputStream(bytes)
    val objectInputStream = new ObjectInputStream(bios)
    objectInputStream.readObject().asInstanceOf[A]
  }


  def toByteArray(buf: ByteBuffer): Array[Byte] = {
    buf.flip()
    val res = new Array[Byte](buf.remaining())
    buf.get(res)
    res
  }
}
