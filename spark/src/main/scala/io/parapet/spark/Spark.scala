package io.parapet.spark

import io.parapet.ProcessRef

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

object Spark {


  //  sealed trait Task extends Api {
  //    val id: String
  //  }
  //  case class MapTask(id: String, data:Array[Row], op: MapOperation) extends Task

  sealed trait Operation extends Serializable

  class MapOperation(f: Row => Row) extends Function1[Row, Row] with Operation {
    override def apply(v1: Row): Row = f(v1)
  }

  def serializeOp(op: Operation): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(op)
    oos.flush()
    oos.close()
    bos.close()
    bos.toByteArray
  }

  def deserializeOp[T <: Operation](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject().asInstanceOf[T]
  }

  class SparkContext(val workersRef: Vector[ProcessRef]) {

  }

}
