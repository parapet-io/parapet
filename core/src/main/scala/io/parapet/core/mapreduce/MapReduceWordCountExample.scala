package io.parapet.core.mapreduce

import cats.effect.IO
import io.parapet.core.Event.Start
import io.parapet.core.Process
import io.parapet.core.mapreduce.MapReduce._
import io.parapet.{CatsApp, core}

object MapReduceWordCountExample extends CatsApp {

  import dsl._

  // number of mapper workers
  val nMapperWorkers = 2
  // number of reducerWorkers
  val nReducerWorkers = 2

  val data = Seq(
    "Hello World Bye World",
    "Hello Map Reduce Goodbye Map Reduce"
  )

  val mapper: Record[Unit, String] => Seq[Record[String, Int]] = record => {
    record.value.split(" ").map(word => Record(word.trim, 1))
  }

  val reducer: (String, Seq[Int]) => Int = (_, values) => values.sum

  override def processes: IO[Seq[core.Process[IO]]] = IO {
    val mapreduce = new MapReduce[IO, Unit, String, String, Int](mapper, nMapperWorkers, reducer, nReducerWorkers)
    val client = Process[IO](ref => {
      case Start =>
        val input = Input(data.map(line => Chunk(Seq(Record[Unit, String]((), line)))))
        input ~> mapreduce
      case out: Output[String, Int] => eval {
        println("Result:")
        out.records.sorted.foreach { case Record(key, value) =>
          println(s"key: $key, value: $value")
        }
      }
    })

    Seq(client, mapreduce)
  }
}
