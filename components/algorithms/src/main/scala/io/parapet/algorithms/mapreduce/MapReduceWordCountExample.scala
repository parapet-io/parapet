package io.parapet.algorithms.mapreduce

import cats.effect.IO
import io.parapet.core.Event.Start
import io.parapet.core.Process
import io.parapet.algorithms.mapreduce.MapReduce._
import io.parapet.{CatsApp, core}

object MapReduceWordCountExample extends CatsApp {

  import dsl._

  // number of mapper workers
  val nMapperWorkers = 2
  // number of reducerWorkers
  val nReducerWorkers = 2

  // source data
  val data = Seq(
    "Hello World Bye World",
    "Hello Map Reduce Goodbye Map Reduce"
  )

  // Mapper function
  val mapper: Record[Unit, String] => Seq[Record[String, Int]] = record => {
    record.value.split(" ").map(word => Record(word.trim, 1))
  }

  // Reducer function
  val reducer: (String, Seq[Int]) => Int = (_, values) => values.sum

  override def processes: IO[Seq[core.Process[IO]]] = IO {
    // MapReduce process
    val mapreduce = new MapReduce[IO, Unit, String, String, Int](mapper, nMapperWorkers, reducer, nReducerWorkers)

    // client process
    val client = Process[IO](ref => {
      case Start =>
        // split the source data in chunks
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

  // Output:
  //  Result:
  //  key: Bye, value: 1
  //  key: Goodbye, value: 1
  //  key: Hello, value: 2
  //  key: Map, value: 2
  //  key: Reduce, value: 2
  //  key: World, value: 2

}
