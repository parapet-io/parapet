package io.parapet.algorithms.mapreduce

import cats.effect.IO
import io.parapet.algorithms.mapreduce.MapReduce._
import io.parapet.core.Event.Start
import io.parapet.core.Process
import io.parapet.testutils.{BasicCatsIOSpec, EventStore}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.OptionValues._


class MapReduceSpec extends FunSuite with BasicCatsIOSpec {

  import dsl._

  test("map reduce") {

    val eventStore = new EventStore[IO, Output[String, Int]]()

    val mapper: Record[Unit, String] => Seq[Record[String, Int]] = record => {
      record.value.split(" ").map(word => Record(word.trim, 1))
    }

    val reducer: (String, Seq[Int]) => Int = (_, values) => values.sum

    val mapreduce = new MapReduce[IO, Unit, String, String, Int](mapper, 2, reducer, 1)

    val lines = Seq(
      "Hello World Bye World",
      "Hello Map Reduce Goodbye Map Reduce"
    )

    val input = Input(lines.map(line => Chunk(Seq(Record[Unit, String]((), line)))))

    val client = Process[IO](ref => {
      case Start => input ~> mapreduce
      case out: Output[String, Int] => eval(eventStore.add(ref, out))
    })


    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(client, mapreduce))).run))

    eventStore.get(client.ref).headOption.value.records.sorted shouldBe
      Seq(Record("Bye", 1),
        Record("Goodbye", 1),
        Record("Hello", 2),
        Record("Map", 2),
        Record("Reduce", 2),
        Record("World", 2))

  }

}
