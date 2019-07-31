package io.parapet.core.mapreduce

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.{Queue => JQueue}

import com.typesafe.scalalogging.StrictLogging
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.mapreduce.MapReduce._
import io.parapet.core.{Event, Process, ProcessRef}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class MapReduce[F[_], K, V, K1: Ordering, V1](
                                               mapperFunction: Record[K, V] => Seq[Record[K1, V1]],
                                               nMapperWorkers: Int,
                                               reducerFunction: (K1, Seq[V1]) => V1,
                                               nReducerWorkers: Int) extends Process[F] with StrictLogging {

  import dsl._

  // fixme: get rid of this variable
  private var client: ProcessRef = _

  override def handle: Receive = {
    case in: Input[K, V] => withSender { sender =>
      val chunks = in.chunks
      val queue = new ConcurrentLinkedQueue[Chunk[K, V]]
      queue.addAll(chunks.asJava)
      eval(client = sender) ++
        evalWith(new Shuffle(ref, chunks.size)) { shuffle =>
          register(ref, shuffle) ++
            (0 until nMapperWorkers).map(i => new MapWorker(i, shuffle.ref, queue))
              .map(worker => register(ref, worker)).fold(unit)(_ ++ _)
        }
    }
    case out: Output[K1, V1] => out ~> client
  }

  class MapWorker(id: Int, master: ProcessRef, queue: JQueue[Chunk[K, V]]) extends Process[F] {

    def loop(result: ListBuffer[Record[K1, V1]]): DslF[F, Unit] = {
      def step: DslF[F, Unit] = flow {
        evalWith(Option(queue.poll())) {
          case Some(chunk) => eval(result ++= chunk.records.flatMap(mapperFunction)) ++ step
          case None => unit
        }
      }

      step
    }

    override def handle: Receive = {
      case Start =>
        val records = ListBuffer[Record[K1, V1]]()
        loop(records) ++ MapOutput(records) ~> master ++ Stop ~> ref
      case Stop => eval(logger.debug(s"MapWorker[$id] stopped"))
    }
  }

  implicit def tupleSort[S[_] <: Seq[_]]: Ordering[(K1, S[V1])] = new Ordering[(K1, S[V1])] {
    override def compare(x: (K1, S[V1]), y: (K1, S[V1])): Int = {
      implicitly[Ordering[K1]].compare(x._1, y._1)
    }
  }

  // sink for Mapper phase
  class Shuffle(master: ProcessRef, mapOutputsExpected: Int) extends Process[F] {
    val mapped = ListBuffer[Record[K1, V1]]()
    val reduced = ListBuffer[Record[K1, V1]]()
    var mapOutputsReceived = 0
    var reduceOutputsReceived = 0
    var reduceOutputsExpected = 0

    def shuffle(): Seq[(K1, Seq[V1])] = {
      mapped.groupBy(_.key).mapValues(_.map(_.value)).toSeq.sorted
    }

    override def handle: Receive = {
      case out: MapOutput[K1, V1] =>
        eval(logger.debug(s"Shuffle: received Map output: $out")) ++
          eval {
            mapped ++= out.records
            mapOutputsReceived = mapOutputsReceived + 1
          } ++ flow {
          if (mapOutputsReceived == mapOutputsExpected) {
            // reduce
            evalWith {
              val records = shuffle()
              reduceOutputsExpected = records.size
              records
            } { records =>
              evalWith {
                (0 until nReducerWorkers).map(i => (i, new ReduceWorker(i))).toMap
              } { workers =>
                workers.values.map(register(ref, _)).fold(unit)(_ ++ _) ++
                  par {
                    records.map {
                      case (key, values) =>
                        ReduceInput(key, values) ~> workers(key.hashCode() % nReducerWorkers)
                    }.fold(unit)(_ ++ _)
                  }
              }
            }
          } else unit
        }

      case out: ReduceOutput[K1, V1] =>
        eval(logger.debug(s"Shuffle: received Reduce output: $out")) ++
          eval {
            reduced ++= out.records
            reduceOutputsReceived = reduceOutputsReceived + 1
          } ++ flow {
          if (reduceOutputsReceived == reduceOutputsExpected) Output(reduced) ~> master ++ Stop ~> ref
          else unit
        }

    }
  }

  class ReduceWorker(id: Int) extends Process[F] {
    override def handle: Receive = {
      case in: ReduceInput[K1, V1] => withSender { sender =>
        ReduceOutput(Seq(Record(in.key, reducerFunction(in.key, in.values)))) ~> sender
      }
      case Stop => eval(logger.debug(s"ReduceWorker[$id] stopped"))
    }
  }

}

object MapReduce {

  case class Record[K, V](key: K, value: V)

  implicit def recordOrder[K: Ordering, V]: Ordering[Record[K, V]] =
    (x: Record[K, V], y: Record[K, V]) => implicitly[Ordering[K]].compare(x.key, y.key)


  case class Chunk[K, V](records: Seq[Record[K, V]]) extends Event

  case class Input[K, V](chunks: Seq[Chunk[K, V]]) extends Event

  case class ReduceInput[K, V](key: K, values: Seq[V]) extends Event

  sealed trait GenericOutput[K, V] extends Event {
    val records: Seq[Record[K, V]]

    protected[mapreduce] def add(record: Record[K, V]): GenericOutput[K, V]

    protected[mapreduce] def addAll(values: Seq[Record[K, V]]): GenericOutput[K, V]
  }

  case class MapOutput[K, V](records: Seq[Record[K, V]]) extends GenericOutput[K, V] {
    def add(record: Record[K, V]): GenericOutput[K, V] = MapOutput(records :+ record)

    def addAll(values: Seq[Record[K, V]]): GenericOutput[K, V] = MapOutput(records ++ values)
  }

  case class ReduceOutput[K, V](records: Seq[Record[K, V]]) extends GenericOutput[K, V] {
    def add(record: Record[K, V]): GenericOutput[K, V] = ReduceOutput(records :+ record)

    def addAll(values: Seq[Record[K, V]]): GenericOutput[K, V] = ReduceOutput(records ++ values)
  }

  case class Output[K, V](records: Seq[Record[K, V]]) extends GenericOutput[K, V] {
    def add(record: Record[K, V]): GenericOutput[K, V] = Output(records :+ record)

    def addAll(values: Seq[Record[K, V]]): GenericOutput[K, V] = Output(records ++ values)
  }

}