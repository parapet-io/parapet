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

/**
  * In-memory implementation of MapReduce algorithm:
  *
  * @param mapperFunction  a function that takes a [[Record[K, V]] - pair of (key, value)
  *                        and produces a sequence of [[Record[K1, V1]]
  * @param nMapperWorkers  the number of mapper workers. Tasks aren't assigned to workers,
  *                        instead workers stealing tasks from the task queue
  * @param reducerFunction a function that takes a key and values associated with this key
  *                        and produces a single value
  * @param nReducerWorkers the number of reducer workers. Records assigned to a worker using a partition function:
  *                        `key.hashCode() % nReducerWorkers`
  * @tparam F  an effect type
  * @tparam K  initial key type
  * @tparam V  initial value type
  * @tparam K1 key type produced by mapper
  * @tparam V1 value type produced by mapper
  */
class MapReduce[F[_], K, V, K1: Ordering, V1](
                                               mapperFunction: Record[K, V] => Seq[Record[K1, V1]],
                                               nMapperWorkers: Int,
                                               reducerFunction: (K1, Seq[V1]) => V1,
                                               nReducerWorkers: Int) extends Process[F] with StrictLogging {

  import dsl._


  override def handle: Receive = {
    case in: Input[K, V] => withSender { sender =>
      val chunks = in.chunks
      val queue = new ConcurrentLinkedQueue[Chunk[K, V]]
      queue.addAll(chunks.asJava)
      evalWith(new Shuffle(sender, chunks.size)) { shuffle =>
        register(ref, shuffle) ++
          (0 until nMapperWorkers).map(i => new MapWorker(i, shuffle.ref, queue))
            .map(worker => register(ref, worker)).fold(unit)(_ ++ _)
      }
    }
  }

  /**
    * Mapper worker reads chucks from the shared queue.
    * Once the queue is empty the worker stops itself.
    *
    * @param id     worker numeric id
    * @param master the master process to send an output
    * @param queue  the chunk queue
    */
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

  /**
    * This process waits for all mappers to finish then groups values by key, sorts, shuffles and sends to the reducers.
    * It waits for reducers to finish then combines outputs and sends final output to the master process.
    *
    * @param master             the process to send an output
    * @param mapOutputsExpected number of mappers to wait
    */
  private class Shuffle(master: ProcessRef, mapOutputsExpected: Int) extends Process[F] {
    private val mapped = ListBuffer[Record[K1, V1]]()
    private val reduced = ListBuffer[Record[K1, V1]]()
    private var mapOutputsReceived = 0
    private var reduceOutputsReceived = 0
    private var reduceOutputsExpected = 0

    // groups by key and sorts
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
                        ReduceInput(key, values) ~> workers(partition(key, nReducerWorkers))
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
          if (reduceOutputsReceived == reduceOutputsExpected) Output(reduced) ~> master ++
            Stop ~> ref // it will also stop all reducer workers
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

  private def partition(key: Any, numReduceTasks: Int): Int = {
    (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks
  }

}

object MapReduce {

  case class Record[K, V](key: K, value: V)

  implicit def tupleOrdering[K: Ordering, V]: Ordering[(K, V)] = new Ordering[(K, V)] {
    override def compare(x: (K, V), y: (K, V)): Int = {
      implicitly[Ordering[K]].compare(x._1, y._1)
    }
  }

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