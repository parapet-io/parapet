package io.parapet.examples.perf

import java.util.concurrent.TimeUnit

import io.parapet.ParApp
import io.parapet.core.Event.{Failure, Start}
import io.parapet.core.{Event, EventLog, Process, ProcessRef}
import io.parapet.examples.perf.PerfTest._

abstract class PerfTest[F[_]](nMessages: Int, nProcesses: Int) extends ParApp[F] {

  // NOT IMPORTANT
  override val eventLog: EventLog[F] = new EventLog[F] {
    override def write(e: Event.Envelope): F[Unit] = {
      ct.delay(println("EventLog: " + e))
    }

    override def read: F[Seq[Event.Envelope]] = ct.pure(Seq.empty)
  }

  def createProcess(id: Int): Process[F] = new Process[F] {

    import dsl._

    override val ref: ProcessRef = ProcessRef(s"p-$id")

    override def handle: Receive = {
      case Request => withSender(Response ~> _)
    }
  }

  def createPublisher(nRequests: Int, consumers: Array[Process[F]]): Process[F] = new Process[F] {

    import dsl._

    override val ref: ProcessRef = ProcessRef("publisher")

    val nConsumers = consumers.length

    var responsesReceived = 0
    var start = 0L

    var pid = 0

    def next: Int = {
      val old = pid
      pid = pid + 1
      old
    }

    override def handle: Receive = {
      case Start =>
        val publish = (0 until nRequests).map(_ => Request ~> consumers(next % nConsumers))
          .fold(unit)(_ ++ _)
        publish ++ eval(start = System.nanoTime())
      case f: Failure => eval(println(f))
      case Response =>
        eval(responsesReceived = responsesReceived + 1) ++
          flow {
            if (responsesReceived == nRequests) eval {
              val time = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
              println("======================================")
              println(s"TIME: $time ms")
              println("======================================")
            } else unit
          }
    }
  }

  override def processes(args: Array[String]): F[Seq[Process[F]]] = {

    val consumers = new Array[Process[F]](nProcesses)
    val allProcesses = new Array[Process[F]](nProcesses + 1)


    (0 until nProcesses).foreach { i =>
      consumers(i) = createProcess(i)
    }
    consumers.copyToArray(allProcesses)
    val publisher = createPublisher(nMessages, consumers)
    allProcesses(nProcesses) = publisher
    ct.pure(allProcesses)
  }
}

object PerfTest {

  object Request extends Event

  object Response extends Event

}