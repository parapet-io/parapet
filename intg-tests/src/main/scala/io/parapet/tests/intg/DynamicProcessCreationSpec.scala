package io.parapet.tests.intg

import cats.implicits._
import io.parapet.core.Event.Start
import io.parapet.core.api.Event
import io.parapet.core.{Process, ProcessRef}
import io.parapet.tests.intg.DynamicProcessCreationSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

abstract class DynamicProcessCreationSpec[F[_]] extends AnyFunSuite with IntegrationSpec[F] {

  test("create child process") {

    val eventStore = new EventStore[F, Event]

    val workersCount = 5
    val tasksCount = 5
    val db = new Database[F]
    val server = new Server[F](workersCount, db.ref, tasksCount, eventStore)

    unsafeRun(eventStore.await(workersCount * tasksCount, createApp(ct.pure(Seq(db, server))).run))


    val expectedEvents = (tasksCount to 1 by -1).map(i => Ack(i))

    eventStore.size shouldBe workersCount * tasksCount

    (0 until workersCount).foreach { i =>
      eventStore.get(ProcessRef(s"worker-$i")) shouldBe expectedEvents
    }

  }

}

object DynamicProcessCreationSpec {

  class Worker[F[_]](id: Int,
                     db: ProcessRef,
                     tasksCount: Int,
                     eventStore: EventStore[F, Event]) extends Process[F] {

    import dsl._

    override val name: String = s"worker-$id"
    override val ref: ProcessRef = ProcessRef(s"worker-$id")

    override def handle: Receive = {
      case Start => Persist(tasksCount) ~> db
      case a@Ack(i) => if (i == 0) unit // done
      else eval(eventStore.add(ref, a)) ++ Persist(i - 1) ~> db
    }
  }

  class Database[F[_]] extends Process[F] {

    import dsl._

    override def handle: Receive = {
      case Persist(id) => withSender(sender => Ack(id) ~> sender)
    }
  }

  class Server[F[_]](workersCount: Int,
                     db: ProcessRef,
                     tasksCount: Int,
                     eventStore: EventStore[F, Event]) extends Process[F] {

    import dsl._

    override def handle: Receive = {
      case Start =>
        (0 until workersCount).map(i => register(ref, new Worker(i, db, tasksCount, eventStore)).void).fold(unit)(_ ++ _)
    }
  }

  case class Persist(id: Int) extends Event

  case class Ack(id: Int) extends Event

}