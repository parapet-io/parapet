package io.parapet.core.intg
import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.flow._
import io.parapet.core.intg.TaskBalancingSpec._
import io.parapet.core.testutils.EventStoreProcess
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.concurrent.duration._
class TaskBalancingSpec extends FlatSpec {

  "Slow worker" should "transfer some events to another worker" in {
    val workerQueueSize = 1
    val numOfWorkers = 2
    val taskSubmissionTimeout = 1.second
    val eventStore = new EventStoreProcess
    val p = new SlowProcess(eventStore)

    val program = TestEvent(1) ~> p ++ TestEvent(2) ~> p ++ TestEvent(3) ~> p ++ TestEvent(4) ~> p ++ terminate

    run(program, config(workerQueueSize, numOfWorkers, taskSubmissionTimeout), p)
    eventStore.events shouldBe Seq(TestEvent(1), TestEvent(2), TestEvent(3), TestEvent(4))

//    Task is defined as pair of Event and Process that should process this event, i.e. e ~> p <=> Task(e, p)
//      Task queue state is denoted as Q[T1(Ei, Pi), T2(Ei, Pi), ..., Tn(Ei, Pi)], for example a queue containing two tasks is defined as Q[T1, T2]
//      we use a short form to name events, e.g. TestEvent(1) is E1, TestEvent(2) is E2 and etc.
//      since all events sent to the same process we simplify our example by substituting T(task) with E(event)
//    1. E1 added to worker-1's queue: Q[E1]
//    2. worker-1 dequeued E1 and started processing: Q[]
//    3. E2 added to worker-1's queue: Q[E2]. note this step might occur after step 1
//    4. Scheduler attempts to enqueue E3 but fails by timeout
//    5. Scheduler removes all events that dedicated to same process that has to process E3 from worker-1's queue
//       followed by E3 and tries to submit them to another worker, e.g. worker-2
//    6. Scheduler repeats steps 1-3:
//        * E2 added to worker-2's queue: Q[E2]
//        * worker-2 dequeued E2 and started processing: Q[]
//        * E3 added to worker-2's queue: Q[E3]
//    7. Scheduler attempts to submit E4 to worker-2 but fails by timeout
//    8. Scheduler repeats step 5, it removes E3 from worker-2's queue and and tries to submit E3 and E4 to worker-1
//    9. Scheduler repeats steps 1-3
//    10. Done
  }

  def run(pProgram: FlowF[IO, Unit],
          pConfig: ParConfig,
          pProcesses: Process[IO]*): Unit = {
    val app = new CatsApp {
      override val config: ParConfig = pConfig
      override val program: ProcessFlow = pProgram
      override val processes: Array[Process[IO]] = pProcesses.toArray
    }
    app.run.unsafeRunSync()
  }

  def config(workerQueueSize: Int, numOfWorkers: Int, taskSubmissionTimeout: FiniteDuration): ParConfig = {
    ParApp.defaultConfig.copy(
      schedulerConfig = ParApp.defaultConfig.schedulerConfig
        .copy(
          numberOfWorkers = numOfWorkers,
          workerQueueCapacity = workerQueueSize,
          taskSubmissionTimeout = taskSubmissionTimeout
        )
    )
  }
}

object TaskBalancingSpec {

  case class TestEvent(taceId: Int) extends Event

  class SlowProcess(eventStore: EventStoreProcess) extends Process[IO] {
    override val name: String = "slow-process"
    override val handle: Receive = {
      case e:TestEvent => delay(2.seconds)  ++ eventStore(e)
    }
  }
}
