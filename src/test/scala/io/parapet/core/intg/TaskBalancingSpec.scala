package io.parapet.core.intg
import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.flow._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.intg.TaskBalancingSpec._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.concurrent.duration._
class TaskBalancingSpec extends FlatSpec {

  "Slow worker" should "transfer some events to another worker" in {

    val p = Process[IO] {
      case TestEvent(id) => delay(10.seconds) ++ eval(println(id))
    }

    val program = TestEvent("1") ~> p ++ TestEvent("2")  ~> p ++ TestEvent("3")  ~> p ++ TestEvent("4")  ~> p

    run(program, config(1, 2), p)
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

  def config(workerQueueSize: Int, numOfWorkers: Int): ParConfig = {
    ParApp.defaultConfig.copy(
      schedulerConfig = ParApp.defaultConfig.schedulerConfig
        .copy(
          numberOfWorkers = numOfWorkers,
          workerQueueCapacity = workerQueueSize
        )
    )
  }
}

object TaskBalancingSpec {

  case class TestEvent(id: String) extends Event

}
