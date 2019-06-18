package io.parapet.core.intg

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.IO
import io.parapet.core.{Event, Process}
import io.parapet.instances.DslInstances.catsInstances.effect._
import io.parapet.instances.DslInstances.catsInstances.flow._
import io.parapet.core.intg.ProcessRecursionSpec._
import io.parapet.implicits._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class ProcessRecursionSpec extends FlatSpec with IntegrationSpec {

  "Process" should "be able to send event to itself" in {
    val deliverCount = new AtomicInteger()
    val count = 100000

    val p = Process.apply[IO](self => {
      case Counter(i) =>
        if (i == 0) terminate
        else eval(deliverCount.incrementAndGet()) ++ Counter(i - 1) ~> self
    })


    val program = Counter(count) ~> p

    run(program, p)

    deliverCount.get() shouldBe count

  }

}

object ProcessRecursionSpec {

  case class Counter(value: Int) extends Event

}