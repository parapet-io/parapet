package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.Event
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events
import io.parapet.tests.intg.BasicCatsIOSpec
import io.parapet.testutils.EventStore
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class AsyncSpec extends AnyFunSuite with BasicCatsIOSpec {

  import dsl._

  def calc(d: Int): DslF[IO, Int] = {
    eval(d * d)
  }

  test("parallel") {
    val eventStore = new EventStore[IO, Event]
    val process = io.parapet.core.Process.builder[IO](ref => {
      case Events.Start =>
        for {
          fibers <- par(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(calc): _*)
          res <- fibers.map(_.join).reduce((fa, fb) => fa.flatMap(a => fb
            .map(b => a + b)))
          _ <- eval(res shouldBe 385)
          _ <- eval(eventStore.add(ref, Events.Stop))
        } yield ()
    }).build
    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(process))).run))
  }
}
