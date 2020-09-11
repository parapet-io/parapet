package io.parapet.examples.dsl

import cats.effect.IO
import cats.syntax.flatMap._
import io.parapet.CatsApp
import io.parapet.core.Event.Start
import io.parapet.core.Process

import scala.concurrent.duration._

object BlockingExample extends CatsApp {

  class Service {
    def blockingCall: IO[String] = IO.sleep(1.second) >> IO("data")
  }

  class Client extends Process[IO] {

    import dsl._

    private lazy val service = new Service()

    override def handle: Receive = {
      case Start =>
          suspendWith(service.blockingCall)(data => eval(println(data)))
    }
  }


  override def processes: IO[Seq[Process[IO]]] = IO(Seq(new Client))
}
