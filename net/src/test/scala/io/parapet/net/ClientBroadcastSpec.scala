package io.parapet.net

import cats.effect.IO
import io.parapet.core.Events
import io.parapet.core.api.Cmd
import io.parapet.tests.intg.BasicCatsIOSpec
import io.parapet.testutils.EventStore
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration._

class ClientBroadcastSpec extends AnyFunSuite with BasicCatsIOSpec {

  import dsl._

  test("broadcast") {
    val numOfClients = 5
    val eventStore = new EventStore[IO, Event]()
    val clients = (1 to numOfClients).map { i =>
      io.parapet.core.Process.builder[IO](_ => {
        case Cmd.netClient.Send(data, Some(reply)) =>
          Cmd.netClient.Rep(Option((new String(data) + "-" + i).getBytes())) ~> reply
      }).ref(ProcessRef(s"net-client-$i")).build
    }.toList

    val broadcast = new ClientBroadcast[IO]

    val app = io.parapet.core.Process.builder[IO](ref => {
      case Events.Start =>
        ClientBroadcast.Send("ping".getBytes(), clients.map(_.ref)) ~> broadcast.ref
      case res: ClientBroadcast.Response =>
        eval(eventStore.add(ref, res))
    }).ref(ProcessRef("ref")).build

    unsafeRun(eventStore.await(1,
      createApp(ct.pure(clients ++ Seq(app, broadcast))).run, timeout = 5.minutes))

    val response = eventStore.get(app.ref).head.asInstanceOf[ClientBroadcast.Response]
    val replies = response.res.collect {
      case scala.util.Success(Cmd.netClient.Rep(Some(data))) => new String(data)
    }.sorted
    replies shouldBe Seq("ping-1", "ping-2", "ping-3", "ping-4", "ping-5")
  }

}
