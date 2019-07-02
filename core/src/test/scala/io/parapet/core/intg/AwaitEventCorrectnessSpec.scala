package io.parapet.core.intg

import java.util.UUID

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import io.parapet.core.Dsl.WithDsl
import io.parapet.core.Event.Start
import io.parapet.core.intg.AwaitEventCorrectnessSpec._
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process}
import io.parapet.implicits._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.concurrent.duration._

class AwaitEventCorrectnessSpec extends FunSuite with WithDsl[IO] with IntegrationSpec with StrictLogging {

  import effectDsl._
  import flowDsl._

  test("await event correctness") {
    val requestAwaitTimeout = 5000.millis
    val heartbeatAwaitTimeout = 5000.millis
    val eventStore = new EventStore[Event]

    val okRequests = (0 until 100).map(_ => Request(genId, 0.millis))
    val delayedRequests = (0 until 100).map(_ => Request(genId, requestAwaitTimeout * 2))

    val requests = okRequests ++ delayedRequests

    val okHeartbeats = (0 until 100).map(_ => Heartbeat(genId, 0.millis))
    val delayedHeartbeats = (0 until 100).map(_ => Heartbeat(genId, heartbeatAwaitTimeout * 2))

    val heartbeats = okHeartbeats ++ delayedHeartbeats

    val allEvents = requests ++ heartbeats

    val delayedEvents = delayedRequests ++ delayedHeartbeats

    val expectedSize = allEvents.size + delayedRequests.size + delayedHeartbeats.size

    val server = new Process[IO] {
      override def handle: Receive = {
        case Request(id, time) =>
          if (time.length == 0) reply(sender => Response(id) ~> sender)
          else
            fork {
              eval(logger.debug(s"server received req [$id, $time]")) ++
                delay(time) ++ reply(sender => Response(id) ~> sender) ++
                eval(logger.debug(s"server sent res [$id]"))
            }
        case Heartbeat(id, time) =>
          if (time.length == 0) reply(sender => Ack(id) ~> sender)
          else
            fork {
              eval(logger.debug(s"server received heartbeat [$id, $time]")) ++
                delay(time) ++ reply(sender => Ack(id) ~> sender) ++
                eval(logger.debug(s"server sent ack [$id]"))
            }
      }
    }

    val client: Process[IO] = new Process[IO] {

      override def handle: Receive = {
        case req: Request =>
          eval(logger.debug(s"client received req [${req.id}, ${req.delay}]")) ++
            req ~> server.selfRef ++
            await {
              case Response(id) if id == req.id =>
            }(Timeout(req.id) ~> selfRef, requestAwaitTimeout) ++
            eval(logger.debug(s"client sent req [${req.id}, ${req.delay}]"))

        case hb: Heartbeat =>
          eval(logger.debug(s"client received heartbeat [${hb.id}, ${hb.delay}]")) ++
            hb ~> server.selfRef ++
            await {
              case Ack(id) if id == hb.id =>
            }(Timeout(hb.id) ~> selfRef, heartbeatAwaitTimeout) ++
            eval(logger.debug(s"client sent heartbeat [${hb.id}, ${hb.delay}]"))


        case r@Response(id) =>
          eval(logger.debug(s"client received response[$id]")) ++
            eval(eventStore.add(selfRef, r))
        case ack@Ack(id) =>
          eval(logger.debug(s"client received ack[$id]"))
          eval(eventStore.add(selfRef, ack))
        case t@Timeout(id) =>
          eval(logger.debug(s"client received timeout[$id]")) ++
            eval(eventStore.add(selfRef, t))
        case Start => allEvents ~> selfRef
      }
    }
    val processes = Array(client, server)

    val program = for {
      fiber <- run(empty, processes).start
      _ <- eventStore.awaitSize(expectedSize).guaranteeCase(_ => fiber.cancel)

    } yield ()

    val reqEventIds = okRequests.map(_.id)
    val delayedReqEventIds = delayedRequests.map(_.id)
    val heartbeatEventIds = okHeartbeats.map(_.id)
    val delayedHeartbeatEventIds = delayedHeartbeats.map(_.id)

    logger.debug(s"reqEventIds: $reqEventIds")
    logger.debug(s"delayedReqEventIds: $delayedReqEventIds")
    logger.debug(s"heartbeatEventIds: $heartbeatEventIds")
    logger.debug(s"delayedHeartbeatEventIds: $delayedHeartbeatEventIds")


    program.unsafeRunSync()


    eventStore.size shouldBe expectedSize

    // includes delayed requests
    val responseEventIds = eventStore.get(client.selfRef)
      .filter(_.isInstanceOf[Response]).map(_.asInstanceOf[Response].id)

    // includes delayed heartbeats
    val ackEventIds = eventStore.get(client.selfRef)
      .filter(_.isInstanceOf[Ack]).map(_.asInstanceOf[Ack].id)

    // includes Request and Heartbeat
    val timeoutEventIds = eventStore.get(client.selfRef)
      .filter(_.isInstanceOf[Timeout]).map(_.asInstanceOf[Timeout].id)

    logger.debug(s"responseEventIds: $responseEventIds")
    logger.debug(s"ackEventIds: $ackEventIds")
    logger.debug(s"timeoutEventIds: $timeoutEventIds")

    assert(requests.map(_.id).toSet.subsetOf(responseEventIds.toSet))
    assert(heartbeats.map(_.id).toSet.subsetOf(ackEventIds.toSet))

    timeoutEventIds.toSet shouldBe delayedEvents.map(_.id).toSet
  }

}

object AwaitEventCorrectnessSpec {

  def genId: String = UUID.randomUUID().toString

  trait WithId {
    val id: String
  }

  case class Request(id: String, delay: FiniteDuration) extends Event with WithId

  case class Response(id: String) extends Event with WithId

  case class Heartbeat(id: String, delay: FiniteDuration) extends Event with WithId

  case class Ack(id: String) extends Event with WithId

  case class Timeout(id: String) extends Event with WithId

}
