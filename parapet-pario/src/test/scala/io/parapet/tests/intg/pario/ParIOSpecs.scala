package io.parapet.tests.intg.pario

import io.parapet.Event
import io.parapet.Event.{ByteEvent, StringEvent}
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events
import io.parapet.core.Events.Start
import io.parapet.core.Process
import io.parapet.core.{Channel, Parapet}
import io.parapet.core.Channel.ChannelInterruptedException
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.SchedulerConfig
import io.parapet.effect.ParIO
import io.parapet.testutils.EventStore
import io.parapet.tests.intg.BasicParIOSpec
import io.parapet.{ProcessRef, core}
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import scala.concurrent.duration.*

class BlockingSpec extends io.parapet.tests.intg.BlockingSpec[ParIO] with BasicParIOSpec

@Ignore
class ChannelSpec extends io.parapet.tests.intg.ChannelSpec[ParIO] with BasicParIOSpec:
  override val config: Parapet.ParConfig = ParConfig(-1, SchedulerConfig(1))

class DslSpec extends io.parapet.tests.intg.DslSpec[ParIO] with BasicParIOSpec

class DynamicProcessCreationSpec extends io.parapet.tests.intg.DynamicProcessCreationSpec[ParIO] with BasicParIOSpec

class ErrorHandlingSpec extends io.parapet.tests.intg.ErrorHandlingSpec[ParIO] with BasicParIOSpec

class EventDeliverySpec extends io.parapet.tests.intg.EventDeliverySpec[ParIO] with BasicParIOSpec

class ProcessBehaviourSpec extends io.parapet.tests.intg.ProcessBehaviourSpec[ParIO] with BasicParIOSpec

class ProcessLifecycleSpec extends io.parapet.tests.intg.ProcessLifecycleSpec[ParIO] with BasicParIOSpec

class ProcessSpec extends io.parapet.tests.intg.ProcessSpec[ParIO] with BasicParIOSpec

class ReplySpec extends io.parapet.tests.intg.ReplySpec[ParIO] with BasicParIOSpec

class SchedulerCorrectnessSpec
    extends io.parapet.tests.intg.scheduler.SchedulerCorrectnessSpec[ParIO]
    with BasicParIOSpec

// `sbt test` runs a short smoke stress (default `SCHEDULER_STRESS_ITERATIONS=5`). For long-running
// stress use the dedicated `schedulerStress` sbt task which sets iterations to `0` (infinite).
class SchedulerStressSpec extends io.parapet.tests.intg.scheduler.SchedulerStressSpec[ParIO] with BasicParIOSpec

class SchedulerSpec extends io.parapet.tests.intg.scheduler.SchedulerSpec[ParIO] with BasicParIOSpec

class SelfSendSpec extends io.parapet.tests.intg.SelfSendSpec[ParIO] with BasicParIOSpec

class SwitchBehaviorSpec extends io.parapet.tests.intg.SwitchBehaviorSpec[ParIO] with BasicParIOSpec

class AsyncSpec extends AnyFunSuite with BasicParIOSpec:
  import dsl._

  private def calc(d: Int): DslF[ParIO, Int] =
    eval(d * d)

  test("parallel") {
    val eventStore = new EventStore[ParIO, Event]
    val process    = core.Process
      .builder[ParIO](ref => { case Events.Start =>
        for
          fibers <- par(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(calc): _*)
          res    <- fibers.map(_.join).reduce((fa, fb) => fa.flatMap(a => fb.map(b => a + b)))
          _      <- eval(res shouldBe 385)
          _      <- eval(eventStore.add(ref, Events.Stop))
        yield ()
      })
      .build

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(process))).run))
  }

@Ignore
class BlockingChannelWithTimeout extends AnyFunSuite with BasicParIOSpec:
  import dsl._

  test("blockingChannelTimeout") {
    val eventStore = new EventStore[ParIO, Event]

    val server = Process
      .builder[ParIO](_ => { case e: ByteEvent =>
        eval(println(s"server received: $e")) ++
          delay(10.seconds) ++
          withSender(sender => ByteEvent("res".getBytes) ~> sender)
      })
      .ref(ProcessRef("server"))
      .build

    val failover = Process
      .builder[ParIO](ref => { case e: ByteEvent =>
        withSender(sender => eval(println(s"failover received: $e from $sender"))) ++
          eval(eventStore.add(ref, StringEvent("success")))
      })
      .ref(ProcessRef("failover"))
      .build

    val clientRef = ProcessRef("client")
    val ch        = new Channel[ParIO](clientRef)
    val client    = Process
      .builder[ParIO](ref => { case Start =>
        register(ref, ch) ++
          blocking {
            race(
              ch.send(
                ByteEvent("request".getBytes()),
                server.ref,
                {
                  case scala.util.Failure(ChannelInterruptedException(_, _)) =>
                    eval(println("channel was interrupted")) ++ ByteEvent("help".getBytes()) ~> failover
                  case res =>
                    eval(println(s"client received: $res"))
                }
              ),
              delay(3.seconds) ++ eval(println("server doesn't respond"))
            ) ++ ch.send(ByteEvent("request".getBytes()), failover.ref, _ => unit)
          }
      })
      .ref(clientRef)
      .build

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(client, server, failover))).run))
    eventStore.get(failover.ref) shouldBe Seq(StringEvent("success"), StringEvent("success"))
  }
