package io.parapet.core

import cats.{Id, Monad, ~>}
import io.parapet.core.Dsl.{Dsl, Eval, FlowOp, Send, UnitFlow, WithDsl}
import io.parapet.core.Event.Start
import io.parapet.core.ProcessSpec.LeaderElection._
import io.parapet.core.ProcessSpec._
import io.parapet.syntax.FlowSyntax
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ProcessSpec extends FlatSpec with WithDsl[Id] with FlowSyntax[Id] {

  "Process with num gt threshold" should "send propose" in {

    val peers = ListBuffer(ProcessRef("p1"), ProcessRef("p2"))

    val state = new LeaderElectionState(
      peers = peers,
      threshold = 0.85,
      random = () => 0.9,
      wheel = () => 0
    )

    val p = new LeaderElection[Id](state)

    val program = p(Start)

    val interpreter = new IdInterpreter()

    program.foldMap(interpreter)

    state.num shouldBe 0.9
    interpreter.execution.trace shouldBe peers.map(Propose(_, state.num))
  }

  "Process with highest num received positive ack" should "be happy" in {

    val peers = ListBuffer(ProcessRef("p1"), ProcessRef("p2"))


    val state = new LeaderElectionState(
      peers = peers,
      threshold = 0.85,
      random = () => 0.9,
      wheel = () => 0
    )
    state.num = 0.9


    val p = new LeaderElection[Id](state)
    val program = peers.map(peer => p(Ack(peer, 0.1, ok = true))).fold(dsl.unit)(_ ++ _)


    val interpreter = new IdInterpreter()

    program.foldMap(interpreter)

    state.num shouldBe 0.9
    state.votes shouldBe 2
    state.peerNum shouldBe peers.map(_ -> 0.1).toMap
  }

}

object ProcessSpec {

  class LeaderElectionState(val peers: ListBuffer[ProcessRef],
                            val threshold: Double = 0.85,
                            val random: () => Double,
                            val wheel: () => Int) {
    var num: Double = 0.0
    var votes = 0

    val peerNum: mutable.Map[ProcessRef, Double] = mutable.Map.empty
  }

  class LeaderElection[F[_]](state: LeaderElectionState) extends ProcessWithState[F, LeaderElectionState](state) {


    import dsl._

    override def handle: Receive = {
      case Start =>
        val num = state.random()
        if (num > state.threshold) {
          eval(state.num = num) ++ state.peers.map(p => Propose(ref, num) ~> p).fold(unit)(_ ++ _)
        } else {
          unit
        }

      case Ack(peer, num, ok) =>
        eval(println(s"peer[$peer] voted [$num, $ok]")) ++
          (if (ok) {
            for {
              _ <- eval {
                state.votes = state.votes + 1
                state.peerNum += (peer -> num)
                // todo check the majority
              }
            } yield ()
          } else {
            eval {
              state.num = num
              // reset votes ?
            }
          })
    }
  }

  // api

  object LeaderElection {

    case class Propose(ref: ProcessRef, num: Double) extends Event

    case class Ack(ref: ProcessRef, num: Double, ok: Boolean) extends Event

    case class Announce(ref: ProcessRef) extends Event

    case class Heartbeat(ref: ProcessRef) extends Event

  }


  // emulator

  case class Message(e: Event, target: ProcessRef)

  abstract class ProcessWithState[F[_], S](state: S) extends io.parapet.core.Process[F]

  class Execution(val trace: ListBuffer[Message] = ListBuffer.empty)

  class IdInterpreter extends (FlowOp[Id, *] ~> Id) {
    val execution: Execution = new Execution()

    override def apply[A](fa: FlowOp[Id, A]): Id[A] = {
      fa match {
        case _: UnitFlow[Id]@unchecked => ()
        case eval: Eval[Id, Dsl[Id, ?], A]@unchecked =>
          implicitly[Monad[Id]].pure(eval.thunk())
        case send: Send[Id]@unchecked =>
          println("send")
          send.receivers.foreach(p => {
            execution.trace.append(Message(send.e(), p))
          })
      }
    }
  }


  class MyProcess extends Process[Id] {

    import dsl._

    override def handle: Receive = {
      case Start => eval(println("test"))
    }

  }

}
