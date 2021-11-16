package io.parapet.core.processes

import com.typesafe.scalalogging.Logger
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.SystemEvent
import io.parapet.core.Process
import io.parapet.core.api.Cmd.coordinator._
import io.parapet.core.processes.Coordinator._
import io.parapet.{Event, ProcessRef}

import scala.concurrent.duration._
import scala.util.Random

class Coordinator[F[_]](override val ref: ProcessRef,
                        id: String,
                        client: ProcessRef,
                        peers: Map[String, ProcessRef],
                        threshold: Double,
                        generator: Generator = defaultGenerator,
                        timeout: FiniteDuration = 3.seconds) extends Process[F] {

  import dsl._

  private val logger = Logger[Coordinator[F]]
  private var _num = 0.0
  private var _votes = 0
  private var _round = 0
  private var _coordinator = Option.empty[String]
  private var _voted = false
  private val peersList = peers.values.toSeq

  private def info = s"id=$id, round=${_round}"

  override def handle: Receive = {
    case _: SystemEvent => unit
    case Start =>
      reset ++ flow {
        val minMax = generator.generate
        logger.debug(s"$info: new round has been started. minMax=$minMax")
        if (minMax.min > threshold) {
          eval {
            _num = minMax.max
            _votes = 1
            _voted = true
          } ++ eval(logger.debug(s"$info: generated num ${_num} > $threshold. send propose")) ++
            sendPropose ++ waitForCoordinator
        } else {
          eval(logger.debug(s"$info: generated num ${_num} < $threshold. wait for coordinator")) ++
            waitForCoordinator
        }
      }

    case Propose(senderId, num) =>
      eval(logger.debug(s"received propose from $senderId with num=$num")) ++ flow {
        if (_coordinator.nonEmpty) {
          Ack(id, _num, AckCode.Elected) ~> peers(senderId) ++ Elected(_coordinator.get) ~> peers(senderId)
        } else if (_voted) {
          Ack(id, _num, AckCode.Voted) ~> peers(senderId)
        } else if (num > _num) {
          eval {
            logger.debug(s"vote for $senderId, old num=${_num}, new num=$num")
            _voted = true
            _num = num
          } ++ Ack(id, _num, AckCode.Ok) ~> peers(senderId)
        } else {
          Ack(id, _num, AckCode.High) ~> peers(senderId)
        }
      }

    case Ack(senderId, num, code) =>
      eval(logger.debug(s"received ack from $senderId with num=$num, code=$code")) ++
        (code match {
          case AckCode.Ok => eval {
            _votes = _votes + 1
          } ++ flow {
            if (_votes >= peers.size / 2 + 1 && _coordinator.isEmpty) {
              eval {
                _coordinator = Option(id)
              } ++ sendToAll(Elected(id), peersList :+ client)
            } else {
              unit
            }
          }
          case AckCode.Voted =>
            eval(logger.debug(s"$senderId has been already voted"))
          case AckCode.High =>
            eval(logger.debug(s"$senderId num $num > ${_num}"))
        })

    case Elected(senderId) => eval {
      logger.debug(s"'$senderId' has been selected as coordinator")
      logger.debug(s"old coordinator=${_coordinator.getOrElse("none")}")
      _coordinator = Option(senderId)
    }

    case Timeout => if (_coordinator.isEmpty) {
      eval(logger.debug(s"${_round} has been expired. start a new round")) ++
        Start ~> ref
    } else {
      unit
    }
  }

  private def sendToAll(event: Event, seq: Seq[ProcessRef]): DslF[F, Unit] = {
    seq.map(event ~> _).fold(unit)(_ ++ _)
  }

  private def sendPropose: DslF[F, Unit] = flow {
    val propose = Propose(id, _num)
    sendToAll(propose, peersList)
  }

  private def reset: DslF[F, Unit] = eval {
    _votes = 0
    _num = 0
    _round = _round + 1
    _voted = false
    _coordinator = Option.empty
  }

  private def waitForCoordinator: DslF[F, Unit] = {
    for {
      _ <- eval(logger.debug("wait for majority of votes or elected event"))
      _ <- fork(delay(timeout) ++ Timeout ~> ref)
    } yield ()
  }
}

object Coordinator {

  case class MinMax(min: Double, max: Double) {
    override def toString: String = s"($min, $max)"
  }

  trait Generator {
    def generate: MinMax
  }

  object defaultGenerator extends Generator {
    override def generate: MinMax = {
      val r = new Random()
      var min = 1.0
      var max = 0.0
      val n = r.nextDouble()
      min = Math.min(min, n)
      max = Math.max(max, n)
      MinMax(min, max)
    }
  }

  case object Timeout extends Event


}