package io.parapet.examples.peer

import cats.effect.IO
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Marchall, Start}
import io.parapet.core.processes.PeerProcess
import io.parapet.{CatsApp, core}
import io.parapet.core.{Event, Process}
import io.parapet.p2p.Protocol

import scala.concurrent.duration._

object PeerApp extends CatsApp {

  import dsl._

  override def processes: IO[Seq[core.Process[IO]]] = {

    val peerProcess = PeerProcess[IO]()

    def sendPing: DslF[IO, Unit] = flow {
      delay(3.seconds, Ping ~> peerProcess.ref) ++ sendPing
    }

    val client = Process[IO](ref => {
      case Start => PeerProcess.Reg(ref) ~> peerProcess.ref
      case PeerProcess.Ack(_) => fork(sendPing)
      case PeerProcess.CmdEvent(cmd) =>
        if (cmd.getCmdType == Protocol.CmdType.DELIVER) {
          eval(println(new String(cmd.getData.toByteArray)))
        } else unit

    })
    IO(Seq(client, peerProcess))
  }


  object Ping extends Event with Marchall {
    override def marshall: Array[Byte] = "PING".getBytes
  }

}
