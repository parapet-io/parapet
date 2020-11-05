package io.parapet.core.processes

import io.parapet.core.ProcessRef
import io.parapet.core.processes.RouletteLeaderElection.ResponseCodes.AckCode
import io.parapet.core.processes.RouletteLeaderElection.{Ack, Announce, Heartbeat, Propose}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class RouletteLeaderElectionProtocolSpec extends FunSuite {

  test("propose") {
    val data = RouletteLeaderElection.encoder.write(Propose(ProcessRef("Propose"), 0.85))
    RouletteLeaderElection.encoder.read(data) shouldBe Propose(ProcessRef("Propose"), 0.85)
  }

  test("ack") {
    val data = RouletteLeaderElection.encoder.write(Ack(ProcessRef("Ack"), 0.86, AckCode.OK))
    RouletteLeaderElection.encoder.read(data) shouldBe Ack(ProcessRef("Ack"), 0.86, AckCode.OK)
  }

  test("announce") {
    val data = RouletteLeaderElection.encoder.write(Announce(ProcessRef("Announce")))
    RouletteLeaderElection.encoder.read(data) shouldBe Announce(ProcessRef("Announce"))
  }

  test("heartbeat") {
    val data = RouletteLeaderElection.encoder.write(Heartbeat(ProcessRef("Heartbeat")))
    RouletteLeaderElection.encoder.read(data) shouldBe Heartbeat(ProcessRef("Heartbeat"))
  }
}
