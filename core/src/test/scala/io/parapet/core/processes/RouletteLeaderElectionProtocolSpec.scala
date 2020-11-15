package io.parapet.core.processes

import io.parapet.core.processes.RouletteLeaderElection.ResponseCodes.AckCode
import io.parapet.core.processes.RouletteLeaderElection.{Ack, Announce, Heartbeat, Propose}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class RouletteLeaderElectionProtocolSpec extends FunSuite {

  test("propose") {
    val data = RouletteLeaderElection.encoder.write(Propose("Propose", 0.85))
    RouletteLeaderElection.encoder.read(data) shouldBe Propose("Propose", 0.85)
  }

  test("ack") {
    val data = RouletteLeaderElection.encoder.write(Ack("Ack", 0.86, AckCode.OK))
    RouletteLeaderElection.encoder.read(data) shouldBe Ack("Ack", 0.86, AckCode.OK)
  }

  test("announce") {
    val data = RouletteLeaderElection.encoder.write(Announce("Announce"))
    RouletteLeaderElection.encoder.read(data) shouldBe Announce("Announce")
  }

  test("heartbeat") {
    val data = RouletteLeaderElection.encoder.write(Heartbeat("Heartbeat"))
    RouletteLeaderElection.encoder.read(data) shouldBe Heartbeat("Heartbeat")
  }
}
