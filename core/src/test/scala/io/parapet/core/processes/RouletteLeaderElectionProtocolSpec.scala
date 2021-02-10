package io.parapet.core.processes

import io.parapet.core.processes.RouletteLeaderElection.ResponseCodes.AckCode
import io.parapet.core.processes.RouletteLeaderElection.{Ack, Announce, Heartbeat, Propose}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import java.nio.ByteBuffer

class RouletteLeaderElectionProtocolSpec extends FunSuite {

  test("propose") {
    val data = addClientId(RouletteLeaderElection.encoder.write(Propose("Propose", 0.85)))
    RouletteLeaderElection.encoder.read(data) shouldBe Propose("Propose", 0.85)
  }

  test("ack") {
    val data = addClientId(RouletteLeaderElection.encoder.write(Ack("Ack", 0.86, AckCode.OK)))
    RouletteLeaderElection.encoder.read(data) shouldBe Ack("Ack", 0.86, AckCode.OK)
  }

  test("announce") {
    val data = addClientId(RouletteLeaderElection.encoder.write(Announce("Announce")))
    RouletteLeaderElection.encoder.read(data) shouldBe Announce("Announce")
  }

  test("heartbeat") {
    val data = addClientId(RouletteLeaderElection.encoder.write(Heartbeat("Heartbeat", Option.empty)))
    RouletteLeaderElection.encoder.read(data) shouldBe Heartbeat("Heartbeat", Option.empty)
  }

  private def addClientId(source: Array[Byte]): Array[Byte] = {
    val buf = ByteBuffer.allocate(4 + source.length)
    buf.putInt(0)
    buf.put(source)
    buf.rewind()
    buf.array()
  }
}
