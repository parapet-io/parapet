package io.parapet.cluster

import cats.effect.IO
import io.parapet.cluster.api.ClusterApi._
import io.parapet.core.Process
import io.parapet.core.processes.RouletteLeaderElection.Req

class ClusterProcess extends Process[IO] {

  import dsl._

  override def handle: Receive = { case Req(clientId, data) =>
    encoder.read(data) match {
      case Join(group, addr) => eval(println(s"client[id: $clientId, addr: $addr] joined group: $group"))
    }
  }
}

object ClusterProcess {}
