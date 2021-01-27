package io.parapet.cluster

import scala.concurrent.duration.FiniteDuration

case class Config(
    id: String,
    address: String,
    peers: Array[String],
    leaderElectionThreshold: Double,
    electionDelay: FiniteDuration,
    heartbeatDelay: FiniteDuration,
    monitorDelay: FiniteDuration,
    peerTimeout: FiniteDuration,
)
