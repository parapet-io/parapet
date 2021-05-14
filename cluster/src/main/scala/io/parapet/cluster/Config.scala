package io.parapet.cluster

import io.parapet.cluster.Config.PeerInfo

import java.io.FileInputStream
import java.util.Properties
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Using

case class Config(
    id: String,
    address: String,
    peers: Array[PeerInfo],
    leaderElectionThreshold: Double,
    electionDelay: FiniteDuration,
    heartbeatDelay: FiniteDuration,
    monitorDelay: FiniteDuration,
    peerTimeout: FiniteDuration,
)

object Config {

  def load(path: String): Config =
    Using.resource(new FileInputStream(path)) { is =>
      val prop = new Properties()
      prop.load(is)
      Config(
        id = prop.getProperty("node.id"),
        address = prop.getProperty("node.address"),
        peers = parsePeers(prop.getProperty("node.peers", "")),
        electionDelay = prop.getProperty("node.election-delay").toInt.seconds,
        heartbeatDelay = prop.getProperty("node.heartbeat-delay").toInt.seconds,
        monitorDelay = prop.getProperty("node.monitor-delay").toInt.seconds,
        peerTimeout = prop.getProperty("node.peer-timeout").toInt.seconds,
        leaderElectionThreshold = prop.getProperty("node.leader-election-threshold").toDouble,
      )
    }

  def parsePeers(str: String): Array[PeerInfo] =
    str
      .split(",")
      .map(p =>
        p.split(":", 2) match {
          case Array(id, address) => PeerInfo(id, address)
          case _ => throw new RuntimeException(s"invalid format=$p")
        },
      )

  case class PeerInfo(id: String, address: String)

}
