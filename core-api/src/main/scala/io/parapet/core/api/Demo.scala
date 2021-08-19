package io.parapet.core.api

import io.parapet.core.api.Cmd.{cluster, leaderElection}

object Demo {


  def main(args: Array[String]): Unit = {
    Cmd(Cmd.leaderElection.Who("1").toByteArray) match {
      case leaderElection.Who(clientId) => println("i'm dod")
      case api: cluster.Api =>
    }
  }

}
