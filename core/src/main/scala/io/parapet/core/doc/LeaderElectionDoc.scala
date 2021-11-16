package io.parapet.core.doc

object LeaderElectionDoc {

  object Lemmas {

    case object Lemma1 extends Lemma {
      override val description: String =
        """
          | A node received a Timeout while waiting for a leader or to be become a coordinator resets its state.
          |""".stripMargin
    }

    case object Lemma2 extends Lemma {
      override val description: String =
        """
          | A node that received Announce become a leader.
          |""".stripMargin
    }

    case object Lemma3 extends Lemma {
      override val description: String =
        """
          | If the leader crashed the other nodes should start a new election round iff the cluster is complete
          | (the majority of nodes are alive).
          |""".stripMargin
    }

    case object Lemma4 extends Lemma {
      override val description: String =
        """
          | Whenever a node joins a cluster it can update its leader from Heartbeat message iff cluster is complete
          | and heartbeat message was sent by the active leader.
          |""".stripMargin
    }

    case object Lemma5 extends Lemma {
      override val description: String =
        """
          | leader crash must trigger new election.
          |""".stripMargin
    }

  }

}
