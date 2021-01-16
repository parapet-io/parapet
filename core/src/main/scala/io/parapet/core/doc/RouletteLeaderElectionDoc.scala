package io.parapet.core.doc

object RouletteLeaderElectionDoc {

  object Lemmas {

    case object Lemma1 extends Lemma {
      override val description: String =
        """
          | A node generated a random number greater than chosen threshold becomes a candidate
          | and sends to all other nodes a special Propose message with its number.
          |""".stripMargin
    }

    case object Lemma2 extends Lemma {
      override val description: String =
        """
          | A node has not voted yet for a bigger number in the current round, received Propose from a node
          | with a bigger number sends Ack(Ok) (success) with its greatest random generated number.
          |""".stripMargin
    }

    case object Lemma3 extends Lemma {
      override val description: String =
        """
          | A node with a number greater than the one from Propose message, sends a Ack(HIGH) (reject) to the sender
          | with its greatest number.
          |""".stripMargin
    }

    case object Lemma4 extends Lemma {
      override val description: String =
        """
          | A node which receives the majority of positive votes becomes a coordinator and randomly elects a leader.
          |""".stripMargin
    }

    case object Lemma5 extends Lemma {
      override val description: String =
        """
          | A coordinator node receives Propose message sends Ack(COORDINATOR) (reject) to the sender.
          |""".stripMargin
    }

    case object Lemma6 extends Lemma {
      override val description: String =
        """
          | A node that already voted in the current round received Propose message
          | sends Ack(VOTED) (reject) to the sender.
          |""".stripMargin
    }

    case object Lemma7 extends Lemma {
      override val description: String =
        """
          | A node received a Timeout while waiting for a leader or to be become a coordinator resets its state.
          |""".stripMargin
    }

    case object Lemma8 extends Lemma {
      override val description: String =
        """
          | A node that received Announce become a leader.
          |""".stripMargin
    }

    case object Lemma9 extends Lemma {
      override val description: String =
        """
          | If the leader crashed the other nodes should start a new election round iff the cluster is complete
          | (the majority of nodes are alive).
          |""".stripMargin
    }

    case object Lemma10 extends Lemma {
      override val description: String =
        """
          | A node sent a Propose to any node in healthy cluster (alive leader) receives Ack(ELECTED) reject.
          |""".stripMargin
    }

    case object Lemma11 extends Lemma {
      override val description: String =
        """
          | Whenever a node joins a cluster it can update its leader from Heartbeat message iff cluster is complete
          | and heartbeat message was sent by the active leader.
          |""".stripMargin
    }

    case object Lemma12 extends Lemma {
      override val description: String =
        """
          | leader crash must trigger new election.
          |""".stripMargin
    }

  }

}
