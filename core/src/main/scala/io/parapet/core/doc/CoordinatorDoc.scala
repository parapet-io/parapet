package io.parapet.core.doc

object CoordinatorDoc {
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
  }
}
