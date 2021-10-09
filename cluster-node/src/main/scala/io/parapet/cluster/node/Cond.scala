package io.parapet.cluster.node

import cats.effect.Concurrent
import io.parapet.cluster.node.Cond._
import io.parapet.core.Dsl.DslF
import io.parapet.core.{Events, ProcessRef}
import io.parapet.core.api.Event

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.FiniteDuration

class Cond[F[_]: Concurrent](reply: ProcessRef, cond: Event => Boolean, timeout: FiniteDuration)
    extends io.parapet.core.Process[F] {

  import dsl._

  private val done = new AtomicBoolean()

  //override val ref: ProcessRef = ProcessRef("cond")

  override def handle: Receive = {
    case Start =>
      eval(println("Cond started")) ++
      fork(delay(timeout) ++ eval(println("Cond timeout")) ++ respond(Result(Option.empty)))
    case e if cond(e) =>
      eval(println(s"cond send success to $reply")) ++
      respond(Result(Option(e)))
    case e => withSender(sender => eval(println(s"Cond no match: $e, $sender")))
  }

  private def respond(res: Result): DslF[F, Unit] =
    for {
      success <- eval(done.compareAndSet(false, true))
      _ <- if (success) {
        eval(println(s"send reply to $reply")) ++ res ~> reply
      }else unit
    } yield ()
}

object Cond {

  case object Start extends Event
  // better types: Success and Timeout
  case class Result(event: Option[Event]) extends Event

}