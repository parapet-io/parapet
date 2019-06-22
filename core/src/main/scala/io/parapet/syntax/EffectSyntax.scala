package io.parapet.syntax

import cats.effect.{Concurrent, Timer}
import cats.syntax.applicativeError._
import cats.syntax.flatMap._

import scala.concurrent.duration._

trait EffectSyntax {

  implicit class EffectOps[F[_] : Concurrent : Timer, A](fa: F[A]) {
    def retryWithBackoff(initialDelay: FiniteDuration, maxRetries: Int, backoffBase: Int = 2): F[A] =
      EffectOps.retryWithBackoff(fa, initialDelay, maxRetries, backoffBase)
  }

  object EffectOps {
    def retryWithBackoff[F[_], A](fa: F[A],
                                  initialDelay: FiniteDuration,
                                  maxRetries: Int, backoffBase: Int = 2)
                                 (implicit timer: Timer[F], ct: Concurrent[F]): F[A] = {
      fa.handleErrorWith { error =>
        if (maxRetries > 0)
          timer.sleep(initialDelay) >> retryWithBackoff(fa, initialDelay * backoffBase, maxRetries - 1)
        else
          ct.raiseError(error)
      }
    }

  }

}