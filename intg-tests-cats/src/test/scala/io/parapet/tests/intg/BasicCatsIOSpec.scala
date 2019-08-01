package io.parapet.tests.intg

import cats.effect.{Concurrent, ContextShift, IO, Timer}
import io.parapet.core.Parallel
import io.parapet.tests.intg.instances.TestAppInstances
import io.parapet.testutils.{IntegrationSpec, TestApp}
import io.parapet.catsnstances.parallel._
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global

trait BasicCatsIOSpec extends IntegrationSpec[IO] with TestAppInstances {


  override val ec: ExecutionContext = global

  override implicit val ctxShift: ContextShift[IO] = IO.contextShift(ec)
  override val ct: Concurrent[IO] = Concurrent[IO]
  override  val pa: Parallel[IO] = implicitly[Parallel[IO]]
  override implicit val timer: Timer[IO] = IO.timer(ec)
  override val testApp: TestApp[IO] = testIOApp

  override def runSync(fa: IO[_]): Unit = fa.unsafeRunSync()
}
