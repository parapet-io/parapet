package io.parapet.tests.intg.messaging

import org.scalatest._

trait BasicZMQSpec extends FunSuite with Retries {
  val retries = 5 // in the case if a port isn't free yet

  override def withFixture(test: NoArgTest): Outcome = {
    if (isRetryable(test)) withFixture(test, retries) else super.withFixture(test)
  }

  def withFixture(test: NoArgTest, count: Int): Outcome = {
    val outcome = super.withFixture(test)
    outcome match {
      case Failed(_) | Canceled(_) => if (count == 1) super.withFixture(test) else withFixture(test, count - 1)
      case other => other
    }
  }
}
