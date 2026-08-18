package io.parapet

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

class ProcessRefSpec extends AnyFunSuite:

  test("child references are stable and scoped by their parent") {
    val orders = ProcessRef.root[Event]("orders")
    val billing = ProcessRef.root[Event]("billing")

    orders.child[Event]("worker") shouldBe ProcessRef[Event]("orders/worker")
    orders.child[Event]("worker") shouldBe orders.child[Event]("worker")
    orders.child[Event]("worker") should not be billing.child[Event]("worker")
    orders.child[Event]("worker").child[Event]("retry-1") shouldBe
      ProcessRef[Event]("orders/worker/retry-1")
  }

  test("named reference segments must be canonical") {
    Seq("", "worker/child", "-worker", "worker child", "воркер").foreach { invalid =>
      assertThrows[IllegalArgumentException] {
        ProcessRef.root[Event](invalid)
      }
    }

    val root = ProcessRef.root[Event]("orders")
    Seq("", "worker/child", "-worker", "worker child", "воркер").foreach { invalid =>
      assertThrows[IllegalArgumentException] {
        root.child[Event](invalid)
      }
    }
  }
