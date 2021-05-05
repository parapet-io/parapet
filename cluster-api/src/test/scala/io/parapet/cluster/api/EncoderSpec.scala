package io.parapet.cluster.api

import io.parapet.cluster.api.ClusterApi._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class EncoderSpec extends AnyFunSuite {

  test("join") {
    val join = Join("1", "localhost:8080", "2")
    val data = encoder.write(join)
    encoder.read(data) shouldBe join
  }

}
