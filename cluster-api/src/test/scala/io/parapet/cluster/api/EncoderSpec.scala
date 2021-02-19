package io.parapet.cluster.api

import io.parapet.cluster.api.ClusterApi._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class EncoderSpec extends FunSuite {

  test("join") {
    val join = Join("1", "localhost:8080", "2")
    val data = encoder.write(join)
    encoder.read(data) shouldBe join
  }

}
