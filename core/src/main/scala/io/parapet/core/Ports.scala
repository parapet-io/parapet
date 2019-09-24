package io.parapet.core

// todo temp solution, remove
class Ports(available: java.util.concurrent.ConcurrentLinkedQueue[Int]) {

  def take: Int = available.poll()

  def release(port: Int) = available.add(port)

}


object Ports {
  //  inclusive
  def apply(from: Int, to: Int): Ports = {
    val q = new java.util.concurrent.ConcurrentLinkedQueue[Int]
    (from to to).foreach { i =>
      q.add(i)
    }
    new Ports(q)
  }
}



