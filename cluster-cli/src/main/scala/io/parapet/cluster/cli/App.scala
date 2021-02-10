package io.parapet.cluster.cli

object App {

  def main(args: Array[String]): Unit = {
    val node =
      new Node(host = "localhost", port = 8881, id = "node-1", server = Array("localhost:5555", "localhost:6666"))
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      node.close()
      println("node closed")
    }))

    node.connect()
    node.join("test")
    Thread.sleep(100000)
  }
}
