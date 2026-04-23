package io.parapet.demo.coloring

@main def runDistributedGraphLab(): Unit =
  val simulation = new GraphColoringSimulation()
  val server = new DemoHttpServer(simulation)
  server.start()
  println(s"Parapet Distributed Graph Lab is running at ${server.url}")
  println("Press Ctrl+C to stop.")
