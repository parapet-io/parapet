package io.parapet.demo.coloring

/** Entry point of the demo-coloring web app.
  *
  * Boots a [[GraphColoringSimulation]], wraps it in a [[DemoHttpServer]], and prints the URL of the served UI. Blocks
  * until the JVM is interrupted.
  */
@main def runDistributedGraphLab(): Unit =
  val simulation = new GraphColoringSimulation()
  val server     = new DemoHttpServer(simulation)
  server.start()
  println(s"Parapet Distributed Graph Lab is running at ${server.url}")
  println("Press Ctrl+C to stop.")
