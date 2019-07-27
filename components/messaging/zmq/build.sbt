name := "messaging-zmq"

val coreVersion = "0.0.1-RC1"

libraryDependencies += "org.zeromq" % "jeromq" % "0.5.1"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.7"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.7"
libraryDependencies += "io.parapet" %% "test-utils" % coreVersion % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.7" % Test
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
libraryDependencies += "net.logstash.logback" % "logstash-logback-encoder" % "5.3" % Test
parallelExecution in Test := false
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)