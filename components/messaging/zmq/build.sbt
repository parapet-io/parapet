name := "messaging-zmq"

libraryDependencies += "org.zeromq" % "jeromq" % "0.5.1"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.7"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.7"
parallelExecution in Test := false
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)