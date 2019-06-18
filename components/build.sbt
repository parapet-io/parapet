
name := "components"

organization in ThisBuild := "io.parapet"

scalaVersion := "2.12.8"

scalacOptions in ThisBuild ++= Seq(
  "-Ypartial-unification",
  "-language:higherKinds",
  "-feature",
  "-deprecation"
)
libraryDependencies in ThisBuild += "io.parapet" %% "core" % "0.0.1-DONOTUSE"

libraryDependencies in ThisBuild += compilerPlugin("org.typelevel" %% "kind-projector" % "0.10.0")
// if your project uses multiple Scala versions, use this for cross building
libraryDependencies in ThisBuild += compilerPlugin("org.typelevel" % "kind-projector" % "0.10.0" cross CrossVersion.binary)
// if your project uses both 2.10 and polymorphic lambdas
libraryDependencies in ThisBuild ++= (scalaBinaryVersion.value match {
  case "2.10" =>
    compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full) :: Nil
  case _ =>
    Nil
})
lazy val global = project
  .in(file("."))
  .aggregate(
    zmqTcpClient, zmqTcpServer
  )


// Network components
lazy val zmqTcpClient = project.in(file("./network/zmq-tcp-client"))
  .settings(
    name := "zmq-tcp-client"
  )

lazy val zmqTcpServer = project.in(file("./network/zmq-tcp-server"))
  .settings(
    name := "zmq-tcp-server"
  )
