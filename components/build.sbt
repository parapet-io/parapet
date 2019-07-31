
name := "components"

organization in ThisBuild := "io.parapet"
val coreVersion = "0.0.1-RC1"
scalaVersion := "2.12.8"

scalacOptions in ThisBuild ++= Seq(
  "-Ypartial-unification",
  "-language:higherKinds",
  "-feature",
  "-deprecation"
)
libraryDependencies in ThisBuild += "io.parapet" %% "core" % coreVersion
libraryDependencies in ThisBuild += "com.chuusai" %% "shapeless" % "2.3.3"
libraryDependencies in ThisBuild += "io.parapet" %% "test-utils" % coreVersion % Test
libraryDependencies in ThisBuild += "org.scalatest" %% "scalatest" % "3.0.7" % Test
libraryDependencies in ThisBuild += "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
libraryDependencies in ThisBuild += "net.logstash.logback" % "logstash-logback-encoder" % "5.3" % Test
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
    algorithms,
    msgApi,
    msgZmq
  )

// Algorithms
lazy val algorithms = project.in(file("./algorithms"))
  .settings(
    name := "algorithms"
  )

// Messaging components
lazy val msgApi = project.in(file("./messaging/api"))
  .settings(
    name := "messaging-api"
  )

lazy val msgZmq = project.in(file("./messaging/zmq"))
  .settings(
    name := "messaging-zmq"
  ).dependsOn(msgApi)