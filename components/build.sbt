
name := "components"

organization in ThisBuild := "io.parapet"

scalaVersion := "2.12.8"

scalacOptions in ThisBuild ++= Seq(
  "-Ypartial-unification",
  "-language:higherKinds",
  "-feature",
  "-deprecation"
)
libraryDependencies in ThisBuild += "io.parapet" %% "core" % "0.0.1-RC1"
libraryDependencies in ThisBuild += "com.chuusai" %% "shapeless" % "2.3.3"
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
    api,
    zmq
  )

// Messaging components
lazy val api = project.in(file("./messaging/api"))
  .settings(
    name := "messaging-api"
  )

lazy val zmq = project.in(file("./messaging/zmq"))
  .settings(
    name := "messaging-zmq"
  ).dependsOn(api)