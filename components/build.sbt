import sbt.url

name := "components"

ThisBuild / organization := "io.parapet"
ThisBuild / organizationName := "parapet"
ThisBuild / organizationHomepage := Some(url("http://parapet.io/"))

val coreVersion = "0.0.1-RC2"
scalaVersion := "2.12.8"

scalacOptions in ThisBuild ++= Seq(
  "-Ypartial-unification",
  "-language:higherKinds",
  "-feature",
  "-deprecation"
)

useGpg := false

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credential")

libraryDependencies in ThisBuild += "io.parapet" %% "core" % coreVersion
libraryDependencies in ThisBuild += "io.monix" %% "monix-eval" % "3.0.0-RC3"
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

libraryDependencies in ThisBuild += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies in ThisBuild += "net.logstash.logback" % "logstash-logback-encoder" % "5.3"

lazy val global = project
  .in(file("."))
  .aggregate(
    algorithms,
    msgApi,
    msgZmq,
    msgIntgTests,
    algorithmsIntgTest
  )

// Algorithms
lazy val algorithms = project.in(file("./algorithms"))
  .settings(
    name := "algorithms"
  )

lazy val algorithmsIntgTest = project.in(file("./algorithms-intg-test"))
  .settings(
    name := "algorithms-intg-test",
    publishLocal := {},
    publish := {},
    libraryDependencies ++= Seq(
      "io.parapet" %% "interop-cats" % coreVersion,
      "io.parapet" %% "interop-scalaz-zio" % coreVersion,
      "io.parapet" %% "test-utils" % coreVersion
    )
  ).dependsOn(algorithms)

// Messaging components
lazy val msgApi = project.in(file("./messaging/api"))
  .settings(
    name := "messaging-api"
  )

lazy val msgZmq = project.in(file("./messaging/zmq"))
  .settings(
    name := "messaging-zmq"
  ).dependsOn(msgApi)

lazy val msgIntgTests = project.in(file("./messaging/intg-tests"))
  .settings(
    name := "messaging-intg-tests",
    publishLocal := {},
    publish := {},
    libraryDependencies ++= Seq(
      "io.parapet" %% "interop-cats" % coreVersion,
      "io.parapet" %% "interop-scalaz-zio" % coreVersion,
      "io.parapet" %% "test-utils" % coreVersion
    )
  ).dependsOn(msgApi, msgZmq)

publishArtifact in global := false
publishArtifact in algorithmsIntgTest := false
publishArtifact in msgIntgTests := false

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/parapet-io/parapet"),
    "scm:git@github.com:parapet-io/parapet.git"
  )
)

ThisBuild / developers := List(
  Developer(
    id    = "dmgcodevil",
    name  = "Roman Pleshkov",
    email = "dmgcodevil@gmail.com",
    url   = url("http://parapet.io/")
  )
)

ThisBuild / description := "A purely functional library to develop distributed and event driven systems."
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("http://parapet.io/"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true