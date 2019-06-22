name := "parapet"

organization in ThisBuild := "io.parapet"

version in ThisBuild := "0.0.1-DONOTUSE"

scalaVersion := "2.12.8"

scalacOptions in ThisBuild ++= Seq(
  "-Ypartial-unification",
  "-language:higherKinds",
  "-feature",
  "-deprecation"
)


lazy val dependencies =
  new {
    // Cats
    val catsEffect = "org.typelevel" %% "cats-effect" % "1.3.1"
    val catsFree = "org.typelevel" %% "cats-free" % "1.6.1"
    val fs2Core = "co.fs2" %% "fs2-core" % "1.0.5"
    // Shapless
    val shapeless = "com.chuusai" %% "shapeless" % "2.3.3"
    // logging
    val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
    val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.2.3"
    // test
    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % Test
    val pegdown = "org.pegdown" % "pegdown" % "1.6.0" % Test
    val logstashLogbackEncoder = "net.logstash.logback" % "logstash-logback-encoder" % "5.3" % Test
  }

libraryDependencies in ThisBuild += dependencies.pegdown

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
    core
  )


lazy val core = project
  .settings(
    name := "core",
    libraryDependencies ++= (commonDependencies ++ catsDependencies)
  )


lazy val catsDependencies = Seq(dependencies.catsEffect, dependencies.catsFree, dependencies.fs2Core)
lazy val commonDependencies = Seq(
  dependencies.shapeless,
  dependencies.scalaLogging,
  dependencies.logbackClassic,
  dependencies.logstashLogbackEncoder,
  dependencies.scalaTest)

resolvers += Resolver.sonatypeRepo("releases")

addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.0")

// if your project uses multiple Scala versions, use this for cross building
addCompilerPlugin("org.typelevel" % "kind-projector" % "0.10.0" cross CrossVersion.binary)



testOptions in ThisBuild in Test += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports")
testOptions in ThisBuild in Test += Tests.Argument("-l", "org.scalatest.tags.Slow")

def testUntilFailed = Command.args("testUntilFailed", "") { (state, args) =>
  val argsList = args.mkString(" ")
  s"testOnly $argsList" :: s"testUntilFailed $argsList" :: state
}

commands += testUntilFailed

parallelExecution in Test := false
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)