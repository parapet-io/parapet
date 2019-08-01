name := "parapet"

organization in ThisBuild := "io.parapet"

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
    // test
    val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
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
    core,
    testUtils,
    interopCats,
    intgTests,
    intgTestsCats,
    interopScalazZio,
    intgTestsScalazZio
  )

lazy val core = project
  .settings(
    name := "core",
    libraryDependencies ++= (commonDependencies ++ catsDependencies),
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.7",
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.7"
  )

lazy val testUtils = project
  .in(file("test-utils"))
  .settings(
    name := "test-utils",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.7"
  ).dependsOn(core)

lazy val intgTests = project
  .in(file("intg-tests"))
  .settings(
    name := "intg-tests",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "org.scalatest" %% "scalatest" % "3.0.7",
      "org.pegdown" % "pegdown" % "1.6.0",
      "net.logstash.logback" % "logstash-logback-encoder" % "5.3"
    ),
  ).dependsOn(core, testUtils)

// Cats Effect interop
lazy val intgTestsCats = project
  .in(file("intg-tests-cats"))
  .settings(
    name := "intg-tests-cats",
    scalacOptions += "-Xcheckinit"
  ).dependsOn(intgTests, interopCats)

lazy val interopCats = project
  .in(file("interop-cats"))
  .settings(
    name := "interop-cats"
  ).dependsOn(core)

lazy val interopScalazZio = project
  .in(file("interop-scalaz-zio"))
  .settings(
    name := "interop-scalaz-zio",
    libraryDependencies ++= Seq("org.scalaz" %% "scalaz-zio-interop-cats" % "1.0-RC5")
  ).dependsOn(core)

lazy val intgTestsScalazZio = project
  .in(file("intg-tests-scalaz-zio"))
  .settings(
    name := "intg-tests-scalaz-zio",
    scalacOptions += "-Xcheckinit"
  ).dependsOn(intgTests, interopScalazZio)

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
//todo
//testOptions in ThisBuild in Test += Tests.Argument("-l", "org.scalatest.tags.Slow")

def testUntilFailed = Command.args("testUntilFailed", "") { (state, args) =>
  val argsList = args.mkString(" ")
  s"testOnly $argsList" :: s"testUntilFailed $argsList" :: state
}

def testFast = Command.command("testFast") { state =>
  "testOnly -- -l org.scalatest.tags.Slow" :: state
}

commands += testUntilFailed
commands += testFast

parallelExecution in Test := false
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)