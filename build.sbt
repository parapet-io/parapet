name := "parapet"

ThisBuild / organization := "io.parapet"
ThisBuild / organizationName := "parapet"
ThisBuild / organizationHomepage := Some(url("http://parapet.io/"))

scalaVersion := "2.13.4"

scalacOptions in ThisBuild ++= Seq(
  "-Ypartial-unification",
  "-language:higherKinds",
  "-feature",
  "-deprecation"
)

resolvers += Resolver.sonatypeRepo("releases")
resolvers in ThisBuild += "maven2" at "https://repo1.maven.org/maven2/"

useGpg := false

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credential")

lazy val dependencies =
  new {
    // Cats
    val catsEffect = "org.typelevel" %% "cats-effect" % "2.3.1"
    val catsFree = "org.typelevel" %% "cats-free" % "2.3.1"
    // Shapless
    val shapeless = "com.chuusai" %% "shapeless" % "2.3.3"
    // logging
    val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
    // test
    val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % Test
    val pegdown = "org.pegdown" % "pegdown" % "1.6.0" % Test
    val logstashLogbackEncoder = "net.logstash.logback" % "logstash-logback-encoder" % "5.3" % Test
    val logbackContrib = "ch.qos.logback.contrib" % "logback-json-classic" % "0.1.5" % Test
    val logbackJackson = "ch.qos.logback.contrib" % "logback-jackson" % "0.1.5" % Test
    // utils
    val sourcecode = "com.lihaoyi" %% "sourcecode" % "0.2.1"
  }

libraryDependencies in ThisBuild += dependencies.pegdown

libraryDependencies in ThisBuild += compilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")
// if your project uses multiple Scala versions, use this for cross building
libraryDependencies in ThisBuild += compilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3" cross CrossVersion.binary)
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
    protobuf,
    interopCats,
    interopMonix,
    testUtils,
    intgTests)

lazy val core = project
  .settings(
    name := "core",
    libraryDependencies ++= (commonDependencies ++ catsDependencies),
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.7",
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.7",
    libraryDependencies += "io.monix" %% "monix-eval" % "3.3.0",
    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.1",
    libraryDependencies += "org.zeromq" % "jeromq" % "0.5.1"
  ).dependsOn(protobuf)

lazy val testUtils = project
  .in(file("test-utils"))
  .settings(
    name := "test-utils",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.7"
  ).dependsOn(core, interopCats, interopMonix)

lazy val interopCats = project
  .in(file("interop-cats"))
  .settings(
    name := "interop-cats"
  ).dependsOn(core)

// todo uncomment once scalaz-zio-interop-cats supports scala 2.13
//lazy val interopScalazZio = project
//  .in(file("interop-scalaz-zio"))
//  .settings(
//    name := "interop-scalaz-zio",
//    libraryDependencies ++= Seq("org.scalaz" %% "scalaz-zio-interop-cats" % "1.0-RC5")
//  ).dependsOn(core)

lazy val interopMonix = project
  .in(file("interop-monix"))
  .settings(
    name := "interop-monix",
    libraryDependencies ++= Seq(
      "io.monix" %% "monix-eval" % "3.3.0")
  ).dependsOn(core)

lazy val intgTests = project
  .in(file("intg-tests"))
  .settings(
    name := "intg-tests",
    publishLocal := {},
    publish := {},
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.7",
      "org.pegdown" % "pegdown" % "1.6.0",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "net.logstash.logback" % "logstash-logback-encoder" % "5.3"
    ),
  ).dependsOn(core, testUtils)

lazy val protobuf = project
  .settings(
    name := "protobuf",
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "compile"
    )
  )


PB.protoSources in protobuf in Compile := Seq(file("protobuf/src/main/protobuf"))
PB.targets in protobuf in Compile := Seq(
  PB.gens.java -> (sourceManaged in protobuf in Compile).value
)

lazy val catsDependencies = Seq(dependencies.catsEffect, dependencies.catsFree)
lazy val commonDependencies = Seq(
  dependencies.shapeless,
  dependencies.scalaLogging,
  dependencies.logbackClassic,
  dependencies.logstashLogbackEncoder,
  dependencies.logbackContrib,
  dependencies.logbackJackson,
  dependencies.scalaTest,
  dependencies.sourcecode
)

testOptions in ThisBuild in Test += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports")

def testUntilFailed = Command.args("testUntilFailed", "") { (state, args) =>
  val argsList = args.mkString(" ")
  s"testOnly $argsList" :: s"testUntilFailed $argsList" :: state
}

ThisBuild / commands += testUntilFailed

parallelExecution in Test := false
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

// PUBLISH TO MAVEN
publishArtifact in global := false
publishArtifact in intgTests := false

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/parapet-io/parapet"),
    "scm:git@github.com:parapet-io/parapet.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id = "dmgcodevil",
    name = "Roman Pleshkov",
    email = "dmgcodevil@gmail.com",
    url = url("http://parapet.io/")
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