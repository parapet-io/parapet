name := "parapet"

ThisBuild / organization := "io.parapet"
ThisBuild / organizationName := "parapet"
ThisBuild / organizationHomepage := Some(url("http://parapet.io/"))

scalaVersion := "2.12.8"

scalacOptions in ThisBuild ++= Seq(
  "-Ypartial-unification",
  "-language:higherKinds",
  "-feature",
  "-deprecation"
)

useGpg := false

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credential")

lazy val dependencies =
  new {
    // Cats
    val catsEffect = "org.typelevel" %% "cats-effect" % "1.3.1"
    val catsFree = "org.typelevel" %% "cats-free" % "1.6.1"
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

lazy val Slow = config("slow").extend(Test)

lazy val global = project
  .in(file("."))
  .configs(Slow).settings(
  inConfig(Slow)(Defaults.testTasks))
  .aggregate(
    core,
    protobuf,
    interopCats,
    interopScalazZio,
    interopMonix,
    testUtils,
    intgTests,
   // algorithms,
   // msgApi,
   // msgZmq,
    //msgIntgTests,
   // algorithmsIntgTest,
    examples)

lazy val core = project
  .settings(
    name := "core",
    libraryDependencies ++= (commonDependencies ++ catsDependencies),
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.7",
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.7",
    libraryDependencies += "io.monix" %% "monix-eval" % "3.0.0-RC3",
    libraryDependencies += "io.parapet" % "p2p" % "1.0.0",
  ).dependsOn(protobuf)

lazy val examples = project
  .in(file("examples"))
  .settings(
    name := "examples",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3",
    libraryDependencies += "net.logstash.logback" % "logstash-logback-encoder" % "5.3"
  ).dependsOn(core, interopCats, interopScalazZio, interopMonix)

/*lazy val perfTesting = project
  .in(file("perf-testing"))
  .settings(
    name := "perf-testing",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3",
    libraryDependencies += "net.logstash.logback" % "logstash-logback-encoder" % "5.3"
  ).dependsOn(core, interopCats, interopScalazZio, interopMonix)*/

lazy val testUtils = project
  .in(file("test-utils"))
  .settings(
    name := "test-utils",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.7"
  ).dependsOn(core, interopCats, interopScalazZio, interopMonix)

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

lazy val interopMonix = project
  .in(file("interop-monix"))
  .settings(
    name := "interop-monix",
    libraryDependencies ++= Seq(
      "io.monix" %% "monix-eval" % "3.0.0-RC3")
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

// Algorithms
lazy val algorithms = project.in(file("./components/algorithms"))
  .settings(
    name := "algorithms"
  ).dependsOn(core)

lazy val algorithmsIntgTest = project.in(file("./components/algorithms-intg-test"))
  .settings(
    name := "algorithms-intg-test",
    publishLocal := {},
    publish := {}
  ).dependsOn(algorithms, testUtils)

// Messaging components
lazy val msgApi = project.in(file("./components/messaging/api"))
  .settings(
    name := "messaging-api"
  ).dependsOn(core)

lazy val msgZmq = project.in(file("./components/messaging/zmq"))
  .settings(
    name := "messaging-zmq"
  ).dependsOn(msgApi)

lazy val msgIntgTests = project.in(file("./components/messaging/intg-tests"))
  .settings(
    name := "messaging-intg-tests",
    publishLocal := {},
    publish := {}
  ).dependsOn(msgZmq, testUtils)

lazy val catsDependencies = Seq(dependencies.catsEffect, dependencies.catsFree)
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
testOptions in ThisBuild in Test += Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.scalatest.tags.Slow")

testOptions in ThisBuild in Slow -= Tests.Argument("-l", "org.scalatest.tags.Slow")
testOptions in ThisBuild in Slow += Tests.Argument("-n", "org.scalatest.tags.Slow")

def testUntilFailed = Command.args("testUntilFailed", "") { (state, args) =>
  val argsList = args.mkString(" ")
  s"testOnly $argsList" :: s"testUntilFailed $argsList" :: state
}

commands += testUntilFailed

parallelExecution in Test := false
parallelExecution in Slow := false
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

// PUBLISH TO MAVEN
publishArtifact in global := false
publishArtifact in intgTests := false
publishArtifact in examples := false
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