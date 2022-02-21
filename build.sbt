name := "parapet"

ThisBuild / organization := "io.parapet"
ThisBuild / organizationName := "parapet"
ThisBuild / organizationHomepage := Some(url("http://parapet.io/"))

ThisBuild / scalaVersion := "2.13.4"

ThisBuild / scalacOptions ++= Seq(
  "-language:higherKinds",
  "-feature",
  "-deprecation"
)

resolvers += Resolver.sonatypeRepo("releases")
ThisBuild / resolvers += "maven2" at "https://repo1.maven.org/maven2/"

useGpg := false

ThisBuild / credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credential")
ThisBuild / publishConfiguration := publishConfiguration.value.withOverwrite(true)
ThisBuild / publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

val scalaTestVersion = "3.2.1"

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
    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % Test
    val pegdown = "org.pegdown" % "pegdown" % "1.6.0" % Test
    val logstashLogbackEncoder = "net.logstash.logback" % "logstash-logback-encoder" % "5.3" % Test
    val logbackContrib = "ch.qos.logback.contrib" % "logback-json-classic" % "0.1.5" % Test
    val logbackJackson = "ch.qos.logback.contrib" % "logback-jackson" % "0.1.5" % Test
    val flexmark = "com.vladsch.flexmark" % "flexmark-all" % "0.36.8" % Test

    // utils
    val sourcecode = "com.lihaoyi" %% "sourcecode" % "0.2.1"
  }

ThisBuild / libraryDependencies += dependencies.pegdown

ThisBuild / libraryDependencies += compilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")
// if your project uses multiple Scala versions, use this for cross building
ThisBuild / libraryDependencies += compilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3" cross CrossVersion.binary)
// if your project uses both 2.10 and polymorphic lambdas
ThisBuild / libraryDependencies ++= (scalaBinaryVersion.value match {
  case "2.10" =>
    compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full) :: Nil
  case _ =>
    Nil
})

lazy val global = project
  .in(file("."))
  .aggregate(
    common,
    coreApi,
    core,
    net,
    cluster,
    clusterNode,
    protobuf,
    interopCats,
    interopMonix,
    testUtils,
    benchmark,
    spark)

lazy val core = project
  .settings(
    name := "core",
    libraryDependencies ++= (commonDependencies ++ catsDependencies),
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.7",
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.7",
    libraryDependencies += "io.monix" %% "monix-eval" % "3.3.0",
    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.1",
    libraryDependencies += "com.vladsch.flexmark" % "flexmark-all" % "0.36.8" % Test
  ).dependsOn(protobuf, coreApi, common)

lazy val cluster = project
  .enablePlugins(JavaAppPackaging, UniversalDeployPlugin)
  .settings(
    name := "cluster",
    libraryDependencies ++= Seq("com.github.scopt" %% "scopt" % "4.0.1",
      "org.slf4j" % "slf4j-log4j12" % "1.7.25"),
    Universal / maintainer := "parapet.io",
    Universal / packageName := "parapet-cluster-" + version.value,
    Universal / mappings += {
      val src = (sourceDirectory in Compile).value
      src / "resources" / "log4j.xml" -> "etc/log4j.xml"
    },
    Universal / mappings += {
      val src = (Compile / sourceDirectory).value
      src / "resources" / "etc" / "node.properties.template" -> "etc/node.properties.template"
    },
    bashScriptExtraDefines += """addJava "-Dlog4j.configuration=file:${app_home}/../etc/log4j.xml""""

  ).dependsOn(core, net, interopCats)

lazy val common = project
  .in(file("common"))
  .settings(
    name := "common",
  )

lazy val net = project
  .in(file("net"))
  .settings(
    name := "net",
    libraryDependencies += "org.zeromq" % "jeromq" % "0.5.1"
  ).dependsOn(core)

lazy val clusterNode = project
  .in(file("cluster-node"))
  .settings(
    name := "cluster-node",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3",
    libraryDependencies += "net.logstash.logback" % "logstash-logback-encoder" % "5.3"
  ).dependsOn(core, net, interopCats)

lazy val coordinatorNode = project
  .in(file("coordinator-node"))
  .settings(
    name := "coordinator-node",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3",
    libraryDependencies += "net.logstash.logback" % "logstash-logback-encoder" % "5.3"
  ).dependsOn(core, net, interopCats)

lazy val coreApi = project
  .in(file("core-api"))
  .settings(
    name := "core-api",
    libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.8.4",
    libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "4.0.10"
  ).dependsOn(common)

lazy val testUtils = project
  .in(file("test-utils"))
  .settings(
    name := "test-utils",
    libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion,
    libraryDependencies += dependencies.flexmark
  ).dependsOn(core)

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
      "org.scalatest" %% "scalatest" % scalaTestVersion,
      dependencies.flexmark,
      "org.pegdown" % "pegdown" % "1.6.0",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "net.logstash.logback" % "logstash-logback-encoder" % "5.3"
    ),
  ).dependsOn(core, testUtils, interopCats, interopMonix)

lazy val benchmark = project
  .in(file("benchmark"))
  .settings(
    name := "benchmark",
    publishLocal := {},
    publish := {},
  ).dependsOn(core, interopCats)

lazy val spark = project
  .in(file("spark"))
  .settings(
    name := "spark",
    libraryDependencies ++= Seq(
      "com.github.freva" % "ascii-table" % "1.2.0",
      "com.github.scopt" %% "scopt" % "4.0.1",
      "com.vladsch.flexmark" % "flexmark-all" % "0.36.8" % Test,
      dependencies.scalaTest),
  ).dependsOn(core, clusterNode)

lazy val sparkWorker = project
  .in(file("spark-worker"))
  .enablePlugins(JavaAppPackaging, UniversalDeployPlugin)
  .settings(
    name := "spark-worker",
    Compile / mainClass := Some("io.parapet.spark.WorkerApp"),
    Universal / maintainer := "parapet.io",
    Universal / packageName := "spark-worker-" + version.value,
    scriptClasspath := Seq("*")
    //    mappings in Universal += {
    //      val src = (sourceDirectory in Compile).value
    //      src / "resources" / "log4j.xml" -> "etc/log4j.xml"
    //    },
    //    mappings in Universal += {
    //      val src = (sourceDirectory in Compile).value
    //      src / "resources" / "etc" / "node.properties.template" -> "etc/node.properties.template"
    //    },
    //    bashScriptExtraDefines += """addJava "-Dlog4j.configuration=file:${app_home}/../etc/log4j.xml""""

  ).dependsOn(spark)

lazy val protobuf = project
  .settings(
    name := "protobuf",
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "compile"
    )
  )


protobuf / Compile / PB.protoSources := Seq(file("protobuf/src/main/protobuf"))
protobuf / Compile / PB.targets  in Compile := Seq(
  PB.gens.java -> (protobuf / Compile / sourceManaged).value
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
  dependencies.flexmark,
  dependencies.sourcecode
)

ThisBuild / Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports")
intgTests / Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-n", "io.parapet.testutils.tags.CatsTest")

def testUntilFailed = Command.args("testUntilFailed", "") { (state, args) =>
  val argsList = args.mkString(" ")
  s"testOnly $argsList" :: s"testUntilFailed $argsList" :: state
}

ThisBuild / commands += testUntilFailed

Test / parallelExecution := false
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

// PUBLISH TO MAVEN
global / publishArtifact := false
intgTests / publishArtifact := false

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