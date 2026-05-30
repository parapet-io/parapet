name := "parapet"

ThisBuild / organization := "io.parapet"
ThisBuild / organizationName := "parapet"
ThisBuild / organizationHomepage := Some(url("https://parapet.io/"))
ThisBuild / homepage := Some(url("https://github.com/parapet-io/parapet"))
ThisBuild / scalaVersion := "3.3.4"
ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)
ThisBuild / licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer(
    id = "dmgcodevil",
    name = "dmgcodevil",
    email = "dmgcodevil@gmail.com",
    url = url("https://github.com/dmgcodevil")
  )
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    browseUrl = url("https://github.com/parapet-io/parapet"),
    connection = "scm:git:https://github.com/parapet-io/parapet.git",
    devConnection = Some("scm:git:git@github.com:parapet-io/parapet.git")
  )
)
ThisBuild / versionScheme := Some("early-semver")
Global / useGpgPinentry := false
ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}

val scalaTestVersion = "3.2.19"
val jeromqVersion = "0.6.0"
val aeronVersion = "1.50.3"
val catsEffectVersion = "3.5.7"

lazy val baseSettings = Seq(
  Test / parallelExecution := false
)

lazy val global = project
  .in(file("."))
  .aggregate(
    parapetCore,
    parapetTestkit,
    parapetCatsEffect,
    parapetPario,
    parapetProtocol,
    parapetNet
  )
  .settings(
    publish / skip := true
  )

lazy val parapetCore = project
  .in(file("parapet-core"))
  .settings(
    baseSettings,
    name := "parapet-core",
    scalacOptions ++= Seq("-Xmax-inlines", "256"),
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "com.lihaoyi" %% "sourcecode" % "0.4.2",
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Test,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test
    )
  )

lazy val schedulerStress = taskKey[Unit](
  "Run SchedulerStressSpec in an unbounded loop (ctrl-C to abort). " +
    "Tune via SCHEDULER_STRESS_ITERATIONS (0 = infinite) and SCHEDULER_STRESS_SEED env vars."
)

lazy val parapetTestkit = project
  .in(file("parapet-testkit"))
  .dependsOn(parapetCore)
  .settings(
    baseSettings,
    name := "parapet-testkit",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion,
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Test
    )
  )

lazy val parapetCatsEffect = project
  .in(file("parapet-cats-effect"))
  .dependsOn(parapetCore, parapetTestkit % "test->compile")
  .settings(
    baseSettings,
    name := "parapet-cats-effect",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Test,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test
    )
  )

lazy val parapetPario = project
  .in(file("parapet-pario"))
  .dependsOn(parapetCore, parapetTestkit % "test->compile")
  .settings(
    baseSettings,
    name := "parapet-pario",
    scalacOptions ++= Seq("-Xmax-inlines", "256"),
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Test,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test
    ),
    schedulerStress := {
      sys.props.getOrElseUpdate("scheduler.stress.iterations", "0")
      (Test / testOnly).toTask(" io.parapet.tests.intg.pario.SchedulerStressSpec").value
    }
  )

lazy val parapetProtocol = project
  .in(file("parapet-protocol"))
  .dependsOn(parapetCore)
  .settings(
    baseSettings,
    name := "parapet-protocol",
    scalacOptions ++= Seq("-Xmax-inlines", "256"),
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, flatPackage = true, scala3Sources = true) -> (Compile / sourceManaged).value / "scalapb"
    ),
    libraryDependencies ++= Seq(
      "com.sksamuel.avro4s" %% "avro4s-core" % "5.0.13",
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
    )
  )

lazy val parapetNet = project
  .in(file("parapet-net"))
  .dependsOn(
    parapetCore % "compile->compile;test->test",
    parapetProtocol,
    parapetTestkit % "test->compile",
    parapetCatsEffect % "test->test;test->compile"
  )
  .settings(
    baseSettings,
    name := "parapet-net",
    libraryDependencies ++= Seq(
      "org.zeromq" % "jeromq" % jeromqVersion,
      "io.aeron" % "aeron-all" % aeronVersion,
      "com.sksamuel.avro4s" %% "avro4s-core" % "5.0.13",
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "org.typelevel" %% "cats-effect" % catsEffectVersion % Test,
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Test,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test
    ),
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, flatPackage = true, scala3Sources = true) -> (Compile / sourceManaged).value / "scalapb"
    ),
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.util.zip=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
