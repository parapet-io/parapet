name := "parapet"

ThisBuild / organization := "io.parapet"
ThisBuild / organizationName := "parapet"
ThisBuild / organizationHomepage := Some(url("https://parapet.io/"))
ThisBuild / scalaVersion := "3.3.4"
ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)

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
  .dependsOn(parapetCore % "compile->compile;test->test", parapetProtocol)
  .settings(
    baseSettings,
    name := "parapet-net",
    libraryDependencies ++= Seq(
      "org.zeromq" % "jeromq" % jeromqVersion,
      "io.aeron" % "aeron-all" % aeronVersion,
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Test,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test
    )
  )
