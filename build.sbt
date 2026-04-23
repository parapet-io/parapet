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

lazy val baseSettings = Seq(
  Test / parallelExecution := false
)

lazy val global = project
  .in(file("."))
  .aggregate(core, intgTests)
  .settings(
    publish / skip := true
  )

lazy val core = project
  .in(file("core"))
  .settings(
    baseSettings,
    name := "parapet-core",
    scalacOptions ++= Seq("-Xmax-inlines", "256"),
    libraryDependencies ++= Seq(
      "com.sksamuel.avro4s" %% "avro4s-core" % "5.0.13",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "com.lihaoyi" %% "sourcecode" % "0.4.2",
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Test,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test
    )
  )

lazy val intgTests = project
  .in(file("intg-tests"))
  .dependsOn(core)
  .settings(
    baseSettings,
    name := "parapet-intg-tests",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion,
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Test
    )
  )
