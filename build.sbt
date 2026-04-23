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

lazy val baseSettings = Seq(
  Test / parallelExecution := false
)

lazy val global = project
  .in(file("."))
  .aggregate(core, protocol, net, raft, demoColoring, intgTests)
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

lazy val protocol = project
  .in(file("protocol"))
  .settings(
    baseSettings,
    name := "parapet-protocol",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, flatPackage = true, scala3Sources = true) -> (Compile / sourceManaged).value / "scalapb"
    ),
    libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
  )

lazy val net = project
  .in(file("net"))
  .dependsOn(core % "compile->compile;test->test")
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

lazy val raft = project
  .in(file("raft"))
  .dependsOn(core % "compile->compile;test->test", protocol)
  .settings(
    baseSettings,
    name := "parapet-raft",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Test,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test
    )
  )

lazy val demoColoring = project
  .in(file("demo-coloring"))
  .dependsOn(core, protocol, net, raft)
  .settings(
    baseSettings,
    name := "parapet-demo-coloring",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.5.6",
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test
    )
  )

lazy val intgTests = project
  .in(file("intg-tests"))
  .dependsOn(core, protocol, net, raft)
  .settings(
    baseSettings,
    name := "parapet-intg-tests",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion,
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Test
    )
  )
