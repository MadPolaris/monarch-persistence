ThisBuild / organization := "net.imadz"
ThisBuild / organizationName := "MadPolaris"
ThisBuild / organizationHomepage := Some(url("https://github.com/MadPolaris"))
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / versionScheme := Some("early-semver")

licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
Compile / packageBin / mappings += baseDirectory.value / "LICENSE" -> "META-INF/LICENSE"
Compile / packageSrc / mappings += baseDirectory.value / "LICENSE" -> "META-INF/LICENSE"

ThisBuild / homepage := Some(url("https://github.com/MadPolaris/monarch-persistence"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/MadPolaris/monarch-persistence"),
    "git@github.com:MadPolaris/monarch-persistence.git"
  ))
ThisBuild / developers := List(
  Developer(
    "MadPolaris",
    "MadPolaris Team",
    "zhongdj@gmail.com",
    url("https://github.com/MadPolaris")
  ))
ThisBuild / sonatypeCredentialHost := "central.sonatype.com"
// Version is derived from git tags by sbt-dynver (via sbt-ci-release):
// push a tag like v0.2 to release that version. Lockstep with the family.

val akkaVersion = "2.6.20"

mimaPreviousArtifacts := Set("net.imadz" %% "monarch-persistence" % "0.2")

resolvers ++= sys.props.get("monarch.core.repository").toSeq.map("monarch-candidate" at _)

resolvers ++= Seq(
  "Aliyun" at "https://maven.aliyun.com/repository/public/"
)

Compile / scalacOptions ++= Seq("-target:11", "-deprecation", "-feature", "-unchecked", "-Xlint")
Compile / javacOptions ++= Seq("--release", "11")

Compile / PB.targets := Seq(scalapb.gen() -> (Compile / sourceManaged).value / "protobuf")

libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.14" % "protobuf",
  "net.imadz" %% "monarch-core" % sys.props.getOrElse("monarch.core.version", "0.2.1"),
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-persistence-testkit" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.13" % Test
)
