ThisBuild / organization := "net.imadz"
ThisBuild / organizationName := "MadPolaris"
ThisBuild / organizationHomepage := Some(url("https://github.com/MadPolaris"))
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / versionScheme := Some("early-semver")

licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))

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

resolvers ++= Seq(
  "Aliyun" at "https://maven.aliyun.com/repository/public/"
)

Compile / scalacOptions ++= Seq("-target:11", "-deprecation", "-feature", "-unchecked", "-Xlint")
Compile / javacOptions ++= Seq("--release", "11")

libraryDependencies ++= Seq(
  "net.imadz" %% "monarch-core" % "0.2",
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-persistence-testkit" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)
