import Dependencies._
import Versions._

val sparkVersion = "2.0.0"

lazy val commonSettings: Seq[Def.Setting[_]] = Defaults.coreDefaultSettings ++ Seq(
  organization := "jobserver",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.10.6",
  resolvers ++= Dependencies.repos,
  credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
  dependencyOverrides += "org.scala-lang" % "scala-library" % scalaVersion.value,
  parallelExecution in Test := false,
  scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature"),
  // We need to exclude jms/jmxtools/etc because it causes undecipherable SBT errors  :(
  ivyXML :=
    <dependencies>
    <exclude module="jms"/>
    <exclude module="jmxtools"/>
    <exclude module="jmxri"/>
    </dependencies>
)

lazy val rootSettings = Seq(
  // Must run Spark tests sequentially because they compete for port 4040!
  parallelExecution in Test := false,

  // disable test for root project
  test := {}
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    rootSettings,
    name := "job",
    libraryDependencies ++= sparkDeps ++ typeSafeConfigDeps ++ sparkExtraDeps ++ coreTestDeps
      ++ jobserverDeps,
    libraryDependencies +="org.apache.spark" %% "spark-mllib" % sparkVersion,
    libraryDependencies +="org.slf4j" % "slf4j-api" % "1.7.5",
    libraryDependencies +="org.slf4j" % "slf4j-simple" % "1.7.5",

    test in assembly := {},
    fork in Test := true
)
