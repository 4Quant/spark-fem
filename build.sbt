import xerial.sbt.Sonatype.SonatypeKeys
import SonatypeKeys._

xerial.sbt.Sonatype.sonatypeSettings

name := "spark-fem"

version := "0.1-SNAPSHOT"

organization := "fourquant"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx_2.10" % "1.1.0"

libraryDependencies += "com.google.guava" % "guava" % "14.0.1"

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>https://github.com/4Quant/spark-fem</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:4quant/spark-fem.git</url>
    <connection>scm:git:git@github.com:4quant/spark-fem.git</connection>
  </scm>
  <developers>
    <developer>
      <id>kmader</id>
      <name>Kevin Mader</name>
      <url>https://github.com/kmader</url>
    </developer>
  </developers>)

// Enable Junit testing.
// libraryDependencies += "com.novocode" % "junit-interface" % "0.9" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"
