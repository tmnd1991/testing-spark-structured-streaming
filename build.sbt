import Settings.{ scalaDocOptionsVersion, scalacOptionsVersion }

inThisBuild(
  List(
    organization := Settings.organization,
    homepage := Some(url(s"https://github.com/tmnd1991/${Settings.name}")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "tmnd1991",
        "Antonio Murgia",
        "ing.murgia@icloud.com",
        url("http://github.com/tmnd1991")
      )
    )
  )
)

name := Settings.name
scalaVersion := Settings.scala
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % Provided
libraryDependencies += "org.scalatest"    %% "scalatest" % "3.0.1"
crossScalaVersions := Settings.crossScalaVersions
description := "Testing Spark Structured Streaming"
scalacOptions ++= scalacOptionsVersion(scalaVersion.value)
scalacOptions.in(Compile, doc) ++= scalaDocOptionsVersion(scalaVersion.value)
libraryDependencies.in(Compile, doc) += "org.apache.spark" %% "spark-sql" % "2.4.7"
