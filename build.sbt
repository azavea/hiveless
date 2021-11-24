import de.heikoseeberger.sbtheader._
import java.time.Year

name         := "spark-ghive"
version      := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.15"
organization := "com.azavea"
scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:higherKinds",
  "-language:postfixOps",
  "-language:existentials",
  "-feature"
)
licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
homepage := Some(url("https://github.com/azavea/spark-ghive"))
developers := List(
  Developer(
    "pomadchin",
    "Grigory Pomadchin",
    "@pomadchin",
    url("https://github.com/pomadchin")
  )
)
versionScheme := Some("semver-spec")

addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full)

libraryDependencies ++= Seq(
  "org.typelevel"            %% "cats-core"         % "2.6.1",
  "org.locationtech.geomesa" %% "geomesa-spark-jts" % "3.3.0",
  "com.chuusai"              %% "shapeless"         % "2.3.7",
  "org.apache.spark"         %% "spark-hive"        % "3.1.2" % Provided
)

headerLicense := Some(HeaderLicense.ALv2(Year.now.getValue.toString, "Azavea"))
headerMappings := Map(
  FileType.scala -> CommentStyle.cStyleBlockComment.copy(
    commentCreator = { (text, existingText) =>
      // preserve year of old headers
      val newText = CommentStyle.cStyleBlockComment.commentCreator.apply(text, existingText)
      existingText.flatMap(_ => existingText.map(_.trim)).getOrElse(newText)
    }
  )
)
