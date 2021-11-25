import de.heikoseeberger.sbtheader._
import java.time.Year

val scalaVersions = Seq("2.12.15")

lazy val commonSettings = Seq(
  scalaVersion       := scalaVersions.head,
  crossScalaVersions := scalaVersions,
  organization       := "com.azavea",
  scalacOptions ++= Seq(
    "-deprecation",
    "-unchecked",
    "-language:implicitConversions",
    "-language:reflectiveCalls",
    "-language:higherKinds",
    "-language:postfixOps",
    "-language:existentials",
    "-feature"
  ),
  licenses               := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  homepage               := Some(url("https://github.com/azavea/hiveless")),
  versionScheme          := Some("semver-spec"),
  Test / publishArtifact := false,
  developers := List(
    Developer(
      "pomadchin",
      "Grigory Pomadchin",
      "@pomadchin",
      url("https://github.com/pomadchin")
    )
  ),
  headerLicense := Some(HeaderLicense.ALv2(Year.now.getValue.toString, "Azavea")),
  headerMappings := Map(
    FileType.scala -> CommentStyle.cStyleBlockComment.copy(
      commentCreator = { (text, existingText) =>
        // preserve year of old headers
        val newText = CommentStyle.cStyleBlockComment.commentCreator.apply(text, existingText)
        existingText.flatMap(_ => existingText.map(_.trim)).getOrElse(newText)
      }
    )
  )
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(name := "hiveless")
  .settings(
    scalaVersion       := scalaVersions.head,
    crossScalaVersions := Nil,
    publish            := {},
    publishLocal       := {}
  )
  .aggregate(core, spatial)

lazy val core = project
  .settings(commonSettings)
  .settings(name := "hiveless-core")
  .settings(
    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),
    libraryDependencies ++= Seq(
      "org.typelevel"    %% "cats-core"  % "2.6.1",
      "com.chuusai"      %% "shapeless"  % "2.3.7",
      "org.apache.spark" %% "spark-hive" % "3.1.2"  % Provided,
      "org.scalatest"    %% "scalatest"  % "3.2.10" % Test
    )
  )

lazy val spatial = project
  .dependsOn(core % "compile->compile;provided->provided")
  .settings(commonSettings)
  .settings(name := "hiveless-spatial")
  .settings(
    libraryDependencies ++= Seq(
      "org.locationtech.geomesa" %% "geomesa-spark-jts" % "3.3.0",
      "org.scalatest"            %% "scalatest"         % "3.2.10" % Test
    )
  )
