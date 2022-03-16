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
    "-feature",
    "-target:jvm-1.8" // ,
    // "-Xsource:3"
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
  ),
  resolvers ++= Seq(
    "apache-snapshots" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
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
      "com.chuusai"      %% "shapeless"  % "2.3.3", // to be compatible with Spark 3.1.x
      "org.apache.spark" %% "spark-hive" % "3.1.2" % Provided,
      "org.scalatest"    %% "scalatest"  % "3.2.11" % Test
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
    ),
    assembly / test := {},
    assembly / assemblyShadeRules := {
      val shadePackage = "com.azavea.shaded.hiveless"
      Seq(
        ShadeRule.rename("shapeless.**" -> s"$shadePackage.shapeless.@1").inAll,
        ShadeRule.rename("cats.kernel.**" -> s"$shadePackage.cats.kernel.@1").inAll
      )
    },
    assembly / assemblyMergeStrategy := {
      case s if s.startsWith("META-INF/services")           => MergeStrategy.concat
      case "reference.conf" | "application.conf"            => MergeStrategy.concat
      case "META-INF/MANIFEST.MF" | "META-INF\\MANIFEST.MF" => MergeStrategy.discard
      case "META-INF/ECLIPSEF.RSA" | "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
      case _                                                => MergeStrategy.first
    }
  )

lazy val geoparquet = project
  .settings(commonSettings)
  .settings(name := "hiveless-geoparquet")
  .settings(
    libraryDependencies ++= Seq(
      "org.locationtech.jts"         % "jts-core"            % "1.18.1",
      "org.locationtech.sfcurve"    %% "sfcurve-zorder"      % "0.2.2",
      "org.apache.spark"            %% "spark-core"          % "3.2.0",
      "org.apache.spark"            %% "spark-sql"           % "3.2.0",
      "org.apache.parquet"           % "parquet-avro"        % "1.13.0-SNAPSHOT",
      "org.apache.parquet"           % "parquet-arrow"       % "1.13.0-SNAPSHOT",
      "org.apache.parquet"           % "parquet-hadoop"      % "1.13.0-SNAPSHOT",
      "org.locationtech.geotrellis" %% "geotrellis-s3-spark" % "3.6.1",
      // "com.github.davidmoten" % "rtree" % "0.9",
      // "com.github.plokhotnyuk.rtree2d" %% "rtree2d-core" % "0.11.10",
      // conflicts with some deps, are these local?
      // "com.google.flatbuffers" % "flatbuffers-java" % "2.0.3",
      // "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.11",
      // "org.codehaus.jackson" % "jackson-core-asl" % "1.9.11",
      "org.scalatest" %% "scalatest" % "3.2.10" % Test
    )
  )
