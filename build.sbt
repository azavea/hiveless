import de.heikoseeberger.sbtheader._
import java.time.Year

val scalaVersions = Seq("2.12.15")

val catsVersion       = "2.7.0"
val shapelessVersion  = "2.3.3" // to be compatible with Spark 3.1.x
val scalaTestVersion  = "3.2.11"
val geomesaVersion    = "3.3.0"
val geotrellisVersion = "3.6.1+1-e4aeec2a-SNAPSHOT"

// GT depends on Shapeless 2.3.7
// To maintain better compat with Spark 3.1.x and DataBricks 9.1 we won't to depend on Shapeless 2.3.3
val excludedDependencies = List(
  ExclusionRule("com.chuusai", "shapeless_2.12"),
  ExclusionRule("com.chuusai", "shapeless_2.13")
)

def ver(for212: String, for213: String) = Def.setting {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, 12)) => for212
    case Some((2, 13)) => for213
    case _             => sys.error("not good")
  }
}

def spark(module: String) = Def.setting {
  "org.apache.spark" %% s"spark-$module" % ver("3.1.3", "3.2.1").value
}

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
  resolvers += "sonatype-snapshot" at "https://oss.sonatype.org/content/repositories/snapshots/"
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
  .aggregate(core, spatial, `spatial-index`)

lazy val core = project
  .settings(commonSettings)
  .settings(name := "hiveless-core")
  .settings(
    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),
    libraryDependencies ++= Seq(
      "org.typelevel"    %% "cats-core" % catsVersion,
      "com.chuusai"      %% "shapeless" % shapelessVersion,
      spark("hive").value % Provided,
      "org.scalatest"    %% "scalatest" % scalaTestVersion % Test
    )
  )

lazy val spatial = project
  .dependsOn(core % "compile->compile;provided->provided")
  .settings(commonSettings)
  .settings(name := "hiveless-spatial")
  .settings(
    libraryDependencies ++= Seq(
      "org.locationtech.geomesa" %% "geomesa-spark-jts" % geomesaVersion,
      "org.scalatest"            %% "scalatest"         % scalaTestVersion % Test
    )
  )

lazy val `spatial-index` = project
  .dependsOn(spatial % "compile->compile;provided->provided")
  .settings(commonSettings)
  .settings(name := "hiveless-spatial-index")
  .settings(
    libraryDependencies ++= Seq(
      "org.locationtech.geotrellis" %% "geotrellis-store" % geotrellisVersion excludeAll (excludedDependencies: _*),
      "org.scalatest"               %% "scalatest"        % scalaTestVersion % Test
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
