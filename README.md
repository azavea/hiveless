# Hiveless

[![CI](https://github.com/azavea/hiveless/actions/workflows/ci.yml/badge.svg)](https://github.com/azavea/hiveless/actions/workflows/ci.yml)
[![Maven Badge](https://img.shields.io/maven-central/v/com.azavea/hiveless-core_2.12?color=blue)](https://search.maven.org/search?q=g:com.azavea%20and%20hiveless)
[![Snapshots Badge](https://img.shields.io/nexus/s/https/oss.sonatype.org/com.azavea/hiveless-core_2.12)](https://oss.sonatype.org/content/repositories/snapshots/com/azavea/hiveless-core_2.12/)

Hiveless is a Scala library for working with [Spark](https://spark.apache.org/) and [Hive](https://hive.apache.org/) using a more expressive typed API.
It adds typed HiveUDFs and implements Spatial Hive UDFs. It consists of the following modules:

* `hiveless-core` with the typed Hive UDFs API and the initial base set of codecs
* `hiveless-jts` with the TWKB JTS encoding support
* `hiveless-spatial` with Hive GIS UDFs (depends on [GeoMesa](https://github.com/locationtech/geomesa))
* `hiveless-spatial-index` with extra Hive GIS UDFs that may be used for the GIS indexing purposes (depends on [GeoMesa](https://github.com/locationtech/geomesa) and [GeoTrellis](https://github.com/locationtech/geotrellis))
  * There is also a forked release [CartoDB/analytics-toolbox-databricks](https://github.com/CartoDB/analytics-toolbox-databricks), which is a complete `hiveless-spatial` and `hiveless-spatial-index` copy at this point. However, it may contain an extended GIS functionality in the future.

## Quick Start

To use Hiveless in your project add the following in your `build.sbt` file as needed:

```scala
resolvers ++= Seq(
  // for snapshot artifacts only
  "oss-sonatype" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= List(
  "com.azavea" %% "hiveless-core"          % "<latest version>",
  "com.azavea" %% "hiveless-spatial"       % "<latest version>",
  "com.azavea" %% "hiveless-spatial-index" % "<latest version>"
)
```

## Hiveless Spatial supported GIS functions

```sql
CREATE OR REPLACE FUNCTION st_geometryFromText as 'com.azavea.hiveless.spatial.ST_GeomFromWKT';
CREATE OR REPLACE FUNCTION st_intersects as 'com.azavea.hiveless.spatial.ST_Intersects';
CREATE OR REPLACE FUNCTION st_simplify as 'com.azavea.hiveless.spatial.ST_Simplify';
 -- ...and more
```

The full list of supported functions can be found [here](./spatial/sql/createUDFs.sql).

## Spatial Query optimizations

```scala
import com.azavea.hiveless.spark.sql.rules.SpatialFilterPushdownRules

val spark: SparkSession = ???
SpatialFilterPushdownRules.registerOptimizations(sparkContext.sqlContext)
```

## License
Code is provided under the Apache 2.0 license available at http://opensource.org/licenses/Apache-2.0,
as well as in the LICENSE file. This is the same license used as Spark.
