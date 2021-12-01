# Hiveless

[![CI](https://github.com/azavea/hiveless/actions/workflows/ci.yml/badge.svg)](https://github.com/azavea/hiveless/actions/workflows/ci.yml)
[![Maven Badge](https://img.shields.io/maven-central/v/com.azavea/hiveless-core_2.12?color=blue)](https://search.maven.org/search?q=g:com.azavea%20and%20hiveless)
[![Snapshots Badge](https://img.shields.io/nexus/s/https/oss.sonatype.org/com.azavea/hiveless-core_2.12)](https://oss.sonatype.org/content/repositories/snapshots/com/azavea/hiveless-core_2.12/)

Hiveless is a Scala library for working with [Spark](https://spark.apache.org/) and [Hive](https://hive.apache.org/) using a more expressive typed API.
It adds typed HiveUDFs and implements Spatial Hive UDFs. It consists of the following modules:

* `hiveless-core` with the typed Hive UDFs API and the initial base set of codecs
* `hiveless-spatial` with Hive GIS UDFs (depends on [GeoMesa](https://github.com/locationtech/geomesa))

## Supported GIS functions (hiveless-spatial)

```sql
CREATE OR REPLACE FUNCTION st_area AS 'com.azavea.hiveless.spatial.ST_Area';
CREATE OR REPLACE FUNCTION st_asLatLonText AS 'com.azavea.hiveless.spatial.ST_AsLatLonText';
CREATE OR REPLACE FUNCTION st_asText AS 'com.azavea.hiveless.spatial.ST_AsText';
CREATE OR REPLACE FUNCTION st_contains AS 'com.azavea.hiveless.spatial.ST_Contains';
CREATE OR REPLACE FUNCTION st_centroid AS 'com.azavea.hiveless.spatial.ST_Centroid';
CREATE OR REPLACE FUNCTION st_covers AS 'com.azavea.hiveless.spatial.ST_Covers';
CREATE OR REPLACE FUNCTION st_crosses AS 'com.azavea.hiveless.spatial.ST_Crosses';
CREATE OR REPLACE FUNCTION st_difference AS 'com.azavea.hiveless.spatial.ST_Difference';
CREATE OR REPLACE FUNCTION st_disjoint AS 'com.azavea.hiveless.spatial.ST_Disjoint';
CREATE OR REPLACE FUNCTION st_equals AS 'com.azavea.hiveless.spatial.ST_Equals';
CREATE OR REPLACE FUNCTION st_exteriorRing AS 'com.azavea.hiveless.spatial.ST_ExteriorRing';
CREATE OR REPLACE FUNCTION st_geoHash AS 'com.azavea.hiveless.spatial.ST_GeoHash';
CREATE OR REPLACE FUNCTION st_isGeomField AS 'com.azavea.hiveless.spatial.ST_IsGeomField';
CREATE OR REPLACE FUNCTION st_geomFromWKT AS 'com.azavea.hiveless.spatial.ST_GeomFromWKT';
CREATE OR REPLACE FUNCTION st_intersection AS 'com.azavea.hiveless.spatial.ST_Intersection';
CREATE OR REPLACE FUNCTION st_intersects AS 'com.azavea.hiveless.spatial.ST_Intersects';
CREATE OR REPLACE FUNCTION st_makeBBOX AS 'com.azavea.hiveless.spatial.ST_MakeBBOX';
CREATE OR REPLACE FUNCTION st_makeLine AS 'com.azavea.hiveless.spatial.ST_MakeLine';
CREATE OR REPLACE FUNCTION st_numGeometries AS 'com.azavea.hiveless.spatial.ST_NumGeometries';
CREATE OR REPLACE FUNCTION st_numPoints AS 'com.azavea.hiveless.spatial.ST_NumPoints';
CREATE OR REPLACE FUNCTION st_overlaps AS 'com.azavea.hiveless.spatial.ST_Overlaps';
CREATE OR REPLACE FUNCTION st_simplify AS 'com.azavea.hiveless.spatial.ST_Simplify';
CREATE OR REPLACE FUNCTION st_simplifyPreserveTopology AS 'com.azavea.hiveless.spatial.ST_SimplifyPreserveTopology';
CREATE OR REPLACE FUNCTION st_touches AS 'com.azavea.hiveless.spatial.ST_Touches';
CREATE OR REPLACE FUNCTION st_within AS 'com.azavea.hiveless.spatial.ST_Within';
CREATE OR REPLACE FUNCTION st_x AS 'com.azavea.hiveless.spatial.ST_X';
CREATE OR REPLACE FUNCTION st_y AS 'com.azavea.hiveless.spatial.ST_Y';
```

## License
Code is provided under the Apache 2.0 license available at http://opensource.org/licenses/Apache-2.0,
as well as in the LICENSE file. This is the same license used as Spark.
