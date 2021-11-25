# Hiveless

Hiveless is a Scala library for working with Spark and Hive using a more expressive typed API.
It adds typed HiveUDFs and implements GIS Hive UDFs.

## Supported GIS functions

```sql
CREATE OR REPLACE FUNCTION st_area AS 'com.azavea.hiveless.spatial.ST_Area';
CREATE OR REPLACE FUNCTION st_contains AS 'com.azavea.hiveless.spatial.ST_Contains';
CREATE OR REPLACE FUNCTION st_covers AS 'com.azavea.hiveless.spatial.ST_Covers';
CREATE OR REPLACE FUNCTION st_crosses AS 'com.azavea.hiveless.spatial.ST_Crosses';
CREATE OR REPLACE FUNCTION st_difference AS 'com.azavea.hiveless.spatial.ST_Difference';
CREATE OR REPLACE FUNCTION st_disjoint AS 'com.azavea.hiveless.spatial.ST_Disjoint';
CREATE OR REPLACE FUNCTION st_equals AS 'com.azavea.hiveless.spatial.ST_Equals';
CREATE OR REPLACE FUNCTION st_exteriorRing AS 'com.azavea.hiveless.spatial.ST_ExteriorRing';
CREATE OR REPLACE FUNCTION st_geomFromWKT AS 'com.azavea.hiveless.spatial.ST_GeomFromWKT';
CREATE OR REPLACE FUNCTION st_geomToWKT AS 'com.azavea.hiveless.spatial.ST_GeomToWKT';
CREATE OR REPLACE FUNCTION st_intersection AS 'com.azavea.hiveless.spatial.ST_Intersection';
CREATE OR REPLACE FUNCTION st_intersects AS 'com.azavea.hiveless.spatial.ST_Intersects';
CREATE OR REPLACE FUNCTION st_makeBBOX AS 'com.azavea.hiveless.spatial.ST_MakeBBOX';
CREATE OR REPLACE FUNCTION st_numPoints AS 'com.azavea.hiveless.spatial.ST_NumPoints';
CREATE OR REPLACE FUNCTION st_overlaps AS 'com.azavea.hiveless.spatial.ST_Overlaps';
CREATE OR REPLACE FUNCTION st_simplify AS 'com.azavea.hiveless.spatial.ST_Simplify';
CREATE OR REPLACE FUNCTION st_simplifyPreserveTopology AS 'com.azavea.hiveless.spatial.ST_SimplifyPreserveTopology';
CREATE OR REPLACE FUNCTION st_touches AS 'com.azavea.hiveless.spatial.ST_Touches';
CREATE OR REPLACE FUNCTION st_within AS 'com.azavea.hiveless.spatial.ST_Within';
```

## License
Code is provided under the Apache 2.0 license available at http://opensource.org/licenses/Apache-2.0,
as well as in the LICENSE file. This is the same license used as Spark.
