# Spark GHive

This tiny project that implements GIS Hive UDFs.

## Usage example

```sql
CREATE OR REPLACE FUNCTION st_area AS 'com.azavea.ghive.jts.udf.functions.ST_Area';
CREATE OR REPLACE FUNCTION st_contains AS 'com.azavea.ghive.jts.udf.functions.ST_Contains';
CREATE OR REPLACE FUNCTION st_covers AS 'com.azavea.ghive.jts.udf.functions.ST_Covers';
CREATE OR REPLACE FUNCTION st_crosses AS 'com.azavea.ghive.jts.udf.functions.ST_Crosses';
CREATE OR REPLACE FUNCTION st_difference AS 'com.azavea.ghive.jts.udf.functions.ST_Difference';
CREATE OR REPLACE FUNCTION st_disjoint AS 'com.azavea.ghive.jts.udf.functions.ST_Disjoint';
CREATE OR REPLACE FUNCTION st_equals AS 'com.azavea.ghive.jts.udf.functions.ST_Equals';
CREATE OR REPLACE FUNCTION st_exteriorRing AS 'com.azavea.ghive.jts.udf.functions.ST_ExteriorRing';
CREATE OR REPLACE FUNCTION st_geomFromWKT AS 'com.azavea.ghive.jts.udf.functions.ST_GeomFromWKT';
CREATE OR REPLACE FUNCTION st_geomToWKT AS 'com.azavea.ghive.jts.udf.functions.ST_GeomToWKT';
CREATE OR REPLACE FUNCTION st_intersection AS 'com.azavea.ghive.jts.udf.functions.ST_Intersection';
CREATE OR REPLACE FUNCTION st_intersects AS 'com.azavea.ghive.jts.udf.functions.ST_Intersects';
CREATE OR REPLACE FUNCTION st_makeBBOX AS 'com.azavea.ghive.jts.udf.functions.ST_MakeBBOX';
CREATE OR REPLACE FUNCTION st_numPoints AS 'com.azavea.ghive.jts.udf.functions.ST_NumPoints';
CREATE OR REPLACE FUNCTION st_overlaps AS 'com.azavea.ghive.jts.udf.functions.ST_Overlaps';
CREATE OR REPLACE FUNCTION st_simplify AS 'com.azavea.ghive.jts.udf.functions.ST_Simplify';
CREATE OR REPLACE FUNCTION st_simplifyPreserveTopology AS 'com.azavea.ghive.jts.udf.functions.ST_SimplifyPreserveTopology';
CREATE OR REPLACE FUNCTION st_touches AS 'com.azavea.ghive.jts.udf.functions.ST_Touches';
CREATE OR REPLACE FUNCTION st_within AS 'com.azavea.ghive.jts.udf.functions.ST_Within';
```
