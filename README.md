# Spark GHive

This tiny project adds GIS Hive UDFs.

## Usage example

```sql
CREATE OR REPLACE FUNCTION st_geomFromWKT AS 'com.azavea.ghive.jts.udf.ST_GeomFromWKT';
CREATE OR REPLACE FUNCTION st_makeBBOX AS 'com.azavea.ghive.jts.udf.ST_MakeBBOX';
CREATE OR REPLACE FUNCTION st_contains AS 'com.azavea.ghive.jts.udf.ST_Contains';

CREATE OR REPLACE FUNCTION st_geomFromWKT_V2 AS 'com.azavea.ghive.jts.udf.v2.ST_GeomFromWKT';
CREATE OR REPLACE FUNCTION st_makeBBOX_V2 AS 'com.azavea.ghive.jts.udf.v2.ST_MakeBBOX';
CREATE OR REPLACE FUNCTION st_contains_V2 AS 'com.azavea.ghive.jts.udf.v2.ST_Contains';
```
