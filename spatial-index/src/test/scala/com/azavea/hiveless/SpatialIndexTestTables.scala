/*
 * Copyright 2022 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.azavea.hiveless

trait SpatialIndexTestTables extends SpatialTestTables { self: SpatialHiveTestEnvironment =>
  override def createViews(): Unit =
    ssc.sql(
      """
        |CREATE TEMPORARY VIEW polygons_csv_view AS (
        |  SELECT *, ST_GeomFromWKT(wkt) AS geom, ST_ExtentFromGeom(ST_GeomFromWKT(wkt)) as bbox FROM polygons_csv
        |);
        |""".stripMargin
    )
}
