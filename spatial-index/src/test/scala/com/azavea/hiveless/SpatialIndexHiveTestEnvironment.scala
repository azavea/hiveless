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

import org.apache.spark.sql.hive.hiveless.spatial.rules.SpatialFilterPushdownRules
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.io.File
import scala.io.Source

trait SpatialIndexHiveTestEnvironment extends SpatialHiveTestEnvironment { self: Suite with BeforeAndAfterAll =>
  import SpatialHiveTestEnvironment._

  private def spatialFunctions: List[String] =
    Source
      .fromFile(new File("../spatial/sql/createUDFs.sql").toURI)
      .using(_.mkString.split(";").toList.map(_.trim).filter(_.nonEmpty))

  private def spatialIndexFunctions: List[String] =
    Source
      .fromFile(new File("../spatial-index/sql/createUDFs.sql").toURI)
      .using(_.mkString.split(";").toList.map(_.trim).filter(_.nonEmpty))

  // function to override Hive SQL functions registration
  override def registerHiveUDFs(ssc: SparkSession): Unit =
    (spatialFunctions ::: spatialIndexFunctions).foreach(ssc.sql)

  // function to override optimizations
  override def registerOptimizations(sqlContext: SQLContext): Unit =
    SpatialFilterPushdownRules.registerOptimizations(sqlContext)
}
