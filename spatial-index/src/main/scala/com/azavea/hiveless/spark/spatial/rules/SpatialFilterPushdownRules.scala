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

package com.azavea.hiveless.spark.spatial.rules

import com.azavea.hiveless.spark.rules.syntax._
import com.azavea.hiveless.spatial._
import com.azavea.hiveless.spatial.index._
import com.azavea.hiveless.spatial.index.ST_IntersectsExtent
import com.azavea.hiveless.serializers.syntax._
import geotrellis.vector._
import cats.syntax.option._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.hive.HivelessInternals.HiveGenericUDF
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.log4s.getLogger

import scala.util.Try

object SpatialFilterPushdownRules extends Rule[LogicalPlan] {
  @transient private[this] lazy val logger = getLogger

  def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformDown {
      // HiveGenericUDF is a private[hive] case class
      case f @ Filter(condition: HiveGenericUDF, plan) if condition.of[ST_IntersectsExtent] =>
        try {
          // ST_IntersectsExtent is NOT polymorphic by the first argument
          // Optimization is done with an assumption that it is always Extent
          val Seq(extentExpr, geometryExpr) = condition.children
          // extract extent from the right
          // ST_IntersectsExtent is polymorphic by the second argument
          // the second argument can be Geometry or extent
          val extent = {
            val g = geometryExpr.eval(null)
            Try(g.convert[Geometry].extent).getOrElse(g.convert[Extent])
          }

          // transform expression
          val expr = AndList(
            List(
              IsNotNull(extentExpr),
              GreaterThanOrEqual(GetStructField(extentExpr, 0, "xmin".some), Literal(extent.xmin)),
              GreaterThanOrEqual(GetStructField(extentExpr, 1, "ymin".some), Literal(extent.ymin)),
              LessThanOrEqual(GetStructField(extentExpr, 2, "xmax".some), Literal(extent.xmax)),
              LessThanOrEqual(GetStructField(extentExpr, 3, "ymax".some), Literal(extent.ymax))
            )
          )

          Filter(expr, plan)
        } catch {
          // fallback to the unoptimized node if optimization failed
          case e: Throwable =>
            logger.warn(
              s"""
                 |${this.getClass.getName}:: ${classOf[ST_IntersectsExtent]} optimization failed: ${e.getMessage}
                 |StackTrace: ${ExceptionUtils.getStackTrace(e)}
                 |""".stripMargin
            )
            f
        }
    }

  def registerOptimizations(sqlContext: SQLContext): Unit =
    Seq(SpatialFilterPushdownRules).foreach { r =>
      if (!sqlContext.experimental.extraOptimizations.contains(r))
        sqlContext.experimental.extraOptimizations ++= Seq(r)
    }
}
