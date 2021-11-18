/*
 * Copyright 2021 Azavea
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

package com.azavea.ghive.jts.udf.v2

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hive.HiveInspectorsExposed
import org.apache.spark.sql.jts.GeometryUDT
import org.locationtech.geomesa.spark.jts.udf.SpatialRelationFunctions

import scala.util.Try

class ST_Contains extends GenericUDF {
  private var data: Array[ObjectInspector] = _

  def getDisplayString(children: Array[String]): String = "st_makeBBOX"

  def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    data = arguments
    PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
  }

  def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = Try {
    val List(l, r) =
      data.toList
        .zip(arguments.toList)
        .map { case (deser, arg) =>
          GeometryUDT.deserialize(
            HiveInspectorsExposed
              .unwrapperFor(deser)(arg.get())
              .asInstanceOf[InternalRow]
          )
        }

    SpatialRelationFunctions.ST_Contains(l, r)
  }.getOrElse(false.asInstanceOf[java.lang.Boolean])
}
