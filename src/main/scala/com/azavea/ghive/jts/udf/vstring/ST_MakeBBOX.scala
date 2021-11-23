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

package com.azavea.ghive.jts.udf.vstring

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.spark.sql.hive.HiveInspectorsExposed
import org.locationtech.geomesa.spark.jts.udf.GeometricConstructorFunctions

class ST_MakeBBOX extends GenericUDF {
  private var data: Array[ObjectInspector] = _

  def getDisplayString(children: Array[String]): String = "st_makeBBOX"

  def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    data = arguments
    PrimitiveObjectInspectorFactory.javaStringObjectInspector
  }

  def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    val List(xmin, ymin, xmax, ymax) =
      data.toList
        .zip(arguments.toList)
        .map { case (deser, arg) => HiveInspectorsExposed.unwrapperFor(deser)(arg.get()).asInstanceOf[Double] }

    GeometricConstructorFunctions.ST_MakeBBOX(xmin, ymin, xmax, ymax)
  }
}
