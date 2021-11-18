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
import org.apache.spark.sql.hive.HiveInspectorsExposed
import org.apache.spark.sql.jts.{GeometryUDT, JTSTypes}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.geomesa.spark.jts.udf.GeometricConstructorFunctions

import scala.util.Try

class ST_GeomFromWKT extends GenericUDF {
  private var data: ObjectInspector = _

  private def initInspector = HiveInspectorsExposed.toWritableInspector(GeometryUDT.sqlType)

  def getDisplayString(children: Array[String]): String = "st_geomFromWKT"

  def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    data = arguments(0)
    initInspector
  }

  def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    val res =
      GeometricConstructorFunctions
        .ST_GeomFromWKT(
          Try(
            HiveInspectorsExposed
              .unwrapperFor(data)(arguments(0).get())
              .asInstanceOf[UTF8String]
              .toString
          ).getOrElse("POLYGON EMPTY")
        )

    HiveInspectorsExposed
      .publicWrapperFor(initInspector, GeometryUDT.sqlType)(GeometryUDT.serialize(res))
      .asInstanceOf[AnyRef]
  }
}
