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

package com.azavea.hiveless

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.spark.sql.hive.HiveInspectorsExposed
import org.apache.spark.sql.types.DataType

trait SparkGenericUDF[R] extends GenericUDF {
  def name: String
  def dataType: DataType
  def serialize: R => Any

  @transient implicit lazy val resultInspector: ObjectInspector = HiveInspectorsExposed.toWritableInspector(dataType)

  implicit protected var inspectors: Array[ObjectInspector] = _

  def getDisplayString(children: Array[String]): String = name

  def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    inspectors = arguments
    resultInspector
  }

  def eval(arguments: Array[GenericUDF.DeferredObject]): R

  def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef =
    HiveInspectorsExposed.wrap(serialize(eval(arguments)), resultInspector, dataType)
}