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

package com.azavea.ghive.jts.udf

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.spark.sql.hive.HiveInspectorsExposed
import org.apache.spark.sql.types.DataType

trait InitializedGenericUDF[B] extends GenericUDF {
  def name: String
  def dataType: DataType
  def serialize: B => Any

  @transient implicit lazy val initInspector: ObjectInspector = HiveInspectorsExposed.toWritableInspector(dataType)

  implicit protected var data: Array[ObjectInspector] = _

  def getDisplayString(children: Array[String]): String = name

  def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    data = arguments
    initInspector
  }

  def eval(arguments: Array[GenericUDF.DeferredObject]): B

  def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef =
    HiveInspectorsExposed.wrap(serialize(eval(arguments)), initInspector, dataType)
}
