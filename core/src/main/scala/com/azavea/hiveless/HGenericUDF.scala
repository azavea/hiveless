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
import org.apache.spark.sql.hive.HivelessInternals
import org.apache.spark.sql.types.DataType

trait HGenericUDF[R] extends GenericUDF {
  def name: String
  def dataType: DataType
  def serialize: R => Any

  protected def serializeNullable: R => Any = HivelessInternals.nullableUDF(serialize)

  @transient lazy val resultInspector: ObjectInspector             = HivelessInternals.toWritableInspector(dataType)
  @transient protected var inputInspectors: Array[ObjectInspector] = _

  def getDisplayString(children: Array[String]): String = name

  def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    inputInspectors = arguments
    resultInspector
  }

  // eval expects deserialized arguments as an input, returns deserialized arguments as well
  def eval(arguments: Array[AnyRef]): R

  def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    val deserializedArguments = arguments.zip(inputInspectors).map { case (a, i) => HivelessInternals.unwrap[AnyRef](a.get, i) }
    HivelessInternals.wrap(serializeNullable(eval(deserializedArguments)), resultInspector, dataType)
  }
}
