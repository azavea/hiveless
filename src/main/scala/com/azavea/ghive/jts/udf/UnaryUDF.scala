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

import com.azavea.ghive.jts.udf.serializers._

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.spark.sql.hive.HiveInspectorsExposed
import org.apache.spark.sql.types.DataType

abstract class UnaryUDF[A: TUnaryDeserializer, B] extends InitializedGenericUDF {
  def dataType: DataType
  def function: A => B
  def default: B
  def serialize: B => Any

  def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    val res =
      UnaryDeserializer[A]
        .deserialize(arguments)
        .map(function)
        .getOrElse(default)

    HiveInspectorsExposed.wrap(serialize(res), initInspector, dataType)
  }
}
