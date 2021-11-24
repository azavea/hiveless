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

import com.azavea.hiveless.serializers.GenericDeserializer
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.spark.sql.types.DataType
import shapeless.HList

import scala.util.Try

abstract class HUDF[L <: HList, R](implicit gd: GenericDeserializer[Try, L]) extends SparkGenericUDF[R] {
  def dataType: DataType
  def function: L => R

  def eval(arguments: Array[GenericUDF.DeferredObject]): R =
    gd.deserialize(arguments).map(function).getOrElse(null.asInstanceOf[R])
}