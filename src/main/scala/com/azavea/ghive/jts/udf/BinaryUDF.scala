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
import com.azavea.ghive.jts.udf.serializers.syntax._
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.spark.sql.types.DataType

abstract class BinaryUDF[T0, T1: TryBinaryDeserializer[T0, *], R] extends SparkGenericUDF[R] {
  def dataType: DataType
  def function: (T0, T1) => R

  def eval(arguments: Array[GenericUDF.DeferredObject]): R =
    arguments.binary.map(function.tupled).getOrElse(null.asInstanceOf[R])
}
