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

import com.azavea.hiveless.serializers.HSerializer
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.spark.sql.hive.HivelessInternals
import org.apache.spark.sql.types.DataType

abstract class HAggregationBuffer[T: HSerializer] extends AggregationBuffer with Serializable {
  protected def accumulator: T
  def dataType: DataType  = HSerializer[T].dataType
  def serialize: T => Any = HSerializer[T].serialize

  protected def serializeNullable: T => Any            = HivelessInternals.nullableUDF(serialize)
  @transient lazy val resultInspector: ObjectInspector = HivelessInternals.toWritableInspector(dataType)

  def union(argument: T): Unit
  def union(argument: Seq[T]): Unit = argument.foreach(union)

  def get: AnyRef = HivelessInternals.wrap(serializeNullable(accumulator), resultInspector)

  def reset: Unit
}

object HAggregationBuffer {
  def apply[T](implicit ev: HAggregationBuffer[T]): HAggregationBuffer[T] = ev
}
