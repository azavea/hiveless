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

import com.azavea.hiveless.serializers.{HConverter, HDataTypes}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hive.HivelessInternals
import org.apache.spark.sql.types.DataType

class HGenericUDAFEvaluator[T: HAggregationBuffer: HDataTypes: HConverter] extends GenericUDAFEvaluator with Serializable {
  def dataType: DataType = HDataTypes[T].dataTypes.head

  protected def convertNullable: Any => T                = HivelessInternals.nullableUDF(HConverter[T].convert)
  @transient lazy val resultInspector                    = HivelessInternals.toWritableInspector(dataType)
  @transient var inputInspectors: Array[ObjectInspector] = _

  override def init(m: GenericUDAFEvaluator.Mode, parameters: Array[ObjectInspector]): ObjectInspector = {
    super.init(m, parameters)
    inputInspectors = parameters
    resultInspector
  }

  def getNewAggregationBuffer: GenericUDAFEvaluator.AggregationBuffer = HAggregationBuffer[T]

  def reset(aggregationBuffer: GenericUDAFEvaluator.AggregationBuffer): Unit =
    aggregationBuffer.asInstanceOf[HAggregationBuffer[T]].reset

  def iterate(aggregationBuffer: GenericUDAFEvaluator.AggregationBuffer, objects: Array[AnyRef]): Unit =
    aggregationBuffer
      .asInstanceOf[HAggregationBuffer[T]]
      .add(objects.zip(inputInspectors).map { case (o, i) => convertNullable(HivelessInternals.unwrap[InternalRow](o, i)) }.filter(_ != null))

  def terminatePartial(aggregationBuffer: GenericUDAFEvaluator.AggregationBuffer): AnyRef =
    aggregationBuffer
      .asInstanceOf[HAggregationBuffer[T]]
      .get

  def merge(aggregationBuffer: GenericUDAFEvaluator.AggregationBuffer, o: Any): Unit =
    iterate(aggregationBuffer, Array(o.asInstanceOf[AnyRef]))

  def terminate(aggregationBuffer: GenericUDAFEvaluator.AggregationBuffer): AnyRef =
    terminatePartial(aggregationBuffer)
}

object HGenericUDAFEvaluator extends Serializable {
  def apply[T: HAggregationBuffer: HDataTypes: HConverter]: HGenericUDAFEvaluator[T] = new HGenericUDAFEvaluator[T]
}
