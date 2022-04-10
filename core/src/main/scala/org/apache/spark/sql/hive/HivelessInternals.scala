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

package org.apache.spark.sql.hive

import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.spark.sql.types._

object HivelessInternals extends HiveInspectors with Serializable {
  type HiveGenericUDF = org.apache.spark.sql.hive.HiveGenericUDF

  def toWritableInspector(dataType: DataType): ObjectInspector = dataType match {
    case ArrayType(tpe, _) =>
      ObjectInspectorFactory.getStandardListObjectInspector(toWritableInspector(tpe))
    case MapType(keyType, valueType, _) =>
      ObjectInspectorFactory.getStandardMapObjectInspector(toWritableInspector(keyType), toWritableInspector(valueType))
    case StringType    => PrimitiveObjectInspectorFactory.writableStringObjectInspector
    case IntegerType   => PrimitiveObjectInspectorFactory.writableIntObjectInspector
    case DoubleType    => PrimitiveObjectInspectorFactory.writableDoubleObjectInspector
    case BooleanType   => PrimitiveObjectInspectorFactory.writableBooleanObjectInspector
    case LongType      => PrimitiveObjectInspectorFactory.writableLongObjectInspector
    case FloatType     => PrimitiveObjectInspectorFactory.writableFloatObjectInspector
    case ShortType     => PrimitiveObjectInspectorFactory.writableShortObjectInspector
    case ByteType      => PrimitiveObjectInspectorFactory.writableByteObjectInspector
    case NullType      => PrimitiveObjectInspectorFactory.writableVoidObjectInspector
    case BinaryType    => PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
    case DateType      => PrimitiveObjectInspectorFactory.writableDateObjectInspector
    case TimestampType => PrimitiveObjectInspectorFactory.writableTimestampObjectInspector
    case DecimalType() => PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector
    case StructType(fields) =>
      ObjectInspectorFactory.getStandardStructObjectInspector(
        java.util.Arrays.asList(fields.map(f => f.name): _*),
        java.util.Arrays.asList(fields.map(f => toWritableInspector(f.dataType)): _*)
      )
    case _: UserDefinedType[_] =>
      val sqlType = dataType.asInstanceOf[UserDefinedType[_]].sqlType
      toWritableInspector(sqlType)
  }

  def unwrap[T](a: Any, deser: ObjectInspector): T = unwrapperFor(deser)(a).asInstanceOf[T]
  def wrap(a: Any, ser: ObjectInspector): AnyRef   = wrapperFor(ser, inspectorToDataType(ser))(a).asInstanceOf[AnyRef]

  def nullableUDF[A1, RT](f: A1 => RT): A1 => RT = {
    case null => null.asInstanceOf[RT]
    case out1 => f(out1)
  }

  implicit class WithTypeConformity(val left: DataType) extends AnyVal {
    def conformsTo(right: DataType): Boolean = right.acceptsType(left)
  }
}
