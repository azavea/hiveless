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

package com.azavea.ghive.jts.udf.serializers

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hive.HiveInspectorsExposed
import org.apache.spark.sql.jts.GeometryUDT
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

import scala.util.Try

trait ArgumentsDeserializer[F[_], T] {
  def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[List[T]]
}

object ArgumentsDeserializer {
  def apply[F[_], T](implicit ev: ArgumentsDeserializer[F, T]): ArgumentsDeserializer[F, T] = ev

  implicit def doubleArgumentsDeserializer: ArgumentsDeserializer[Try, Double] =
    new ArgumentsDeserializer[Try, Double] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): Try[List[Double]] = Try {
        data.toList
          .zip(arguments.toList)
          .map { case (deser, arg) => HiveInspectorsExposed.unwrap[Double](arg.get, deser) }
      }
    }

  implicit def stringArgumentsDeserializer: ArgumentsDeserializer[Try, String] =
    new ArgumentsDeserializer[Try, String] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): Try[List[String]] = Try {
        data.toList
          .zip(arguments.toList)
          .map { case (deser, arg) => HiveInspectorsExposed.unwrap[UTF8String](arg.get, deser).toString }
      }
    }

  implicit def geometryArgumentsDeserializer: ArgumentsDeserializer[Try, Geometry] =
    new ArgumentsDeserializer[Try, Geometry] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): Try[List[Geometry]] = Try {
        data.toList
          .zip(arguments.toList)
          .map { case (deser, arg) => GeometryUDT.deserialize(HiveInspectorsExposed.unwrap[InternalRow](arg.get, deser)) }
      }
    }
}
