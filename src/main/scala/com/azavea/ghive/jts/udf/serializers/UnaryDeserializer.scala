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
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

import scala.util.Try

trait UnaryDeserializer[F[_], T] extends Serializable {
  def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): F[T]
  def deserialize(argument: GenericUDF.DeferredObject, inspector: ObjectInspector): F[T] =
    deserialize(Array(argument))(Array(inspector))
}

object UnaryDeserializer extends Serializable {
  def apply[F[_], T](implicit ev: UnaryDeserializer[F, T]): UnaryDeserializer[F, T] = ev

  implicit val intUnaryDeserializer: UnaryDeserializer[Try, Int] =
    new UnaryDeserializer[Try, Int] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): Try[Int] = Try {
        HiveInspectorsExposed.unwrap[Decimal](arguments.head.get, inspectors.head).toInt
      }
    }

  implicit val doubleUnaryDeserializer: UnaryDeserializer[Try, Double] =
    new UnaryDeserializer[Try, Double] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): Try[Double] = Try {
        HiveInspectorsExposed.unwrap[Decimal](arguments.head.get, inspectors.head).toDouble
      }
    }

  implicit val stringUnaryDeserializer: UnaryDeserializer[Try, String] =
    new UnaryDeserializer[Try, String] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): Try[String] = Try {
        HiveInspectorsExposed.unwrap[UTF8String](arguments.head.get, inspectors.head).toString
      }
    }

  implicit val geometryUnaryDeserializer: UnaryDeserializer[Try, Geometry] =
    new UnaryDeserializer[Try, Geometry] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): Try[Geometry] = Try {
        GeometryUDT.deserialize(HiveInspectorsExposed.unwrap[InternalRow](arguments.head.get, inspectors.head))
      }
    }
}
