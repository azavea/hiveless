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

import cats.Functor
import cats.syntax.functor._
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.hive.HiveInspectorsExposed
import org.apache.spark.sql.jts.GeometryUDT
import org.locationtech.jts.geom.Geometry

import scala.util.Try

trait BinaryDeserializer[F[_], T0, T1] extends Serializable {
  def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T0, T1)]
}

object BinaryDeserializer extends Serializable {
  def apply[F[_], T0, T1](implicit ev: BinaryDeserializer[F, T0, T1]): BinaryDeserializer[F, T0, T1] = ev

  implicit def ADbinaryDeserializer[F[_]: Functor, T](implicit ad: ArgumentsDeserializer[F, T]): BinaryDeserializer[F, T, T] =
    new BinaryDeserializer[F, T, T] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T, T)] =
        ad.deserialize(arguments).map { case List(f, s) => (f, s) }
    }

  implicit val geometryDoubleBinaryDeserializer: BinaryDeserializer[Try, Geometry, Double] = new BinaryDeserializer[Try, Geometry, Double] {
    def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): Try[(Geometry, Double)] = Try {
      val List(argGeom, argP)     = arguments.toList
      val List(deserGeom, deserP) = data.toList

      val ir   = HiveInspectorsExposed.unwrap[InternalRow](argGeom.get, deserGeom)
      val geom = GeometryUDT.deserialize(ir)
      val p    = HiveInspectorsExposed.unwrap[Decimal](argP.get, deserP).toDouble

      (geom, p)
    }
  }

  implicit val geometryIntBinaryDeserializer: BinaryDeserializer[Try, Geometry, Int] = new BinaryDeserializer[Try, Geometry, Int] {
    def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): Try[(Geometry, Int)] = Try {
      val List(argGeom, argP)     = arguments.toList
      val List(deserGeom, deserP) = data.toList

      val ir   = HiveInspectorsExposed.unwrap[InternalRow](argGeom.get, deserGeom)
      val geom = GeometryUDT.deserialize(ir)
      val p    = HiveInspectorsExposed.unwrap[Int](argP.get, deserP)

      (geom, p)
    }
  }
}
