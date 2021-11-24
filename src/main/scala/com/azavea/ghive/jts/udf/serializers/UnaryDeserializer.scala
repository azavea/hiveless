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
import cats.{~>, Id}

import scala.util.Try

trait UnaryDeserializer[F[_], T] extends Serializable { self =>
  def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): F[T]
  def deserialize(argument: GenericUDF.DeferredObject, inspector: ObjectInspector): F[T] =
    deserialize(Array(argument))(Array(inspector))

  def mapK[G[_]](f: F ~> G): UnaryDeserializer[G, T] =
    new UnaryDeserializer[G, T] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): G[T] =
        f(self.deserialize(arguments))
    }
}

object UnaryDeserializer extends Serializable {
  def apply[F[_], T](implicit ev: UnaryDeserializer[F, T]): UnaryDeserializer[F, T] = ev

  // format: off
  /**
   * 1. Spark throws java.lang.NullPointerException, investigate reasons; happens due to the FunctionK usage i.e. UnaryDeserializer[Id, T].mapK(λ[Id ~> Try](Try(_)))
   * 2. Anonymous functions are not allowed as well: Functor for UnaryDeserializer[F, *]
   *      fails with unable to find class: com.azavea.ghive.jts.udf.serializers.UnaryDeserializer$$$Lambda$4321/1862326200
   *      i.e. decimalUnaryDeserializer.map(_.toInt)
   */
  // format: on
  implicit def tryUnaryDeserializer[T: UnaryDeserializer[Id, *]]: UnaryDeserializer[Try, T] =
    new UnaryDeserializer[Try, T] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): Try[T] =
        Try(UnaryDeserializer[Id, T].deserialize(arguments))
    }

  /** Spark internal deserializers. */
  implicit val internalRowUnaryDeserializer: UnaryDeserializer[Id, InternalRow] =
    new UnaryDeserializer[Id, InternalRow] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): InternalRow =
        HiveInspectorsExposed.unwrap[InternalRow](arguments.head.get, inspectors.head)
    }

  implicit val utf8StringUnaryDeserializer: UnaryDeserializer[Id, UTF8String] =
    new UnaryDeserializer[Id, UTF8String] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): UTF8String =
        HiveInspectorsExposed.unwrap[UTF8String](arguments.head.get, inspectors.head)
    }

  implicit val decimalUnaryDeserializer: UnaryDeserializer[Id, Decimal] =
    new UnaryDeserializer[Id, Decimal] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): Decimal =
        HiveInspectorsExposed.unwrap[Decimal](arguments.head.get, inspectors.head)
    }

  /** JvmRepr deserializers. */
  implicit val doubleUnaryDeserializer: UnaryDeserializer[Id, Double] =
    new UnaryDeserializer[Id, Double] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): Double =
        decimalUnaryDeserializer.deserialize(arguments).toDouble
    }

  implicit val intUnaryDeserializer: UnaryDeserializer[Id, Int] =
    new UnaryDeserializer[Id, Int] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): Int =
        decimalUnaryDeserializer.deserialize(arguments).toInt
    }

  implicit val stringUnaryDeserializer: UnaryDeserializer[Id, String] =
    new UnaryDeserializer[Id, String] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): String =
        utf8StringUnaryDeserializer.deserialize(arguments).toString
    }

  implicit val geometryUnaryDeserializer: UnaryDeserializer[Id, Geometry] =
    new UnaryDeserializer[Id, Geometry] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): Geometry =
        GeometryUDT.deserialize(internalRowUnaryDeserializer.deserialize(arguments))
    }
}
