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

package com.azavea.hiveless.serializers

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hive.HivelessInternals
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String
import cats.Id
import shapeless.HNil

import scala.util.Try

trait UnaryDeserializer[F[_], T] extends HDeserialier[F, T] {
  def deserialize(argument: GenericUDF.DeferredObject, inspector: ObjectInspector): F[T] =
    deserialize(Array(argument), Array(inspector))
}

object UnaryDeserializer extends Serializable {
  def apply[F[_], T](implicit ev: UnaryDeserializer[F, T]): UnaryDeserializer[F, T] = ev

  // format: off
  /**
   * On DataBricks:
   *   1. Spark throws java.lang.NullPointerException, investigate reasons; happens due to the FunctionK usage i.e. UnaryDeserializer[Id, T].mapK(λ[Id ~> Try](Try(_)))
   *   2. Anonymous functions are not allowed as well: Functor for UnaryDeserializer[F, *]
   *        fails with unable to find class: com.azavea.hiveless.serializers.UnaryDeserializer$$$Lambda$4321/1862326200
   *        i.e. decimalUnaryDeserializer.map(_.toInt)
   */
  // format: on
  implicit def tryUnaryDeserializer[T: UnaryDeserializer[Id, *]]: UnaryDeserializer[Try, T] =
    (arguments, inspectors) => Try(UnaryDeserializer[Id, T].deserialize(arguments, inspectors))

  /** Derivation helper deserializer. */
  implicit val hnilUnaryDeserializer: UnaryDeserializer[Id, HNil] = (_, _) => HNil

  /** Spark internal deserializers. */
  implicit val internalRowUnaryDeserializer: UnaryDeserializer[Id, InternalRow] =
    (arguments, inspectors) => HivelessInternals.unwrap[InternalRow](arguments.head.get, inspectors.head)

  implicit val utf8StringUnaryDeserializer: UnaryDeserializer[Id, UTF8String] =
    (arguments, inspectors) => HivelessInternals.unwrap[UTF8String](arguments.head.get, inspectors.head)

  implicit val decimalUnaryDeserializer: UnaryDeserializer[Id, Decimal] =
    (arguments, inspectors) => HivelessInternals.unwrap[Decimal](arguments.head.get, inspectors.head)

  val nativeDoubleUnaryDeserializer: UnaryDeserializer[Id, Double] =
    (arguments, inspectors) => HivelessInternals.unwrap[Double](arguments.head.get, inspectors.head)

  val nativeIntUnaryDeserializer: UnaryDeserializer[Id, Int] =
    (arguments, inspectors) => HivelessInternals.unwrap[Int](arguments.head.get, inspectors.head)

  /** JvmRepr deserializers. */
  implicit val doubleUnaryDeserializer: UnaryDeserializer[Id, Double] =
    (arguments, inspectors) =>
      Try(decimalUnaryDeserializer.deserialize(arguments, inspectors).toDouble)
        .getOrElse(nativeDoubleUnaryDeserializer.deserialize(arguments, inspectors))

  implicit val intUnaryDeserializer: UnaryDeserializer[Id, Int] =
    (arguments, inspectors) =>
      Try(decimalUnaryDeserializer.deserialize(arguments, inspectors).toInt)
        .getOrElse(nativeIntUnaryDeserializer.deserialize(arguments, inspectors))

  implicit val stringUnaryDeserializer: UnaryDeserializer[Id, String] =
    (arguments, inspectors) => utf8StringUnaryDeserializer.deserialize(arguments, inspectors).toString
}
