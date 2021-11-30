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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String
import cats.Id
import shapeless.HNil

import scala.util.Try

trait UnaryDeserializer[F[_], T] extends HDeserialier[F, T] {
  def deserializeUnary(argument: AnyRef): F[T] =
    deserialize(Array(argument))
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
    arguments => Try(UnaryDeserializer[Id, T].deserialize(arguments))

  /** Derivation helper deserializer. */
  implicit val hnilUnaryDeserializer: UnaryDeserializer[Id, HNil] = _ => HNil

  /** Spark internal deserializers. */
  implicit val internalRowUnaryDeserializer: UnaryDeserializer[Id, InternalRow] =
    _.asInstanceOf[InternalRow]

  implicit val utf8StringUnaryDeserializer: UnaryDeserializer[Id, UTF8String] =
    _.asInstanceOf[UTF8String]

  implicit val decimalUnaryDeserializer: UnaryDeserializer[Id, Decimal] =
    _.asInstanceOf[Decimal]

  val nativeDoubleUnaryDeserializer: UnaryDeserializer[Id, Double] =
    _.asInstanceOf[Double]

  val nativeIntUnaryDeserializer: UnaryDeserializer[Id, Int] =
    _.asInstanceOf[Int]

  /** JvmRepr deserializers. */
  implicit val doubleUnaryDeserializer: UnaryDeserializer[Id, Double] =
    arguments => Try(decimalUnaryDeserializer.deserialize(arguments).toDouble).getOrElse(nativeDoubleUnaryDeserializer.deserialize(arguments))

  implicit val intUnaryDeserializer: UnaryDeserializer[Id, Int] =
    arguments => Try(decimalUnaryDeserializer.deserialize(arguments).toInt).getOrElse(nativeIntUnaryDeserializer.deserialize(arguments))

  implicit val stringUnaryDeserializer: UnaryDeserializer[Id, String] =
    arguments => utf8StringUnaryDeserializer.deserialize(arguments).toString
}
