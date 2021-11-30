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

import cats.Id
import shapeless.HNil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

trait HTypeable[F[_], T] extends Serializable {
  def deserialize(arguments: Array[AnyRef]): F[T]
  def deserialize(argument: AnyRef): F[T] = deserialize(Array(argument))
}

object HTypeable extends Serializable {
  def apply[F[_], T](implicit ev: HTypeable[F, T]): HTypeable[F, T] = ev

  // format: off
  /**
   * On DataBricks:
   *   1. Spark throws java.lang.NullPointerException, investigate reasons; happens due to the FunctionK usage i.e. HTypeable[Id, T].mapK(λ[Id ~> Try](Try(_)))
   *   2. Anonymous functions are not allowed as well: Functor for HTypeable[F, *]
   *        fails with unable to find class: com.azavea.hiveless.serializers.HTypeable$$$Lambda$4321/1862326200
   *        i.e. decimalHTypeable.map(_.toInt)
   */
  // format: on
  implicit def tryHTypeable[T: HTypeable[Id, *]]: HTypeable[Try, T] =
    arguments => Try(HTypeable[Id, T].deserialize(arguments))

  /** Derivation helper deserializer. */
  implicit val hnilHTypeable: HTypeable[Id, HNil] = _ => HNil

  /** Spark internal deserializers. */
  implicit val internalRowHTypeable: HTypeable[Id, InternalRow] =
    a => a.head.asInstanceOf[InternalRow]

  implicit val utf8StringHTypeable: HTypeable[Id, UTF8String] =
    a => a.head.asInstanceOf[UTF8String]

  implicit val decimalHTypeable: HTypeable[Id, Decimal] =
    a => a.head.asInstanceOf[Decimal]

  val nativeDoubleHTypeable: HTypeable[Id, Double] =
    a => a.head.asInstanceOf[Double]

  val nativeIntHTypeable: HTypeable[Id, Int] =
    a => a.head.asInstanceOf[Int]

  /** JvmRepr deserializers. */
  implicit val doubleHTypeable: HTypeable[Id, Double] =
    arguments => Try(decimalHTypeable.deserialize(arguments).toDouble).getOrElse(nativeDoubleHTypeable.deserialize(arguments))

  implicit val intHTypeable: HTypeable[Id, Int] =
    arguments => Try(decimalHTypeable.deserialize(arguments).toInt).getOrElse(nativeIntHTypeable.deserialize(arguments))

  implicit val stringHTypeable: HTypeable[Id, String] =
    arguments => utf8StringHTypeable.deserialize(arguments).toString
}
