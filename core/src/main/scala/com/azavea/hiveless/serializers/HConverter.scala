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

trait HConverter[F[_], T] extends Serializable {
  def convert(argument: AnyRef): F[T]
}

object HConverter extends Serializable {
  def apply[F[_], T](implicit ev: HConverter[F, T]): HConverter[F, T] = ev

  // format: off
  /**
   * On DataBricks:
   *   1. Spark throws java.lang.NullPointerException, investigate reasons; happens due to the FunctionK usage i.e. HConverter[Id, T].mapK(λ[Id ~> Try](Try(_)))
   *   2. Anonymous functions are not allowed as well: Functor for HConverter[F, *]
   *        fails with unable to find class: com.azavea.hiveless.serializers.HConverter$$$Lambda$4321/1862326200
   *        i.e. decimalHConverter.map(_.toInt)
   */
  // format: on
  implicit def tryHConverter[T: HConverter[Id, *]]: HConverter[Try, T] = new HConverter[Try, T] {
    def convert(argument: AnyRef): Try[T] = Try(HConverter[Id, T].convert(argument))
  }

  /** Derivation helper. */
  implicit val hnilHConverter: HConverter[Id, HNil] = new HConverter[Id, HNil] {
    def convert(argument: AnyRef): HNil = HNil
  }

  /** Spark internal type casts. */
  implicit val internalRowHConverter: HConverter[Id, InternalRow] = new HConverter[Id, InternalRow] {
    def convert(argument: AnyRef): InternalRow = argument.asInstanceOf[InternalRow]
  }
  implicit val utf8StringHConverter: HConverter[Id, UTF8String] = new HConverter[Id, UTF8String] {
    def convert(argument: AnyRef): UTF8String = argument.asInstanceOf[UTF8String]
  }
  implicit val decimalHConverter: HConverter[Id, Decimal] = new HConverter[Id, Decimal] {
    def convert(argument: AnyRef): Decimal = argument.asInstanceOf[Decimal]
  }

  val nativeDoubleHConverter: HConverter[Id, Double] = new HConverter[Id, Double] {
    def convert(argument: AnyRef): Double = argument.asInstanceOf[Double]
  }
  val nativeIntHConverter: HConverter[Id, Int] = new HConverter[Id, Int] {
    def convert(argument: AnyRef): Int = argument.asInstanceOf[Int]
  }

  /** JvmRepr casts. */
  implicit val doubleHConverter: HConverter[Id, Double] = new HConverter[Id, Double] {
    def convert(argument: AnyRef): Double = Try(decimalHConverter.convert(argument).toDouble).getOrElse(nativeDoubleHConverter.convert(argument))
  }

  implicit val intHConverter: HConverter[Id, Int] = new HConverter[Id, Int] {
    def convert(argument: AnyRef): Int = Try(decimalHConverter.convert(argument).toInt).getOrElse(nativeIntHConverter.convert(argument))
  }

  implicit val stringHConverter: HConverter[Id, String] = new HConverter[Id, String] {
    def convert(argument: AnyRef): String = utf8StringHConverter.convert(argument).toString
  }
}
