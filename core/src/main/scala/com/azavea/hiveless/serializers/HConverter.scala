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

import org.apache.spark.unsafe.types.UTF8String

import java.{lang => jl}

trait HConverter[T] {
  def convert(argument: Any): T
}

trait IdHConverter[T] extends HConverter[T] { def convert(argument: Any): T = argument.asInstanceOf[T] }

object HConverter {
  def apply[T](implicit ev: HConverter[T]): HConverter[T] = ev

  implicit val booleanConverter: HConverter[Boolean] = new IdHConverter[Boolean] {}
  implicit val doubleConverter: HConverter[Double]   = new IdHConverter[Double] {}
  implicit val floatConverter: HConverter[Float]     = new IdHConverter[Float] {}
  implicit val longConverter: HConverter[Long]       = new IdHConverter[Long] {}
  implicit val integerConverter: HConverter[Int]     = new IdHConverter[Int] {}
  implicit val shortConverter: HConverter[Short]     = new IdHConverter[Short] {}
  implicit val byteConverter: HConverter[Byte]       = new IdHConverter[Byte] {}
  implicit val stringConverter: HConverter[String] = new HConverter[String] {
    def convert(argument: Any): String = argument.asInstanceOf[UTF8String].toString
  }

  implicit val jlBooleanConverter: HConverter[jl.Boolean] = new IdHConverter[jl.Boolean] {}
  implicit val jlDoubleConverter: HConverter[jl.Double]   = new IdHConverter[jl.Double] {}
  implicit val jlFloatConverter: HConverter[jl.Float]     = new IdHConverter[jl.Float] {}
  implicit val jlLongConverter: HConverter[jl.Long]       = new IdHConverter[jl.Long] {}
  implicit val jlIntegerConverter: HConverter[jl.Integer] = new IdHConverter[jl.Integer] {}
  implicit val jlShortConverter: HConverter[jl.Short]     = new IdHConverter[jl.Short] {}
  implicit val jlByteConverter: HConverter[jl.Byte]       = new IdHConverter[jl.Byte] {}
}
