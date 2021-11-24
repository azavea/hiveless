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

package com.azavea.hiveless

import com.azavea.hiveless.serializers.GenericDeserializer
import org.apache.spark.sql.types.{DataType, IntegerType}
import shapeless.HList

import java.{lang => jl}
import scala.util.Try

abstract class HUDFInteger[L <: HList](implicit gd: GenericDeserializer[Try, L]) extends HUDF[L, jl.Integer] {
  def dataType: DataType                  = IntegerType
  def serialize: jl.Integer => jl.Integer = identity
}
