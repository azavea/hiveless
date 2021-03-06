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

import com.azavea.hiveless.serializers.{HDeserializer, HSerializer}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.spark.sql.types.DataType

import scala.util.{Failure, Success, Try}

abstract class HUDF[P, R](implicit d: HDeserializer[Try, P], s: HSerializer[R]) extends HGenericUDF[R] {
  def name: String        = this.getClass.getName.split("\\.").last
  def dataType: DataType  = s.dataType
  def serialize: R => Any = s.serialize
  def function: P => R

  def eval(arguments: Array[GenericUDF.DeferredObject]): R =
    d.deserialize(arguments, inputInspectors).map(function) match {
      case Success(r) => r
      // if arguments are null we can't deserialize it
      // however nulls can appear due to filtering results (that's how it is handled)
      // that is not an error state
      case Failure(HDeserializer.Errors.NullArgument) => null.asInstanceOf[R]
      case Failure(e)                                 => throw e
    }
}
