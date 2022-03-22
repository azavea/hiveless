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

package com.azavea.hiveless.implicits

import com.azavea.hiveless.serializers.{HConverter, HDeserialier, HSerializer, UnaryDeserializer}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

object syntax extends Serializable {
  implicit class DeferredObjectOps(val self: GenericUDF.DeferredObject) extends AnyVal {

    /** Behaves like a regular get, but throws when the result is null. */
    def getNonEmpty: AnyRef = Option(self.get) match {
      case Some(r) => r
      case _       => throw HDeserialier.Errors.NullArgument
    }
  }

  implicit class ArrayDeferredObjectOps(val self: Array[GenericUDF.DeferredObject]) extends AnyVal {
    def deserialize[F[_], T: UnaryDeserializer[F, *]](inspectors: Array[ObjectInspector]): F[T] =
      UnaryDeserializer[F, T].deserialize(self, inspectors)
  }

  implicit class ConverterOps(val self: Any) extends AnyVal {
    def convert[T: HConverter]: T = HConverter[T].convert(self)
  }

  implicit class SerializerOps[T](val self: T) extends AnyVal {
    def serialize(implicit ev: HSerializer[T]): Any = HSerializer[T].serialize(self)
  }
}
