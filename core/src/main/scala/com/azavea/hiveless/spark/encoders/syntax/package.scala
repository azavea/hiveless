/*
 * Copyright 2022 Azavea
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

package com.azavea.hiveless.spark.encoders

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe.TypeTag

/**
 * Source: https://github.com/locationtech/rasterframes/blob/0.10.1/core/src/main/scala/org/locationtech/rasterframes/encoders/syntax/package.scala
 */
package object syntax extends Serializable {
  implicit class CachedExpressionOps[T](val self: T) extends AnyVal {
    def toInternalRow(implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): InternalRow = {
      val toRow = SerializersCache.serializer[T]
      toRow(self)
    }

    def toRow(implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): Row = {
      val toRow = SerializersCache.rowSerialize[T]
      toRow(self)
    }
  }

  implicit class CachedExpressionRowOps(val self: Row) extends AnyVal {
    def as[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): T = {
      val fromRow = SerializersCache.rowDeserialize[T]
      fromRow(self)
    }
  }

  implicit class CachedInternalRowOps(val self: InternalRow) extends AnyVal {
    def as[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): T = {
      val fromRow = SerializersCache.deserializer[T]
      fromRow(self)
    }
  }
}
