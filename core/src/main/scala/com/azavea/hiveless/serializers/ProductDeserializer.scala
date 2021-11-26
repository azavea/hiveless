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

import cats.Functor
import cats.syntax.functor._
import shapeless.{::, Generic, HList, HNil, IsTuple}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

/** Deserializer that helps to hide Shapeless from the user API. */
trait ProductDeserializer[F[_], P] extends HDeserialier[F, P]

object ProductDeserializer extends Serializable {
  def apply[F[_], P](implicit ev: ProductDeserializer[F, P]): ProductDeserializer[F, P] = ev

  /** A corner case, to avoid Tuple1[T] usage. */
  implicit def tuple1FromGenericDeserializerDeserializer[F[_]: Functor, P](implicit
      d: GenericDeserializer[F, P :: HNil]
  ): ProductDeserializer[F, P] = new ProductDeserializer[F, P] {
    def deserialize(arguments: Array[GenericUDF.DeferredObject], inspectors: Array[ObjectInspector]): F[P] =
      d.deserialize(arguments, inspectors).map(_.head)
  }

  implicit def tuplenFromGenericDeserializer[F[_]: Functor, P: IsTuple, R <: HList](implicit
      gen: Generic.Aux[P, R],
      d: GenericDeserializer[F, R]
  ): ProductDeserializer[F, P] = new ProductDeserializer[F, P] {
    def deserialize(arguments: Array[GenericUDF.DeferredObject], inspectors: Array[ObjectInspector]): F[P] =
      d.deserialize(arguments, inspectors).map(gen.from)
  }
}
