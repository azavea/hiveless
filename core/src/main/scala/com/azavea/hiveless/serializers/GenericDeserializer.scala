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

import cats.{Apply, Functor}
import cats.syntax.apply._
import cats.syntax.functor._
import shapeless.{::, Generic, HList, HNil, IsTuple}

trait GenericDeserializer[F[_], L] extends HDeserialier[F, L]

object GenericDeserializer extends Serializable {
  def apply[F[_], L](implicit ev: GenericDeserializer[F, L]): GenericDeserializer[F, L] = ev

  /** A corner case, to avoid Tuple1[T] usage. */
  implicit def genericDeserializerTuple1[F[_]: Functor, P](implicit
      d: GenericDeserializer[F, P :: HNil]
  ): GenericDeserializer[F, P] = new GenericDeserializer[F, P] {
    def deserialize(arguments: Array[AnyRef]): F[P] = d.deserialize(arguments).map(_.head)
  }

  implicit def genericDeserializerTuple[F[_]: Functor, P: IsTuple, R <: HList](implicit
      gen: Generic.Aux[P, R],
      d: GenericDeserializer[F, R]
  ): GenericDeserializer[F, P] = new GenericDeserializer[F, P] {
    def deserialize(arguments: Array[AnyRef]): F[P] = d.deserialize(arguments).map(gen.from)
  }

  // format: off
  /**
   * Intentionally not converted into lambda expression, causes the following failure on DataBricks:
   *   Unable to find class: com.azavea.hiveless.serializers.GenericDeserializer$$$Lambda$4543/585871703
   */
  // format: on
  implicit def genericDeserializerHNil[F[_]: UnaryDeserializer[*[_], HNil]]: GenericDeserializer[F, HNil] = new GenericDeserializer[F, HNil] {
    def deserialize(arguments: Array[AnyRef]): F[HNil] =
      UnaryDeserializer[F, HNil].deserialize(arguments)
  }

  // format: off
  /**
   * WARNING: wrapping arguments into shapeless.Lazy causes the following failure on DataBricks:
   *   Serialization trace:
   *     t$1 (shapeless.Lazy$$anon$1)
   *     dh$1 (com.azavea.hiveless.serializers.GenericDeserializer$$anon$2)
   *     inst$macro$7$1 (com.azavea.hiveless.spatial.ST_Contains$$anonfun$$lessinit$greater$2)
   *     t$1 (shapeless.Lazy$$anon$1)
   *     dt$1 (com.azavea.hiveless.serializers.GenericDeserializer$$anon$2)
   */
  // format: on
  implicit def genericDeserializerHCons[F[_]: Apply, H, T <: HList](implicit
      dh: UnaryDeserializer[F, H],
      dt: GenericDeserializer[F, T]
  ): GenericDeserializer[F, H :: T] = new GenericDeserializer[F, H :: T] {
    def deserialize(arguments: Array[AnyRef]): F[H :: T] =
      (dh.deserializeUnary(arguments.head), dt.deserialize(arguments.tail)).mapN(_ :: _)
  }
}
