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

trait GenericConverter[F[_], L] extends Serializable {
  def convert(arguments: Array[AnyRef]): F[L]
}

object GenericConverter extends Serializable {
  def apply[F[_], L](implicit ev: GenericConverter[F, L]): GenericConverter[F, L] = ev

  /** A corner case, to avoid Tuple1[T] usage. */
  implicit def genericTypeableTuple1[F[_]: Functor, T](implicit
      d: GenericConverter[F, T :: HNil]
  ): GenericConverter[F, T] = new GenericConverter[F, T] {
    def convert(arguments: Array[AnyRef]): F[T] = d.convert(arguments).map(_.head)
  }

  implicit def genericTypeableTuple[F[_]: Functor, T: IsTuple, L <: HList](implicit
      gen: Generic.Aux[T, L],
      d: GenericConverter[F, L]
  ): GenericConverter[F, T] = new GenericConverter[F, T] {
    def convert(arguments: Array[AnyRef]): F[T] = d.convert(arguments).map(gen.from)
  }

  // format: off
  /**
   * Intentionally not converted into lambda expression, causes the following failure on DataBricks:
   *   Unable to find class: com.azavea.hiveless.serializers.GenericTypeable$$$Lambda$4543/585871703
   */
  // format: on
  implicit def genericTypeableHNil[F[_]: HConverter[*[_], HNil]]: GenericConverter[F, HNil] = new GenericConverter[F, HNil] {
    def convert(arguments: Array[AnyRef]): F[HNil] = HConverter[F, HNil].convert(arguments)
  }

  // format: off
  /**
   * WARNING: wrapping arguments into shapeless.Lazy causes the following failure on DataBricks:
   *   Serialization trace:
   *     t$1 (shapeless.Lazy$$anon$1)
   *     dh$1 (com.azavea.hiveless.serializers.GenericTypeable$$anon$2)
   *     inst$macro$7$1 (com.azavea.hiveless.spatial.ST_Contains$$anonfun$$lessinit$greater$2)
   *     t$1 (shapeless.Lazy$$anon$1)
   *     dt$1 (com.azavea.hiveless.serializers.GenericTypeable$$anon$2)
   */
  // format: on
  implicit def genericTypeableHCons[F[_]: Apply, H, T <: HList](implicit
      ch: HConverter[F, H],
      ct: GenericConverter[F, T]
  ): GenericConverter[F, H :: T] = new GenericConverter[F, H :: T] {
    def convert(arguments: Array[AnyRef]): F[H :: T] = (ch.convert(arguments.head), ct.convert(arguments.tail)).mapN(_ :: _)
  }
}
