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

package com.azavea.ghive.jts.udf.serializers

import cats.Apply
import cats.syntax.apply._
import shapeless.{::, HList, HNil}

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

trait GenericDeserializer[F[_], L <: HList] extends Serializable {
  def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): F[L]
}

object GenericDeserializer extends Serializable {
  def apply[F[_], L <: HList](implicit ev: GenericDeserializer[F, L]): GenericDeserializer[F, L] = ev

  implicit def gdHNil[F[_]: UnaryDeserializer[*[_], HNil]]: GenericDeserializer[F, HNil] = new GenericDeserializer[F, HNil] {
    def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): F[HNil] =
      UnaryDeserializer[F, HNil].deserialize(arguments)
  }

  // format: off
  /**
   * WARNING: wrapping arguments into shapeless.Lazy causes the following failure:
   *   Serialization trace:
   *     t$1 (shapeless.Lazy$$anon$1)
   *     dh$1 (com.azavea.ghive.jts.udf.serializers.GenericDeserializer$$anon$2)
   *     inst$macro$7$1 (com.azavea.ghive.jts.udf.spatial.ST_Contains$$anonfun$$lessinit$greater$2)
   *     t$1 (shapeless.Lazy$$anon$1)
   *     dt$1 (com.azavea.ghive.jts.udf.serializers.GenericDeserializer$$anon$2)
   */
  // format: on
  implicit def gdHCons[F[_]: Apply, H, T <: HList](implicit
      dh: UnaryDeserializer[F, H],
      dt: GenericDeserializer[F, T]
  ): GenericDeserializer[F, H :: T] = new GenericDeserializer[F, H :: T] {
    def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit inspectors: Array[ObjectInspector]): F[H :: T] =
      (dh.deserialize(arguments.head, inspectors.head), dt.deserialize(arguments.tail)(inspectors.tail)).mapN(_ :: _)
  }
}
