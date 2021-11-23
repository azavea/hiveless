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

import cats.Functor
import cats.syntax.functor._

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

trait BinaryDeserializer[F[_], T0, T1] {
  def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T0, T1)]
}

object BinaryDeserializer {
  type H[F[_], T] = BinaryDeserializer[F, T, T]

  def apply[F[_], T0, T1](implicit ev: BinaryDeserializer[F, T0, T1]): BinaryDeserializer[F, T0, T1] = ev
  def instance[F[_], T](implicit ev: BinaryDeserializer[F, T, T]): BinaryDeserializer[F, T, T]       = ev

  implicit def ADbinaryDeserializer[F[_]: Functor, T](implicit ad: ArgumentsDeserializer[F, T]): BinaryDeserializer[F, T, T] =
    new BinaryDeserializer[F, T, T] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T, T)] =
        ad.deserialize(arguments).map { case List(f, s) => (f, s) }
    }
}
