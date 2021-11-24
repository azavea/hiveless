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

trait TernaryDeserializer[F[_], T0, T1, T2] {
  def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T0, T1, T2)]
}

object TernaryDeserializer {
  def apply[F[_], T0, T1, T2](implicit ev: TernaryDeserializer[F, T0, T1, T2]): TernaryDeserializer[F, T0, T1, T2] = ev

  implicit def ADternaryDeserializer[F[_]: Functor, T](implicit ad: ArgumentsDeserializer[F, T]): TernaryDeserializer[F, T, T, T] =
    new TernaryDeserializer[F, T, T, T] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T, T, T)] =
        ad.deserialize(arguments).map { case List(fst, snd, trd) => (fst, snd, trd) }
    }
}
