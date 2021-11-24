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

trait QuarternaryDeserializer[F[_], T0, T1, T2, T3] extends Serializable {
  def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T0, T1, T2, T3)]
}

object QuarternaryDeserializer extends Serializable {
  def apply[F[_], T0, T1, T2, T3](implicit ev: QuarternaryDeserializer[F, T0, T1, T2, T3]): QuarternaryDeserializer[F, T0, T1, T2, T3] = ev

  implicit def ADquarternaryDeserializer[F[_]: Functor, T](implicit ad: ArgumentsDeserializer[F, T]): QuarternaryDeserializer[F, T, T, T, T] =
    new QuarternaryDeserializer[F, T, T, T, T] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T, T, T, T)] =
        ad.deserialize(arguments).map { case List(fst, snd, trd, frt) => (fst, snd, trd, frt) }
    }
}
