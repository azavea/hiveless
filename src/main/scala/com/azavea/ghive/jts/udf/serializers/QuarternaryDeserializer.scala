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

import com.azavea.ghive.jts.udf.serializers.syntax._
import cats.Apply
import cats.syntax.apply._

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

trait QuarternaryDeserializer[F[_], T0, T1, T2, T3] extends Serializable {
  def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T0, T1, T2, T3)]
}

object QuarternaryDeserializer extends Serializable {
  def apply[F[_], T0, T1, T2, T3](implicit ev: QuarternaryDeserializer[F, T0, T1, T2, T3]): QuarternaryDeserializer[F, T0, T1, T2, T3] = ev

  implicit def fromUnaryQuarternaryDeserializer[
      F[_]: Apply,
      T0: UnaryDeserializer[F, *],
      T1: UnaryDeserializer[F, *],
      T2: UnaryDeserializer[F, *],
      T3: UnaryDeserializer[F, *]
  ]: QuarternaryDeserializer[F, T0, T1, T2, T3] =
    new QuarternaryDeserializer[F, T0, T1, T2, T3] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T0, T1, T2, T3)] = {
        val List(at0, at1, at2, at3) = arguments.toList
        val List(d0, d1, d2, d3)     = data.toList

        (at0.deserialize[F, T0](d0), at1.deserialize[F, T1](d1), at2.deserialize[F, T2](d2), at3.deserialize[F, T3](d3)).mapN { case (t0, t1, t2, t3) =>
          (t0, t1, t2, t3)
        }
      }
    }
}
