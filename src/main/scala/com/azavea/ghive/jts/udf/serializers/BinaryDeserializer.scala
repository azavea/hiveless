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

trait BinaryDeserializer[F[_], T0, T1] extends Serializable {
  def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T0, T1)]
}

object BinaryDeserializer extends Serializable {
  def apply[F[_], T0, T1](implicit ev: BinaryDeserializer[F, T0, T1]): BinaryDeserializer[F, T0, T1] = ev

  implicit def fromUnaryBinaryDeserializer[F[_]: Apply, T0: UnaryDeserializer[F, *], T1: UnaryDeserializer[F, *]]: BinaryDeserializer[F, T0, T1] =
    new BinaryDeserializer[F, T0, T1] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T0, T1)] = {
        val List(at0, at1) = arguments.toList
        val List(d0, d1)   = data.toList

        (at0.deserialize[F, T0](d0), at1.deserialize[F, T1](d1)).mapN { case (t0, t1) => t0 -> t1 }
      }
    }
}
