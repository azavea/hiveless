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
import shapeless.{::, HNil}

package object syntax extends Serializable {
  implicit class genericUDFDeferredObjectsOps(val self: Array[GenericUDF.DeferredObject]) extends AnyVal {
    def unary[F[_], T: UnaryDeserializer[F, *]](implicit inspectors: Array[ObjectInspector]): F[T] =
      UnaryDeserializer[F, T].deserialize(self)

    def binary[F[_]: Functor, T0, T1: BinaryDeserializer[F, T0, *]](implicit inspectors: Array[ObjectInspector]): F[(T0, T1)] =
      GenericDeserializer[F, T0 :: T1 :: HNil]
        .deserialize(self)
        .map(_.tupled)

    def ternary[F[_]: Functor, T0, T1, T2: TernaryDeserializer[F, T0, T1, *]](implicit inspectors: Array[ObjectInspector]): F[(T0, T1, T2)] =
      GenericDeserializer[F, T0 :: T1 :: T2 :: HNil]
        .deserialize(self)
        .map(_.tupled)

    def quarternary[F[_]: Functor, T0, T1, T2, T3: QuarternaryDeserializer[F, T0, T1, T2, *]](implicit inspectors: Array[ObjectInspector]): F[(T0, T1, T2, T3)] =
      GenericDeserializer[F, T0 :: T1 :: T2 :: T3 :: HNil]
        .deserialize(self)
        .map(_.tupled)
  }

  implicit class genericUDFDeferredObjectOps(val self: GenericUDF.DeferredObject) extends AnyVal {
    def deserialize[F[_], T: UnaryDeserializer[F, *]](inspector: ObjectInspector): F[T] =
      UnaryDeserializer[F, T].deserialize(self, inspector)
  }
}
