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

import org.apache.spark.sql.types.DataType
import shapeless.{::, Generic, HList, HNil, IsTuple}

/** HDataType of type T (jvmRepr) that matches the catalystRepr */
trait HDataTypes[T] {
  def dataTypes: List[DataType]
}
object HDataTypes {
  def apply[T](implicit ev: HDataTypes[T]): HDataTypes[T] = ev

  implicit def tuple1DataTypesHCons[P](implicit d: HDataTypes[P :: HNil]): HDataTypes[P] =
    new HDataTypes[P] { def dataTypes: List[DataType] = d.dataTypes }

  implicit def tupleDataTypes[P: IsTuple, L <: HList](implicit gen: Generic.Aux[P, L], d: HDataTypes[L]): HDataTypes[P] =
    new HDataTypes[P] { def dataTypes: List[DataType] = d.dataTypes }

  implicit val hnilHDataTypes: HDataTypes[HNil] = new HDataTypes[HNil] { def dataTypes: List[DataType] = Nil }

  implicit def dataTypesHCons[H, T <: HList](implicit h: HSerializer[H], t: HDataTypes[T]): HDataTypes[H :: T] = new HDataTypes[H :: T] {
    def dataTypes: List[DataType] = h.dataType :: t.dataTypes
  }
}
