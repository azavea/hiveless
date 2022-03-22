/*
 * Copyright 2022 Azavea
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

package com.azavea.hiveless.spatial

import com.azavea.hiveless.serializers.{HConverter, HSerializer, UnaryDeserializer}
import com.azavea.hiveless.implicits.syntax._
import cats.Id
import geotrellis.proj4.CRS
import org.apache.spark.sql.types.{DataType, StringType}

package object index extends Serializable {
  implicit def crsConverter: HConverter[CRS] = new HConverter[CRS] {
    def convert(argument: Any): CRS = CRS.fromString(argument.convert[String])
  }

  implicit def crsUnaryDeserializer: UnaryDeserializer[Id, CRS] =
    (arguments, inspectors) => arguments.deserialize[Id, String](inspectors).convert[CRS]

  implicit def crsSerializer: HSerializer[CRS] = new HSerializer[CRS] {
    def dataType: DataType    = StringType
    def serialize: CRS => Any = crs => crs.toProj4String.serialize
  }
}
