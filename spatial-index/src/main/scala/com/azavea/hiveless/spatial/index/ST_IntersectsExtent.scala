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

package com.azavea.hiveless.spatial.index

import com.azavea.hiveless.HUDF
import com.azavea.hiveless.spatial._
import com.azavea.hiveless.serializers.UnaryDeserializer
import geotrellis.vector._
import org.locationtech.jts.geom.Geometry
import shapeless._

class ST_IntersectsExtent extends HUDF[(Extent :+: Geometry :+: CNil, Extent :+: Geometry :+: CNil), Boolean] {
  val name: String = "st_intersectsExtent"
  def function = { case (left: (Extent :+: Geometry :+: CNil), right: (Extent :+: Geometry :+: CNil)) =>
    val l = left
      .select[Extent]
      .getOrElse(
        left
          .select[Geometry]
          .map(_.extent)
          .getOrElse(throw UnaryDeserializer.Errors.ProductDeserializationError[(Extent, Geometry)](this.getClass, "first"))
      )

    val r = right
      .select[Extent]
      .getOrElse(
        right
          .select[Geometry]
          .map(_.extent)
          .getOrElse(throw UnaryDeserializer.Errors.ProductDeserializationError[(Extent, Geometry)](this.getClass, "second"))
      )

    l.intersects(r)
  }
}
