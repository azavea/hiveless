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
import com.azavea.hiveless.implicits.tupler._
import com.azavea.hiveless.serializers.UnaryDeserializer
import geotrellis.vector._
import org.locationtech.jts.geom.Geometry
import shapeless._

class ST_Intersects extends HUDF[(ST_Intersects.Arg, ST_Intersects.Arg), Boolean] {
  val name: String = "st_intersects"
  def function     = ST_Intersects.apply
}

object ST_Intersects {
  import UnaryDeserializer.Errors.ProductDeserializationError

  type Arg = Extent :+: Geometry :+: CNil

  def apply(left: Arg, right: Arg): Boolean = {
    val l = left
      .select[Extent]
      .orElse(left.select[Geometry].map(_.extent))
      .getOrElse(throw ProductDeserializationError[Arg](this.getClass, "first"))

    val r = right
      .select[Extent]
      .orElse(right.select[Geometry].map(_.extent))
      .getOrElse(throw ProductDeserializationError[Arg](this.getClass, "second"))

    l.intersects(r)
  }
}
