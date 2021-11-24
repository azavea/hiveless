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

package com.azavea.ghive.jts.udf.spatial

import com.azavea.ghive.jts.udf.HUDFString
import com.azavea.ghive.jts.udf.coercions._
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.locationtech.jts.geom.Geometry
import shapeless.{::, HNil}

class ST_GeomToWKT extends HUDFString[Geometry :: HNil] {
  val name: String = "st_geomToWKT"
  def function     = WKTUtils.write
}
