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

import com.azavea.ghive.jts.udf.HUDFGeometry
import com.azavea.ghive.jts.udf.coercions._
import org.locationtech.geomesa.spark.jts.udf.GeometricConstructorFunctions
import shapeless.{::, HNil}

class ST_MakeBBOX extends HUDFGeometry[Double :: Double :: Double :: Double :: HNil] {
  val name: String = "st_makeBBOX"
  def function     = GeometricConstructorFunctions.ST_MakeBBOX
}
