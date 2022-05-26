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

package com.azavea.hiveless.serializers

import com.azavea.hiveless.{SpatialHiveTestEnvironment, SpatialTestTables}
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec

class HSerializerSpec extends AnyFunSpec with SpatialHiveTestEnvironment with SpatialTestTables {
  override def registerHiveUDFs(ssc: SparkSession): Unit = {
    super.registerHiveUDFs(ssc)
    ssc.sql("CREATE OR REPLACE FUNCTION groupString as 'com.azavea.hiveless.serializers.GroupString';")
  }

  describe("HSerializerSpec") {
    it("should serialize array of strings") {
      val (str, n) = ("HSerializerSpecString", 3)
      val expected = str.grouped(n).toArray
      val df       = ssc.sql(s"SELECT groupString('$str', $n);")

      df.collect().head.getAs[Array[String]](0) shouldBe expected
    }
  }
}
