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

package com.azavea.hiveless.spark.rules

import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.hive.HivelessInternals
import org.apache.spark.sql.hive.HivelessInternals.HiveGenericUDF
import org.apache.spark.sql.types.{DataType, StructType}

import scala.reflect.{classTag, ClassTag}

package object syntax extends Serializable {
  implicit class HiveGenericUDFOps(val self: HiveGenericUDF) extends AnyVal {
    def of[T: ClassTag]: Boolean = self.funcWrapper.functionClassName == classTag[T].toString
  }

  def AndList(list: List[Expression]): Expression = list.reduce(And)

  implicit class DataTypeConformityOps(val left: DataType) extends AnyVal {
    def conformsToSchema(schema: StructType): Boolean =
      HivelessInternals.WithTypeConformity(left).conformsTo(schema)

    def conformsToDataType(dataType: DataType): Boolean =
      HivelessInternals.WithTypeConformity(left).conformsTo(dataType)
  }
}
