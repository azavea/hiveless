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

import com.azavea.hiveless.spark.encoders.syntax._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.util.ArrayData

import java.{lang => jl}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

trait HSerializer[T] extends Serializable {
  def dataType: DataType
  def serialize: T => Any
}

/**
 * Identity serializer that just carries the corresponding Spark type. Useful since HSerializer.instance may cause serialization errors.
 */
trait IdHSerializer[T] extends HSerializer[T] { def serialize: T => T = identity }

object HSerializer extends Serializable {
  def apply[T](implicit ev: HSerializer[T]): HSerializer[T] = ev

  // format: off
  /**
   * Intentionally not used for instances implementation, causes the following failure on DataBricks:
   *   Unable to find class: com.azavea.hiveless.serializers.HSerializer$$$Lambda$5659/1670981434
   *   Serialization trace:
   *     s$1 (com.azavea.hiveless.serializers.HSerializer$$anon$1)
   */
  // format: on
  def instance[T](dt: DataType, s: T => Any): HSerializer[T] = new HSerializer[T] {
    val dataType: DataType  = dt
    def serialize: T => Any = s
  }

  // format: off
  /**
   * Derive HSerializer from ExpressionEncoder.
   * Intentionally not used for instances implementation, causes the following failure on DataBricks:
   *   org.apache.spark.SparkException: Job aborted due to stage failure: Task serialization failed: com.esotericsoftware.kryo.KryoException: java.util.ConcurrentModificationException
   *   Serialization trace:
   *     classes (sun.misc.Launcher$AppClassLoader)
   *     classloader (java.security.ProtectionDomain)
   *     context (java.security.AccessControlContext)
   *     acc (com.databricks.backend.daemon.driver.ClassLoaders$LibraryClassLoader)
   *     classLoader (scala.reflect.runtime.JavaMirrors$JavaMirror)
   *     mirror (scala.reflect.api.TypeTags$TypeTagImpl)
   *     tg$1 (com.azavea.hiveless.serializers.HSerializer$$anon$2)
   */
  // format: on
  def expressionEncoderSerializer[T: TypeTag](implicit enc: ExpressionEncoder[T]): HSerializer[T] = new HSerializer[T] {
    def dataType: DataType  = enc.schema
    def serialize: T => Any = _.toInternalRow
  }

  implicit val booleanSerializer: HSerializer[Boolean] = new IdHSerializer[Boolean] { def dataType: DataType = BooleanType }
  implicit val doubleSerializer: HSerializer[Double]   = new IdHSerializer[Double] { def dataType: DataType = DoubleType }
  implicit val floatSerializer: HSerializer[Float]     = new IdHSerializer[Float] { def dataType: DataType = FloatType }
  implicit val longSerializer: HSerializer[Long]       = new IdHSerializer[Long] { def dataType: DataType = LongType }
  implicit val integerSerializer: HSerializer[Int]     = new IdHSerializer[Int] { def dataType: DataType = IntegerType }
  implicit val shortSerializer: HSerializer[Short]     = new IdHSerializer[Short] { def dataType: DataType = ShortType }
  implicit val byteSerializer: HSerializer[Byte]       = new IdHSerializer[Byte] { def dataType: DataType = ByteType }
  implicit val stringSerializer: HSerializer[String] = new HSerializer[String] {
    def dataType: DataType       = StringType
    def serialize: String => Any = UTF8String.fromString
  }

  implicit val jlBooleanSerializer: HSerializer[jl.Boolean] = new IdHSerializer[jl.Boolean] { def dataType: DataType = BooleanType }
  implicit val jlDoubleSerializer: HSerializer[jl.Double]   = new IdHSerializer[jl.Double] { def dataType: DataType = DoubleType }
  implicit val jlFloatSerializer: HSerializer[jl.Float]     = new IdHSerializer[jl.Float] { def dataType: DataType = FloatType }
  implicit val jlLongSerializer: HSerializer[jl.Long]       = new IdHSerializer[jl.Long] { def dataType: DataType = LongType }
  implicit val jlIntegerSerializer: HSerializer[jl.Integer] = new IdHSerializer[jl.Integer] { def dataType: DataType = IntegerType }
  implicit val jlShortSerializer: HSerializer[jl.Short]     = new IdHSerializer[jl.Short] { def dataType: DataType = ShortType }
  implicit val jlByteSerializer: HSerializer[jl.Byte]       = new IdHSerializer[jl.Byte] { def dataType: DataType = ByteType }

  // format: off
  /**
   * Array of bytes is handled differently in between Spark / Hive comparing to the other Array types.
   * The Spark DataType in this case corresponds to BinaryType, and no serialization is needed.
   */
  // format: on
  implicit def binarySerializer[C[_]](implicit ev: C[Byte] => Seq[Byte]): HSerializer[C[Byte]] = new HSerializer[C[Byte]] {
    def dataType: DataType        = BinaryType
    def serialize: C[Byte] => Any = seq => seq.toArray
  }

  implicit def arraySerializer[T: HSerializer: ClassTag: λ[τ => C[τ] => Seq[τ]], C[_]]: HSerializer[C[T]] = new HSerializer[C[T]] {
    def dataType: DataType     = ArrayType(HSerializer[T].dataType)
    def serialize: C[T] => Any = seq => ArrayData.toArrayData(seq.toArray)
  }
}
