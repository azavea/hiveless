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

import com.azavea.hiveless.serializers.syntax._
import com.azavea.hiveless.spark.encoders.syntax._
import com.azavea.hiveless.utils.HShow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hive.HivelessInternals.unwrap
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String
import cats.Id
import cats.syntax.apply._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.util.ArrayData
import shapeless.{:+:, CNil, Coproduct, HNil, Inl, Inr, IsTuple}

import scala.reflect.ClassTag
import scala.util.Try
import scala.reflect.runtime.universe.TypeTag

trait UnaryDeserializer[F[_], T] extends HDeserialier[F, T]

object UnaryDeserializer extends Serializable {
  sealed abstract class Errors(override val getMessage: String) extends RuntimeException
  object Errors {
    case class ProductDeserializationError[T: IsTuple: HShow](clz: Class[_], name: String)
        extends Errors(
          s"""
             |${clz.getName}: could not deserialize the $name input argument:
             |should match one of the following types: ${HShow[T].show().trim.init}""".stripMargin
        )
  }

  def apply[F[_], T](implicit ev: UnaryDeserializer[F, T]): UnaryDeserializer[F, T] = ev

  def id[T](implicit ev: UnaryDeserializer[Id, T]): UnaryDeserializer[Id, T] = ev

  // format: off
  /**
   * On DataBricks:
   *   1. Spark throws java.lang.NullPointerException, investigate reasons; happens due to the FunctionK usage i.e. UnaryDeserializer[Id, T].mapK(λ[Id ~> Try](Try(_)))
   *   2. Anonymous functions are not allowed as well: Functor for UnaryDeserializer[F, *]
   *        fails with unable to find class: com.azavea.hiveless.serializers.UnaryDeserializer$$$Lambda$4321/1862326200
   *        i.e. decimalUnaryDeserializer.map(_.toInt)
   */
  // format: on
  implicit def tryUnaryDeserializer[T: UnaryDeserializer[Id, *]]: UnaryDeserializer[Try, T] =
    (arguments, inspectors) => Try(id[T].deserialize(arguments, inspectors))

  implicit def optionalUnaryDeserializer[T: UnaryDeserializer[Id, *]]: UnaryDeserializer[Id, Option[T]] =
    (arguments, inspectors) => (arguments.headOption, inspectors.headOption).mapN(id[T].deserialize)

  // format: off
  /**
   * Derive UnaryDeserializers from ExpressionEncoders.
   * Intentionally not used for instances implementation, causes the following failure on DataBricks;
   * TypeTags are not Kryo serializable by default:
   *   org.apache.spark.SparkException: Job aborted due to stage failure: Task serialization failed: com.esotericsoftware.kryo.KryoException: java.util.ConcurrentModificationException
   *   Serialization trace:
   *     classes (sun.misc.Launcher$AppClassLoader)
   *     classloader (java.security.ProtectionDomain)
   *     context (java.security.AccessControlContext)
   *     acc (com.databricks.backend.daemon.driver.ClassLoaders$LibraryClassLoader)
   *     classLoader (scala.reflect.runtime.JavaMirrors$JavaMirror)
   *     mirror (scala.reflect.api.TypeTags$TypeTagImpl)
   *     evidence$3$1 (com.azavea.hiveless.serializers.UnaryDeserializer$$anonfun$expressionEncoderUnaryDeserializer$2)
   *     evidence$1$1 (com.azavea.hiveless.serializers.UnaryDeserializer$$anonfun$tryUnaryDeserializer$3)
   *     dh$1 (com.azavea.hiveless.serializers.GenericDeserializer$$anon$4)
   *     d$2 (com.azavea.hiveless.serializers.GenericDeserializer$$anon$2)
   */
  // format: on
  def expressionEncoderUnaryDeserializer[T: TypeTag: ExpressionEncoder]: UnaryDeserializer[Id, T] =
    (arguments, inspectors) => arguments.deserialize[InternalRow](inspectors).as[T]

  /** Derivation helper deserializer. */
  implicit val hnilUnaryDeserializer: UnaryDeserializer[Id, HNil] = (_, _) => HNil

  implicit val cnilUnaryDeserializer: UnaryDeserializer[Id, CNil] = (_, _) => null.asInstanceOf[CNil]

  /** Spark internal deserializers. */
  implicit val internalRowUnaryDeserializer: UnaryDeserializer[Id, InternalRow] =
    (arguments, inspectors) => unwrap[InternalRow](arguments.head.getNonEmpty, inspectors.head)

  implicit val utf8StringUnaryDeserializer: UnaryDeserializer[Id, UTF8String] =
    (arguments, inspectors) => unwrap[UTF8String](arguments.head.getNonEmpty, inspectors.head)

  implicit val decimalUnaryDeserializer: UnaryDeserializer[Id, Decimal] =
    (arguments, inspectors) => unwrap[Decimal](arguments.head.getNonEmpty, inspectors.head)

  implicit val arrayDataUnaryDeserializer: UnaryDeserializer[Id, ArrayData] =
    (arguments, inspectors) => unwrap[ArrayData](arguments.head.getNonEmpty, inspectors.head)

  val nativeDoubleUnaryDeserializer: UnaryDeserializer[Id, Double] =
    (arguments, inspectors) =>
      Try(unwrap[Double](arguments.head.getNonEmpty, inspectors.head))
        .getOrElse(unwrap[Int](arguments.head.getNonEmpty, inspectors.head).toDouble)

  val nativeFloatUnaryDeserializer: UnaryDeserializer[Id, Float] =
    (arguments, inspectors) =>
      Try(unwrap[Float](arguments.head.getNonEmpty, inspectors.head))
        .getOrElse(unwrap[Int](arguments.head.getNonEmpty, inspectors.head).toFloat)

  val nativeLongUnaryDeserializer: UnaryDeserializer[Id, Long] =
    (arguments, inspectors) =>
      Try(unwrap[Long](arguments.head.getNonEmpty, inspectors.head))
        .getOrElse(unwrap[Int](arguments.head.getNonEmpty, inspectors.head).toLong)

  val nativeIntUnaryDeserializer: UnaryDeserializer[Id, Int] =
    (arguments, inspectors) => unwrap[Int](arguments.head.getNonEmpty, inspectors.head)

  val nativeShortUnaryDeserializer: UnaryDeserializer[Id, Short] =
    (arguments, inspectors) =>
      Try(unwrap[Short](arguments.head.getNonEmpty, inspectors.head))
        .getOrElse(unwrap[Int](arguments.head.getNonEmpty, inspectors.head).toShort)

  val nativeByteUnaryDeserializer: UnaryDeserializer[Id, Byte] =
    (arguments, inspectors) =>
      Try(unwrap[Byte](arguments.head.getNonEmpty, inspectors.head))
        .getOrElse(unwrap[Int](arguments.head.getNonEmpty, inspectors.head).toByte)

  def nativeArrayUnaryDeserializer[T]: UnaryDeserializer[Id, Array[T]] =
    (arguments, inspectors) => unwrap[Array[T]](arguments.head.getNonEmpty, inspectors.head)

  /** JvmRepr deserializers. */
  implicit val doubleUnaryDeserializer: UnaryDeserializer[Id, Double] =
    (arguments, inspectors) =>
      Try(decimalUnaryDeserializer.deserialize(arguments, inspectors).toDouble)
        .getOrElse(nativeDoubleUnaryDeserializer.deserialize(arguments, inspectors))

  implicit val floatUnaryDeserializer: UnaryDeserializer[Id, Float] =
    (arguments, inspectors) =>
      Try(decimalUnaryDeserializer.deserialize(arguments, inspectors).toFloat)
        .getOrElse(nativeFloatUnaryDeserializer.deserialize(arguments, inspectors))

  implicit val longUnaryDeserializer: UnaryDeserializer[Id, Long] =
    (arguments, inspectors) =>
      Try(decimalUnaryDeserializer.deserialize(arguments, inspectors).toLong)
        .getOrElse(nativeLongUnaryDeserializer.deserialize(arguments, inspectors))

  implicit val intUnaryDeserializer: UnaryDeserializer[Id, Int] =
    (arguments, inspectors) =>
      Try(decimalUnaryDeserializer.deserialize(arguments, inspectors).toInt)
        .getOrElse(nativeIntUnaryDeserializer.deserialize(arguments, inspectors))

  implicit val shortUnaryDeserializer: UnaryDeserializer[Id, Short] =
    (arguments, inspectors) =>
      Try(decimalUnaryDeserializer.deserialize(arguments, inspectors).toShort)
        .getOrElse(nativeShortUnaryDeserializer.deserialize(arguments, inspectors))

  implicit val byteUnaryDeserializer: UnaryDeserializer[Id, Byte] =
    (arguments, inspectors) =>
      Try(decimalUnaryDeserializer.deserialize(arguments, inspectors).toByte)
        .getOrElse(nativeByteUnaryDeserializer.deserialize(arguments, inspectors))

  implicit val stringUnaryDeserializer: UnaryDeserializer[Id, String] =
    (arguments, inspectors) => utf8StringUnaryDeserializer.deserialize(arguments, inspectors).toString

  implicit def seqUnaryDeserializer[T: HSerializer: ClassTag]: UnaryDeserializer[Id, Array[T]] =
    (arguments, inspectors) =>
      Try(arrayDataUnaryDeserializer.deserialize(arguments, inspectors).toArray[T](HSerializer[T].dataType))
        .getOrElse(nativeArrayUnaryDeserializer.deserialize(arguments, inspectors))

  implicit def unaryDeserializerCCons[H, T <: Coproduct](implicit
    dh: UnaryDeserializer[Id, H],
    dt: UnaryDeserializer[Id, T]
  ): UnaryDeserializer[Id, H :+: T] =
    (arguments, inspectors) => Try(dh.deserialize(arguments, inspectors)).map(Inl(_)).getOrElse(Inr(dt.deserialize(arguments, inspectors)))
}
