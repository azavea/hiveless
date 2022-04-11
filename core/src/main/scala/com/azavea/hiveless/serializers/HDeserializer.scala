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
import cats.{Apply, Functor, Id}
import cats.syntax.apply._
import cats.syntax.functor._
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.util.ArrayData
import shapeless.ops.coproduct.{CoproductToEither, EitherToCoproduct}
import shapeless.{:+:, ::, CNil, Coproduct, Generic, HList, HNil, Inl, Inr, IsTuple}

import scala.reflect.{classTag, ClassTag}
import scala.util.Try
import scala.reflect.runtime.universe.TypeTag

trait HDeserializer[F[_], T] extends Serializable {
  def deserialize(arguments: Array[GenericUDF.DeferredObject], inspectors: Array[ObjectInspector]): F[T]

  def deserialize(argument: GenericUDF.DeferredObject, inspector: ObjectInspector): F[T] =
    deserialize(Array(argument), Array(inspector))
}

object HDeserializer extends Serializable {
  sealed abstract class Errors(override val getMessage: String) extends RuntimeException
  object Errors {
    final case object NullArgument extends Errors("NULL argument passed.")
    final case class ProductDeserializationError[T: ClassTag, A: HShow](name: String)
        extends Errors(
          s"""
             |${classTag[T]}: could not deserialize the $name input argument,
             |it should match one of the following types: ${HShow[A].show()}""".stripMargin
        )
  }

  def apply[F[_], T](implicit ev: HDeserializer[F, T]): HDeserializer[F, T] = ev

  def id[T](implicit ev: HDeserializer[Id, T]): HDeserializer[Id, T] = ev

  // format: off
  /**
   * On DataBricks:
   *   1. Spark throws java.lang.NullPointerException, investigate reasons; happens due to the FunctionK usage i.e. HDeserializer[Id, T].mapK(λ[Id ~> Try](Try(_)))
   *   2. Anonymous functions are not allowed as well: Functor for HDeserializer[F, *]
   *        fails with unable to find class: com.azavea.hiveless.serializers.HDeserializer$$$Lambda$4321/1862326200
   *        i.e. decimalHDeserializer.map(_.toInt)
   */
  // format: on
  implicit def tryHDeserializer[T: HDeserializer[Id, *]]: HDeserializer[Try, T] =
    (arguments, inspectors) => Try(id[T].deserialize(arguments, inspectors))

  implicit def optionalHDeserializer[T: HDeserializer[Id, *]]: HDeserializer[Id, Option[T]] =
    (arguments, inspectors) => (arguments.headOption, inspectors.headOption).mapN(id[T].deserialize)

  // format: off
  /**
   * Derive HDeserializers from ExpressionEncoders.
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
   *     evidence$3$1 (com.azavea.hiveless.serializers.HDeserializer$$anonfun$expressionEncoderHDeserializer$2)
   *     evidence$1$1 (com.azavea.hiveless.serializers.HDeserializer$$anonfun$tryHDeserializer$3)
   *     dh$1 (com.azavea.hiveless.serializers.GenericDeserializer$$anon$4)
   *     d$2 (com.azavea.hiveless.serializers.GenericDeserializer$$anon$2)
   */
  // format: on
  def expressionEncoderHDeserializer[T: TypeTag: ExpressionEncoder]: HDeserializer[Id, T] =
    (arguments, inspectors) => arguments.deserialize[InternalRow](inspectors).as[T]

  /** Derivation helper deserializers. */
  implicit val hnilHDeserializer: HDeserializer[Id, HNil] = (_, _) => HNil

  implicit val chnilHDeserializer: HDeserializer[Id, CNil] = (_, _) => null.asInstanceOf[CNil]

  /** Spark internal deserializers. */
  implicit val internalRowHDeserializer: HDeserializer[Id, InternalRow] =
    (arguments, inspectors) => unwrap[InternalRow](arguments.head.getNonEmpty, inspectors.head)

  implicit val utf8StringHDeserializer: HDeserializer[Id, UTF8String] =
    (arguments, inspectors) => unwrap[UTF8String](arguments.head.getNonEmpty, inspectors.head)

  implicit val decimalHDeserializer: HDeserializer[Id, Decimal] =
    (arguments, inspectors) => unwrap[Decimal](arguments.head.getNonEmpty, inspectors.head)

  implicit val arrayDataHDeserializer: HDeserializer[Id, ArrayData] =
    (arguments, inspectors) => unwrap[ArrayData](arguments.head.getNonEmpty, inspectors.head)

  val nativeDoubleHDeserializer: HDeserializer[Id, Double] =
    (arguments, inspectors) =>
      Try(unwrap[Double](arguments.head.getNonEmpty, inspectors.head))
        .getOrElse(unwrap[Int](arguments.head.getNonEmpty, inspectors.head).toDouble)

  val nativeFloatHDeserializer: HDeserializer[Id, Float] =
    (arguments, inspectors) =>
      Try(unwrap[Float](arguments.head.getNonEmpty, inspectors.head))
        .getOrElse(unwrap[Int](arguments.head.getNonEmpty, inspectors.head).toFloat)

  val nativeLongHDeserializer: HDeserializer[Id, Long] =
    (arguments, inspectors) =>
      Try(unwrap[Long](arguments.head.getNonEmpty, inspectors.head))
        .getOrElse(unwrap[Int](arguments.head.getNonEmpty, inspectors.head).toLong)

  val nativeIntHDeserializer: HDeserializer[Id, Int] =
    (arguments, inspectors) => unwrap[Int](arguments.head.getNonEmpty, inspectors.head)

  val nativeShortHDeserializer: HDeserializer[Id, Short] =
    (arguments, inspectors) =>
      Try(unwrap[Short](arguments.head.getNonEmpty, inspectors.head))
        .getOrElse(unwrap[Int](arguments.head.getNonEmpty, inspectors.head).toShort)

  val nativeByteHDeserializer: HDeserializer[Id, Byte] =
    (arguments, inspectors) =>
      Try(unwrap[Byte](arguments.head.getNonEmpty, inspectors.head))
        .getOrElse(unwrap[Int](arguments.head.getNonEmpty, inspectors.head).toByte)

  def nativeArrayHDeserializer[T]: HDeserializer[Id, Array[T]] =
    (arguments, inspectors) => unwrap[Array[T]](arguments.head.getNonEmpty, inspectors.head)

  /** JvmRepr deserializers. */
  implicit val doubleHDeserializer: HDeserializer[Id, Double] =
    (arguments, inspectors) =>
      Try(decimalHDeserializer.deserialize(arguments, inspectors).toDouble)
        .getOrElse(nativeDoubleHDeserializer.deserialize(arguments, inspectors))

  implicit val floatHDeserializer: HDeserializer[Id, Float] =
    (arguments, inspectors) =>
      Try(decimalHDeserializer.deserialize(arguments, inspectors).toFloat)
        .getOrElse(nativeFloatHDeserializer.deserialize(arguments, inspectors))

  implicit val longHDeserializer: HDeserializer[Id, Long] =
    (arguments, inspectors) =>
      Try(decimalHDeserializer.deserialize(arguments, inspectors).toLong)
        .getOrElse(nativeLongHDeserializer.deserialize(arguments, inspectors))

  implicit val intHDeserializer: HDeserializer[Id, Int] =
    (arguments, inspectors) =>
      Try(decimalHDeserializer.deserialize(arguments, inspectors).toInt)
        .getOrElse(nativeIntHDeserializer.deserialize(arguments, inspectors))

  implicit val shortHDeserializer: HDeserializer[Id, Short] =
    (arguments, inspectors) =>
      Try(decimalHDeserializer.deserialize(arguments, inspectors).toShort)
        .getOrElse(nativeShortHDeserializer.deserialize(arguments, inspectors))

  implicit val byteHDeserializer: HDeserializer[Id, Byte] =
    (arguments, inspectors) =>
      Try(decimalHDeserializer.deserialize(arguments, inspectors).toByte)
        .getOrElse(nativeByteHDeserializer.deserialize(arguments, inspectors))

  implicit val stringHDeserializer: HDeserializer[Id, String] =
    (arguments, inspectors) => utf8StringHDeserializer.deserialize(arguments, inspectors).toString

  implicit def seqHDeserializer[T: HSerializer: ClassTag]: HDeserializer[Id, Array[T]] =
    (arguments, inspectors) =>
      Try(arrayDataHDeserializer.deserialize(arguments, inspectors).toArray[T](HSerializer[T].dataType))
        .getOrElse(nativeArrayHDeserializer.deserialize(arguments, inspectors))

  /** Coproduct deserializers. */
  implicit def eitherHDeserializer[L, R, P <: Coproduct](implicit
    etp: EitherToCoproduct.Aux[L, R, P],
    dp: HDeserializer[Id, P],
    pte: CoproductToEither.Aux[P, Either[L, R]]
  ): HDeserializer[Id, Either[L, R]] =
    (arguments, inspectors) => pte(dp.deserialize(arguments, inspectors))

  implicit def cconsHDeserializer[H, T <: Coproduct](implicit
    dh: HDeserializer[Id, H],
    dt: HDeserializer[Id, T]
  ): HDeserializer[Id, H :+: T] =
    (arguments, inspectors) => Try(dh.deserialize(arguments, inspectors)).map(Inl(_)).getOrElse(Inr(dt.deserialize(arguments, inspectors)))

  /** HList deserializers. */
  // format: off
  /**
   * Intentionally not converted into lambda expression, causes the following failure on DataBricks:
   *   Unable to find class: com.azavea.hiveless.serializers.HDeserializer$$$Lambda$4543/585871703
   */
  // format: on
  implicit def tupleHDeserializer[F[_]: Functor, T: IsTuple, L <: HList](implicit
    gen: Generic.Aux[T, L],
    d: HDeserializer[F, L]
  ): HDeserializer[F, T] = new HDeserializer[F, T] {
    def deserialize(arguments: Array[GenericUDF.DeferredObject], inspectors: Array[ObjectInspector]): F[T] =
      d.deserialize(arguments, inspectors).map(gen.from)
  }

  // format: off
  /**
   * WARNING: wrapping arguments into shapeless.Lazy causes the following failure on DataBricks:
   *   Serialization trace:
   *     t$1 (shapeless.Lazy$$anon$1)
   *     dh$1 (com.azavea.hiveless.serializers.HDeserializer$$anon$2)
   *     inst$macro$7$1 (com.azavea.hiveless.spatial.ST_Contains$$anonfun$$lessinit$greater$2)
   *     t$1 (shapeless.Lazy$$anon$1)
   *     dt$1 (com.azavea.hiveless.serializers.HDeserialize$$anon$2)
   */
  // format: on
  implicit def hconsHDeserializer[F[_]: Apply, H, T <: HList](implicit
    dh: HDeserializer[F, H],
    dt: HDeserializer[F, T]
  ): HDeserializer[F, H :: T] = new HDeserializer[F, H :: T] {
    def deserialize(arguments: Array[GenericUDF.DeferredObject], inspectors: Array[ObjectInspector]): F[H :: T] =
      // take and drop allow us to handle options safely
      // take is left for semantics reasons only
      (dh.deserialize(arguments.take(1), inspectors.take(1)), dt.deserialize(arguments.drop(1), inspectors.drop(1))).mapN(_ :: _)
  }
}
