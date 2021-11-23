package com.azavea.ghive.jts.udf.serializers

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.spark.sql.hive.HiveInspectorsExposed

import scala.util.Try

trait QuarternaryDeserializer[F[_], T0, T1, T2, T3] {
  def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T0, T1, T2, T3)]
}

object QuarternaryDeserializer {
  def apply[T](implicit ev: TQuarternaryDeserializer[T]): TQuarternaryDeserializer[T] = ev

  implicit def doubleQuarternaryDeserializer: QuarternaryDeserializer[Try, Double, Double, Double, Double] =
    new QuarternaryDeserializer[Try, Double, Double, Double, Double] {
      def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): Try[(Double, Double, Double, Double)] = Try {
        val List(fst, snd, trd, frth) =
          data.toList
            .zip(arguments.toList)
            .map { case (deser, arg) => HiveInspectorsExposed.unwrap[Double](arg.get, deser) }

        println((fst, snd, trd, frth))
        (fst, snd, trd, frth)
      }
    }
}
