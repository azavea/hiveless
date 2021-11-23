package com.azavea.ghive.jts.udf.serializers

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hive.HiveInspectorsExposed
import org.apache.spark.sql.jts.GeometryUDT
import org.locationtech.jts.geom.Geometry

import scala.util.Try

trait BinaryDeserializer[F[_], T0, T1] {
  def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[(T0, T1)]
}

object BinaryDeserializer {
  def apply[T](implicit ev: TBinaryDeserializer[T]): TBinaryDeserializer[T] = ev

  implicit def geometryBinaryDeserializer: BinaryDeserializer[Try, Geometry, Geometry] = new BinaryDeserializer[Try, Geometry, Geometry] {
    def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): Try[(Geometry, Geometry)] = Try {
      val List(l, r) =
        data.toList
          .zip(arguments.toList)
          .map { case (deser, arg) => GeometryUDT.deserialize(HiveInspectorsExposed.unwrap[InternalRow](arg.get, deser)) }

      (l, r)
    }
  }
}
