package com.azavea.ghive.jts.udf.serializers

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hive.HiveInspectorsExposed
import org.apache.spark.sql.jts.GeometryUDT
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

import scala.util.Try

trait UnaryDeserializer[F[_], T] {
  def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): F[T]
}

object UnaryDeserializer {
  def apply[T](implicit ev: TUnaryDeserializer[T]): TUnaryDeserializer[T] = ev

  implicit def stringUnaryDeserializer: UnaryDeserializer[Try, String] = new UnaryDeserializer[Try, String] {
    def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): Try[String] = Try {
      val deser = data.head
      val arg   = arguments.head
      HiveInspectorsExposed.unwrap[UTF8String](arg.get, deser).toString
    }
  }

  implicit def geometryUnaryDeserializer: UnaryDeserializer[Try, Geometry] = new UnaryDeserializer[Try, Geometry] {
    def deserialize(arguments: Array[GenericUDF.DeferredObject])(implicit data: Array[ObjectInspector]): Try[Geometry] = Try {
      val deser = data.head
      val arg   = arguments.head
      GeometryUDT.deserialize(HiveInspectorsExposed.unwrap[InternalRow](arg.get, deser))
    }
  }
}
