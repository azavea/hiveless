package com.azavea.geoparquet

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom._
import org.apache.spark.sql.functions.{col, udf}
import org.locationtech.sfcurve.zorder.Z2

case class bbox(xmin: Double, xmax: Double, ymin: Double, ymax: Double)

object ParquetTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("geoparquet")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = SparkSession.builder().config(sparkConf).getOrCreate()
    try {


    } finally ssc.stop()
  }

  val toWKB = { wkt: String =>
    try {
      val geom = WKT.read(wkt)
      WKB.write(geom)
    } catch {
      case e: Exception =>
        println(e)
        Array.empty[Byte]
    }
  }

  def scaleLat(lat: Double): Int = {
    (lat + 90)/180 * (1 << 30)
  }.toInt

  def scaleLong(lng: Double): Int = {
    (lng + 180)/360 * (1 << 30)
  }.toInt

  def z2index(x: Double, y: Double): Long =
    Z2(scaleLong(x), scaleLat(y)).z

  val toMinZ2 = udf { wkt: String =>
    val geom = WKT.read(wkt)
    val env = geom.getEnvelopeInternal()
    z2index(env.getMinX, env.getMinY)
  }

  val toMaxZ2 = udf { wkt: String =>
    val geom = WKT.read(wkt)
    val env = geom.getEnvelopeInternal()
    z2index(env.getMaxX, env.getMaxY)
  }

  // Switzerland-ish
  val xmin = 7.02439457
  val xmax = 7.93392696
  val ymin = 46.42925416
  val ymax = 47.19834404

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  def write_parquet(ssc: SparkSession): Unit = {
    val path = "/tmp/gadm_lvl2.csv"
    val udfToWKB = udf(toWKB)

    val udfBBOX = udf { wkt: String =>
      val geom = WKT.read(wkt)
      val env = geom.getEnvelopeInternal()
      bbox(env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
    }

    val df = ssc
      .read
      .options(Map("header" -> "true", "sep" -> "\t"))
      .csv(path)
      .withColumn("z2_min", toMinZ2(col("geom")))
      .withColumn("z2_max", toMaxZ2(col("geom")))
      .withColumn("bbox", udfBBOX(col("geom")))
      .withColumn("geom", udfToWKB(col("geom")))

    println("-------")
    df.printSchema()
    println("-------")
    df.show()
    println("-------")

    df.write.mode("overwrite").parquet("/tmp/gadm_lvl2.parquet")
  }

  def query_by_bbox(ssc: SparkSession): Unit = {
    val input = "/tmp/gadm_lvl2.parquet"
    val df = ssc.read.parquet(input)
      .where(
        col("bbox.xmin") >= xmin and
        col("bbox.xmax") <= xmax and
        col("bbox.ymin") >= ymin and
        col("bbox.ymax") <= ymax)

    for (i <- 0 until 10) {
      time {
        df.count()
      }
    }
  }

  def query_by_z2(ssc: SparkSession): Unit = {
    val z2bboxMin = z2index(xmin,ymin)
    val z2bboxMax = z2index(xmax,ymax)
    println("min: " + z2bboxMin)
    println("max: " + z2bboxMax)

    val input = "/tmp/gadm_lvl2.parquet"

    val df = ssc.read.parquet(input)
      .where(col("z2_min") >= z2bboxMin and col("z2_max") <= z2bboxMax)

    for (i <- 0 until 10) {
      time {
        df.count()
      }
    }
  }

  def query_by_jts(ssc: SparkSession): Unit = {
    val env = new Envelope(7.02439457, 7.93392696, 46.42925416, 47.19834404)

    val myIntersects = udf { wkb: Array[Byte] =>
      val geom = WKB.read(wkb)
      geom.getEnvelopeInternal().intersects(env)
    }

    val input = "/tmp/gadm_lvl2.parquet"

    val df = ssc.read.parquet(input)
      .where(myIntersects(col("geom")))

    for (i <- 0 until 10) {
      time {
        df.count()
      }
    }
    //df.show(300)
    //println("Count: " + df.count())
  }
}
