package com.azavea.geoparquet

import geotrellis.layer.{LayoutLevel, SpatialKey, ZoomedLayoutScheme}
import geotrellis.proj4.LatLng
import geotrellis.store.index.zcurve.Z2
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom._
import org.apache.spark.sql.functions.udf
import org.locationtech.geomesa.spark.jts.util.WKBUtils
import org.locationtech.geomesa.spark.jts.util.WKTUtils

import java.util.concurrent.TimeUnit
import scala.io.StdIn

object ParquetPartitionedTest {
  // parquet input, which is the write function output
  val input = "/tmp/gadm_lvl2-blocked-001m-partitioned-sorted-dropped.parquet"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("geoparquet")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    var ssc: SparkSession = null
    try {
      ssc = SparkSession.builder().config(sparkConf).getOrCreate()
      write_parquet(ssc)
      // query_by_bbox(ssc)
      // /tmp/gadm_lvl2-old.parquet
      // query_by_jts(ssc)
      // query_by_z2(ssc)

      println("Hit enter to exit.")
      StdIn.readLine()

    } finally if (ssc != null) ssc.stop()
  }

  val toWKB = { wkt: String =>
    try {
      val geom = WKTUtils.read(wkt)
      WKBUtils.write(geom)
    } catch {
      case e: Exception =>
        println(e)
        Array.empty[Byte]
    }
  }

  def scaleLat(lat: Double): Int = {
    (lat + 90) / 180 * (1 << 30)
  }.toInt

  def scaleLong(lng: Double): Int = {
    (lng + 180) / 360 * (1 << 30)
  }.toInt

  def z2index(x: Double, y: Double): Long =
    Z2(scaleLong(x), scaleLat(y)).z

  val toMinZ2 = udf { wkt: String =>
    val geom = WKTUtils.read(wkt)
    val env  = geom.getEnvelopeInternal()
    z2index(env.getMinX, env.getMinY)
  }

  val toMaxZ2 = udf { wkt: String =>
    val geom = WKTUtils.read(wkt)
    val env  = geom.getEnvelopeInternal()
    z2index(env.getMaxX, env.getMaxY)
  }

  // Switzerland-ish
  val xmin = 7.02439457
  val xmax = 7.93392696
  val ymin = 46.42925416
  val ymax = 47.19834404

  def time[R](block: => R): R = {
    val t0     = System.nanoTime()
    val result = block // call-by-name
    val t1     = System.nanoTime()
    val d      = t1 - t0
    println(s"Elapsed time: ${TimeUnit.NANOSECONDS.toSeconds(d)} s")
    println(s"Elapsed time: ${TimeUnit.NANOSECONDS.toMillis(d)} ms")
    result
  }

  def write_parquet(ssc: SparkSession): Unit = {
    import ssc.implicits._

    val path     = "/tmp/gadm_lvl2.csv"
    val udfToWKB = udf(toWKB)

    val layoutScheme           = ZoomedLayoutScheme(LatLng)
    val LayoutLevel(_, layout) = layoutScheme.levelForZoom(10)

    val udfParitionCentroid = udf { wkt: String =>
      val geom                 = WKTUtils.read(wkt)
      val p                    = geom.getEnvelopeInternal().centre()
      val SpatialKey(col, row) = layout.mapTransform(p.getX, p.getY)
      Z2(col, row).z >> 8
    }

    val udfBBOX = udf { wkt: String =>
      val geom = WKTUtils.read(wkt)
      val env  = geom.getEnvelopeInternal()
      bbox(env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
    }

    val df = ssc.read
      .options(Map("header" -> "true", "sep" -> "\t"))
      .csv(path)
      .withColumn("z2_min", toMinZ2($"geom"))
      .withColumn("z2_max", toMaxZ2($"geom"))
      .withColumn("bbox", udfBBOX($"geom"))
      .withColumn("partition", udfParitionCentroid($"geom"))
      .withColumn("geom", udfToWKB($"geom"))

    df
      // .limit(650)
      // .repartition(200) // spatial partitioning
      .repartition($"partition")
      .sortWithinPartitions("partition", "z2_min", "z2_max")
      // we use z2_min / max for sorting
      // and partitioin for partitioing
      // we don't need it to perform queries
      .drop($"z2_min")
      .drop($"z2_max")
      .drop($"partition")
      .write
      // public static final String BLOCK_SIZE = "parquet.block.size";
      //    public static final String PAGE_SIZE = "parquet.page.size";
      .option("parquet.block.size", (0.01 * 1024 * 1024).toInt) // 128 * 1024 * 1024
      // .option("parquet.page.size", "64") // 1 * 1024 * 1024
      // .option("parquet.compression", "UNCOMPRESSED") // one of: UNCOMPRESSED, SNAPPY, GZIP, LZO, ZSTD. Default: UNCOMPRESSED. Supersedes mapred.output.compress*
      // .bucketBy(200, "z2_max")
      // .sortBy("z2_min")
      // .partitionBy("z2_min")
      .mode("overwrite")
      .option("parquet.enable.dictionary", "true")
      // .option("parquet.bloom.filter.enabled#z2_min", "true")
      // .option("parquet.bloom.filter.expected.ndv#z2_min", "1000000000")
      .parquet(input)
  }

  def query_by_bbox(ssc: SparkSession): Unit = {
    import ssc.implicits._

    val df = ssc.read
      .parquet(input)
      .where(
        $"bbox.xmin" >= xmin and
          $"bbox.xmax" <= xmax and
          $"bbox.ymin" >= ymin and
          $"bbox.ymax" <= ymax
      )

    for (_ <- 0 until 10)
      time {
        df.collect().length
      }
  }

  def query_by_z2(ssc: SparkSession): Unit = {
    import ssc.implicits._

    val z2bboxMin = z2index(xmin, ymin)
    val z2bboxMax = z2index(xmax, ymax)
    println("min: " + z2bboxMin)
    println("max: " + z2bboxMax)

    val df = ssc.read.parquet(input).where($"z2_min" >= z2bboxMin and $"z2_max" <= z2bboxMax)

    for (_ <- 0 until 10)
      time {
        df.collect().length
      }
  }

  def query_by_jts(ssc: SparkSession): Unit = {
    import ssc.implicits._
    val env = new Envelope(7.02439457, 7.93392696, 46.42925416, 47.19834404)

    val myIntersects = udf { wkb: Array[Byte] =>
      val geom = WKBUtils.read(wkb)
      geom.getEnvelopeInternal().intersects(env)
    }

    val df = ssc.read.parquet(input).where(myIntersects($"geom"))

    for (_ <- 0 until 10)
      time {
        df.collect().length
      }
    // df.show(300)
    // println("Count: " + df.count())
  }
}
