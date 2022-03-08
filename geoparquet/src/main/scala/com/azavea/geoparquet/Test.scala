package com.azavea.geoparquet

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.MetadataBuilder

object Test {

  //org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader
  //org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReader

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("geoparquet")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // .set("spark.kryoserializer.buffer.max", "2040m")

    val ssc = SparkSession.builder().config(sparkConf).getOrCreate()

    try {
      // val path = "/Users/daunnc/subversions/git/github/pomadchin/geoparquet/examples/geoparquet/example_id_pandas_geojson_spark_2.parquet"
      val path = "/Users/daunnc/subversions/git/github/pomadchin/geoparquet/examples/geoparquet/spark_output.snappy.parquet"
      val df = ssc.read.parquet(path)

      println("-------")
      println(df.schema.prettyJson)
      df.printSchema()
      println("-------")
      println(df.collect().toList)
      println("-------")

    } finally ssc.close()
  }

  def main_(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("geoparquet")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .set("spark.kryoserializer.buffer.max", "2040m")

    val ssc = SparkSession.builder().config(sparkConf).getOrCreate()

    import ssc.implicits._

    try {
      val path = "/Users/daunnc/subversions/git/github/pomadchin/geoparquet/examples/geoparquet/example_id_pandas_geojson.parquet"
      val df = ssc.read.parquet(path)

      println("-------")
      println(df.schema.prettyJson)
      df.printSchema()
      println("-------")
      println(df.collect().toList)
      println("-------")

      df.select(
        $"location_id",
        $"list_id",
        $"geometry",
        $"__index_level_0__".as(
          "test",
          new MetadataBuilder()
            .putString("geom", "sparky")
            .build()
        )
      ).write.parquet("/Users/daunnc/subversions/git/github/pomadchin/geoparquet/examples/geoparquet/example_id_pandas_geojson_spark_2.parquet")

    } finally ssc.close()
  }

//  def read(path: String) = {
//    import org.apache.parquet.column.page.PageReadStore
//    import org.apache.parquet.example.data.simple.SimpleGroup
//    import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
//    import org.apache.parquet.hadoop.ParquetFileReader
//    import org.apache.parquet.hadoop.util.HadoopInputFile
//    import org.apache.parquet.io.ColumnIOFactory
//    import org.apache.parquet.io.MessageColumnIO
//    import java.util
//    val simpleGroups = new util.ArrayList[AnyRef]
//    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Nothing(filePath), new Nothing))
//    val schema = reader.getFooter.getFileMetaData.getSchema
//    val fields = schema.getFields
//    var pages = null
//    while ( {
//      (pages = reader.readNextRowGroup) != null
//    }) {
//      val rows = pages.getRowCount
//      val columnIO = new ColumnIOFactory().getColumnIO(schema)
//      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
//      var i = 0
//      while ( {
//        i < rows
//      }) {
//        val simpleGroup = recordReader.read.asInstanceOf[SimpleGroup]
//        simpleGroups.add(simpleGroup)
//
//        i += 1
//      }
//    }
//    reader.close()
//    return new Nothing(simpleGroups, fields)
//  }
}
