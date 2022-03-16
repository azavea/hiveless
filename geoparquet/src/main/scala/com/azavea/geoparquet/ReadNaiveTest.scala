package com.azavea.geoparquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.internal.column.columnindex.OffsetIndex
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.schema.Type

import scala.collection.mutable.ListBuffer

object ReadNaiveTest {
  def main(args: Array[String]): Unit = {
    // val path = "/tmp/spark_output.snappy.parquet"
    val path = "/tmp/java_write_output.snappy.parquet"

    val res = ParquetReaderUtils.getParquetDataIndex(path)

    println(s"res.schema: ${res.schema}")
    println(s"res.data.length: ${res.data.length}")
  }
}

import scala.collection.JavaConverters._

object ParquetReaderUtils {
  def getParquetDataIndex(filePath: String): Parquet = {
    val simpleGroups = new ListBuffer[SimpleGroup]()
    val reader       = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration))
    val schema       = reader.getFooter.getFileMetaData.getSchema
    val fields       = schema.getFields
    // reader.readOffsetIndex()
    // reader.getRowGroups.asScala.map(_.)
    // RowGroup == Page == BlockMetaData
    lazy val mds: List[BlockMetaData] = reader.getRowGroups.asScala.toList
    // chunks by 100 in this case remember in the morning
    lazy val rowCounts = mds.map(_.getRowCount)
    // sum of all rows - 10000
    lazy val rowCountsSum = mds.map(_.getRowCount).sum
    // column names in fact
    lazy val paths = mds.map(_.getColumns.asScala.map(_.getPath))
    // offsets
    lazy val offsets: List[OffsetIndex] = mds.map(md => reader.readOffsetIndex(md.getColumns.asScala.head))

    // var pages: PageReadStore = reader.readRowGroup(idx)
    var pages: PageReadStore = reader.readNextRowGroup
    while (pages != null) {
      val rows         = pages.getRowCount
      val columnIO     = new ColumnIOFactory().getColumnIO(schema)
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      var i            = 0
      while (i < rows) {
        val simpleGroup = recordReader.read.asInstanceOf[SimpleGroup]
        simpleGroups += simpleGroup

        i += 1
      }
      // pages = reader.readNextRowGroup
      pages = null
    }
    reader.close()
    Parquet(simpleGroups.toList, fields.asScala.toList)
  }

  def getParquetData(filePath: String): Parquet = {
    val simpleGroups = new ListBuffer[SimpleGroup]()
    // is it a single fragment?
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration))

    val schema = reader.getFooter.getFileMetaData.getSchema
    val fields = schema.getFields
    var pages  = reader.readNextRowGroup

    // row1 |
    // row2 |
    // ...  |
    // rowN |

    // group1 -> row 0 -> 99
    // ...
    // grop n -> row 0 -> 99

    while (pages != null) {
      val rows         = pages.getRowCount
      val columnIO     = new ColumnIOFactory().getColumnIO(schema)
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      var i            = 0
      while (i < rows) {
        val simpleGroup = recordReader.read.asInstanceOf[SimpleGroup]
        simpleGroups += simpleGroup

        i += 1
      }
      pages = reader.readNextRowGroup
    }
    reader.close()
    new Parquet(simpleGroups.toList, fields.asScala.toList)
  }
}

case class Parquet(var data: List[SimpleGroup], var schema: List[Type])
