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

import java.io.IOException
import scala.collection.mutable.ListBuffer

object ReadManualTest {
  def main(args: Array[String]): Unit = {
    // val path = "/tmp/gadm_lvl2-blocked.parquet/part-00000-682faa7f-3d07-4a45-a7a3-a5e3b9c38787-c000.snappy.parquet"
    val path = "/tmp/gadm_lvl2-blocked-0001m.parquet/part-00007-275d04b8-72e2-4706-b227-8af1e19fe5f1-c000.snappy.parquet"

    val res = ParquetReaderUtilsManual.getParquetDataIndex(path)

    println(s"res.schema: ${res.schema}")
    println(s"res.data.length: ${res.data.length}")
  }
}

import scala.collection.JavaConverters._

object ParquetReaderUtilsManual {
  def getParquetDataIndex(filePath: String): Parquet = {
    val simpleGroups = new ListBuffer[SimpleGroup]()
    val reader       = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration))
    val schema       = reader.getFooter.getFileMetaData.getSchema

    println("~~~~~~~~~~")
    println(reader.getFooter.getFileMetaData.getKeyValueMetaData.asScala.toList)
    println("~~~~~~~~~~")

    val fields = schema.getFields
    // reader.readOffsetIndex()
    // reader.getRowGroups.asScala.map(_.)
    // RowGroup == Page == BlockMetaData
    lazy val mds: List[BlockMetaData] = reader.getRowGroups.asScala.toList

    println(s"mds.length: ${mds.length}")

    // chunks by 100 in this case remember in the morning
    lazy val rowCounts = mds.map(_.getRowCount)
    // sum of all rows - 10000
    lazy val rowCountsSum = mds.map(_.getRowCount).sum
    // column names in fact
    lazy val paths = mds.map(_.getColumns.asScala.map(_.getPath))
    // offsets

    println("/////////")
    mds.head.getColumns.asScala.foreach { c =>
      println(s"${c.getPath}: ${c.getStatistics}")
    }
    println("/////////")

    lazy val offsets: List[OffsetIndex] = mds.map(md => reader.readOffsetIndex(md.getColumns.asScala.head))

    // println(s"md: ${mds}")
    // println(s"rowCounts: ${rowCounts}")
    // println(s"rowCountsSum: ${rowCountsSum}")
    // println(s"paths: ${paths}")
    println(s"--offsets--")
    offsets.foreach(o => println(o))
    println(s"-----------")

    // var pages2: PageReadStore = reader.readNextRowGroup
    var pages: PageReadStore = reader.readRowGroup(2)
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
