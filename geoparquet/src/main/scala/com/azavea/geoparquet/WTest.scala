package com.azavea.geoparquet

// Generic Avro dependencies
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.bytes.{BytesInput, HeapByteBufferAllocator}
import org.apache.parquet.column.page.PageWriter
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.hadoop.{CodecFactory, ColumnChunkPageWriteStore, ParquetFileReader, ParquetFileWriter}
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.apache.parquet.column.Encoding
import org.apache.parquet.format.converter.ParquetMetadataConverter

// Hadoop stuff
import org.apache.hadoop.fs.Path;

// Generic Parquet dependencies
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetWriter;

// Avro->Parquet dependencies
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroParquetWriter;
// 1.12.2
class UserRank(val id: Int, val rank: Int)

object WTest {

  def main(args: Array[String]): Unit = {
    write2()
  }

  // https://github.com/hydrogen18/write-parquet-example/blob/master/src/main/java/com/hydrogen18/examples/Main.java

  def main_(args: Array[String]): Unit = {
    val avroSchema = ReflectData.get().getSchema(classOf[UserRank])
    val parquetSchema = new AvroSchemaConverter().convert(avroSchema)

    /*val dataToWrite = Array.ofDim[UserRank](3)
    dataToWrite(0) = new UserRank(1, 3)
    dataToWrite(1) = new UserRank(2, 0)
    dataToWrite(2) = new UserRank(3, 100)*/

    val filePath = new Path("/Users/daunnc/subversions/git/github/pomadchin/geoparquet/examples/geoparquet/java_write_output.snappy.parquet");
    val blockSize = 10
    val pageSize = 64

    println(avroSchema)

    val parquetWriter = new AvroParquetWriter[GenericData.Record](
      filePath,
      avroSchema,
      CompressionCodecName.SNAPPY,
      blockSize,
      pageSize
    )

    /*val parquetWriter = AvroParquetWriter
      .builder[GenericData.Record](filePath)
      .withDataModel(ReflectData.get())
      .withSchema(avroSchema)
      .withPageSize(20)
      .build()*/

    (0 until 10000).map { i =>
      val r = new GenericData.Record(avroSchema)
      r.put("id", i)
      r.put("rank", i * 10)
      r
    }.foreach { r => parquetWriter.write(r) }

    /*val r1 = new GenericData.Record(avroSchema)
    r1.put("id", 1)
    r1.put("rank", 3)

    val r2 = new GenericData.Record(avroSchema)
    r2.put("id", 2)
    r2.put("rank", 0)

    val r3 = new GenericData.Record(avroSchema)
    r3.put("id", 3)
    r3.put("rank", 100)*/

    /*val parquetWriter = new AvroParquetWriter[UserRank](
      filePath,
      avroSchema,
      CompressionCodecName.SNAPPY,
      blockSize,
      pageSize
    )*/

    /*parquetWriter.write(r1)
    parquetWriter.write(r2)
    parquetWriter.write(r3)*/

    // for (obj <- dataToWrite.toList) {
      // parquetWriter.write(obj)
   // }

    parquetWriter.close()
  }

  def write2() = {
    val filePath = new Path("/Users/daunnc/subversions/git/github/pomadchin/geoparquet/examples/geoparquet/java_write_output_test.snappy.parquet");
    val file = HadoopOutputFile.fromPath(filePath, new Configuration())

    val avroSchema = ReflectData.get().getSchema(classOf[UserRank])
    val parquetSchema = new AvroSchemaConverter().convert(avroSchema)

    val blockSize = 10
    val pageSize = 64
    val rowCount = 1
    val nullCount = 0
    val valueCount = rowCount - nullCount

    val d = 1
    val r = 2
    val v = 3
    // TODO: what does it mean?
    val definitionLevels = BytesInput.fromInt(d)
    val repetitionLevels = BytesInput.fromInt(r)

    val statistics =
      Statistics
        .getBuilderForReading(Types.required(PrimitiveTypeName.BINARY).named("test_binary"))
        .build()

    val dataEncoding = Encoding.PLAIN

    def compressor(codec: CompressionCodecName) = new CodecFactory(new Configuration(), pageSize).getCompressor(codec)

    val writer = new ParquetFileWriter(
      file,
      parquetSchema,
      Mode.CREATE,
      ParquetWriter.DEFAULT_BLOCK_SIZE,
      ParquetWriter.MAX_PADDING_SIZE_DEFAULT,
      // default params
      64,
      2147483647,
      true
    )

    writer.start()
    writer.startBlock(rowCount)

    val store = new ColumnChunkPageWriteStore(
      compressor(CompressionCodecName.SNAPPY),
      parquetSchema,
      new HeapByteBufferAllocator(),
      Integer.MAX_VALUE
    )

    // https://parquet.apache.org/documentation/latest/#:~:text=To%20encode%20nested%20columns%2C%20Parquet,path%20has%20the%20value%20repeated.
    import scala.collection.JavaConverters._
    parquetSchema.getColumns.asScala.zipWithIndex.foreach { case (col, idx) =>
      val pageWriter: PageWriter = store.getPageWriter(col)
      pageWriter.writePageV2(
        rowCount,
        nullCount,
        valueCount,
        repetitionLevels,
        definitionLevels,
        dataEncoding,
        BytesInput.fromInt(idx * 10),
        statistics
      )
      store.flushToFileWriter(writer);
    }
    writer.endBlock()
    // writer.end(new java.util.HashMap[String, String]())

    writer.startBlock(rowCount)
    parquetSchema.getColumns.asScala.zipWithIndex.foreach { case (col, idx) =>
      val pageWriter: PageWriter = store.getPageWriter(col)
      pageWriter.writePageV2(
        rowCount,
        nullCount,
        valueCount,
        repetitionLevels,
        definitionLevels,
        dataEncoding,
        BytesInput.fromInt((idx + 10) * 10),
        statistics
      )
      store.flushToFileWriter(writer);
    }

    writer.endBlock()

    writer.startBlock(rowCount * 2)
    parquetSchema.getColumns.asScala.zipWithIndex.foreach { case (col, idx) =>
      val pageWriter: PageWriter = store.getPageWriter(col)
      pageWriter.writePageV2(
        rowCount,
        nullCount,
        valueCount,
        repetitionLevels,
        definitionLevels,
        dataEncoding,
        BytesInput.fromInt(((idx + 31) + 10) * 10),
        statistics
      )
      pageWriter.writePageV2(
        rowCount,
        nullCount,
        valueCount,
        repetitionLevels,
        definitionLevels,
        dataEncoding,
        BytesInput.fromInt(((idx + 32) + 10) * 10),
        statistics
      )
      store.flushToFileWriter(writer);
    }
    writer.endBlock()

    writer.end(new java.util.HashMap[String, String]())

    // val footer = ParquetFileReader.readFooter(new Configuration(), filePath, ParquetMetadataConverter.NO_FILTER)
    // val reader = new ParquetFileReader(new Configuration(), footer.getFileMetaData(), filePath, footer.getBlocks(), parquetSchema.getColumns())
  }
}

// https://github.com/apache/parquet-mr/blob/5608695f5777de1eb0899d9075ec9411cfdf31d3/parquet-hadoop/src/test/java/org/apache/parquet/hadoop/TestColumnChunkPageWriteStore.java#L161-L181




