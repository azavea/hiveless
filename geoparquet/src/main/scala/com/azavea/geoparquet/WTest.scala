package com.azavea.geoparquet

// Generic Avro dependencies
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.reflect.ReflectData;

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
  // https://github.com/hydrogen18/write-parquet-example/blob/master/src/main/java/com/hydrogen18/examples/Main.java

  def main(args: Array[String]): Unit = {
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

}
