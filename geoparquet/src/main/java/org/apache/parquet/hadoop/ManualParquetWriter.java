package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Map;

// make it scala
public class ManualParquetWriter<T> extends ParquetWriter<T> {

    private final ManualInternalParquetRecordWriter<T> writer;
    private final CodecFactory codecFactory;

    public ManualParquetWriter(Path file, WriteSupport<T> writeSupport, CompressionCodecName compressionCodecName, int pageSize, Configuration conf, Map<String, String> extraMetaData) throws IOException {
        // super(file, writeSupport, compressionCodecName, DEFAULT_BLOCK_SIZE, pageSize);
        this(
                HadoopOutputFile.fromPath(file, conf),
                ParquetFileWriter.Mode.OVERWRITE, // TODO: a bit dirty, should only be OVERWRITE
                writeSupport,
                compressionCodecName,
                extraMetaData,
                DEFAULT_BLOCK_SIZE,
                DEFAULT_IS_VALIDATING_ENABLED,
                conf,
                MAX_PADDING_SIZE_DEFAULT,
                ParquetProperties.builder()
                        .withPageSize(pageSize)
                        .withDictionaryPageSize(pageSize)
                        .withDictionaryEncoding(DEFAULT_IS_DICTIONARY_ENABLED)
                        .withWriterVersion(DEFAULT_WRITER_VERSION)
                        .build(), null
        );
    }

    ManualParquetWriter(
            OutputFile file,
            ParquetFileWriter.Mode mode,
            WriteSupport<T> writeSupport,
            CompressionCodecName compressionCodecName,
            Map<String, String> extraMetaData,
            long rowGroupSize,
            boolean validating,
            Configuration conf,
            int maxPaddingSize,
            ParquetProperties encodingProps,
            FileEncryptionProperties encryptionProperties) throws IOException {

        // constructor creates a file, ParquetFileWriter.Mode.CREATE would not work in this case
        super(file, mode, writeSupport, compressionCodecName, rowGroupSize, validating, conf, maxPaddingSize, encodingProps, encryptionProperties);

        WriteSupport.WriteContext writeContext = writeSupport.init(conf);
        MessageType schema = writeContext.getSchema();

        // encryptionProperties could be built from the implementation of EncryptionPropertiesFactory when it is attached.
        if (encryptionProperties == null) {
            String path = file == null ? null : file.getPath();
            encryptionProperties = ParquetOutputFormat.createEncryptionProperties(conf,
                    path == null ? null : new Path(path), writeContext);
        }

        ParquetFileWriter fileWriter = new ParquetFileWriter(
                file, schema, mode, rowGroupSize, maxPaddingSize,
                encodingProps.getColumnIndexTruncateLength(), encodingProps.getStatisticsTruncateLength(),
                encodingProps.getPageWriteChecksumEnabled(), encryptionProperties);
        fileWriter.start();

        this.codecFactory = new CodecFactory(conf, encodingProps.getPageSizeThreshold());
        CodecFactory.BytesCompressor compressor = codecFactory.getCompressor(compressionCodecName);
        extraMetaData.putAll(writeContext.getExtraMetaData());
        this.writer = new ManualInternalParquetRecordWriter(
                fileWriter,
                writeSupport,
                schema,
                extraMetaData,
                compressor,
                validating,
                encodingProps
        );
    }

    @Override
    public void write(T object) {
        writer.write(object);
    }

    public void nextBlock() throws IOException {
        writer.nextBlock();
    }

    @Override
    public void close() throws IOException {
        try {
            writer.close();
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            // release after the writer closes in case it is used for a last flush
            codecFactory.release();
        }
    }

    /**
     * @return the ParquetMetadata written to the (closed) file.
     */
    @Override
    public ParquetMetadata getFooter() {
        return writer.getFooter();
    }

    /**
     * @return the total size of data written to the file and buffered in memory
     */
    @Override
    public long getDataSize() {
        return writer.getDataSize();
    }
}
