/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriteStore;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.hadoop.CodecFactory.BytesCompressor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport.FinalizedWriteContext;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ManualInternalParquetRecordWriter<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ManualInternalParquetRecordWriter.class);

    private final ParquetFileWriter parquetFileWriter;
    private final WriteSupport<T> writeSupport;
    private final MessageType schema;
    private final Map<String, String> extraMetaData;
    private final BytesCompressor compressor;
    private final boolean validating;
    private final ParquetProperties props;

    private boolean closed;

    private long recordCount = 0;
    private long lastRowGroupEndPos = 0;

    private ColumnWriteStore columnStore;
    private ColumnChunkPageWriteStore pageStore;
    private BloomFilterWriteStore bloomFilterWriteStore;
    private RecordConsumer recordConsumer;

    private InternalFileEncryptor fileEncryptor;
    private int rowGroupOrdinal;

    /**
     * @param parquetFileWriter the file to write to
     * @param writeSupport the class to convert incoming records
     * @param schema the schema of the records
     * @param extraMetaData extra meta data to write in the footer of the file
     * @param compressor the codec used to compress
     */
    public ManualInternalParquetRecordWriter(
            ParquetFileWriter parquetFileWriter,
            WriteSupport<T> writeSupport,
            MessageType schema,
            Map<String, String> extraMetaData,
            BytesCompressor compressor,
            boolean validating,
            ParquetProperties props) {
        this.parquetFileWriter = parquetFileWriter;
        this.writeSupport = Objects.requireNonNull(writeSupport, "writeSupport cannot be null");
        this.schema = schema;
        this.extraMetaData = extraMetaData;
        this.compressor = compressor;
        this.validating = validating;
        this.props = props;
        this.fileEncryptor = parquetFileWriter.getEncryptor();
        this.rowGroupOrdinal = 0;
        initStore();
    }

    public ParquetMetadata getFooter() {
        return parquetFileWriter.getFooter();
    }

    private void initStore() {
        ColumnChunkPageWriteStore columnChunkPageWriteStore = new ColumnChunkPageWriteStore(compressor,
                schema, props.getAllocator(), props.getColumnIndexTruncateLength(), props.getPageWriteChecksumEnabled(),
                fileEncryptor, rowGroupOrdinal);
        pageStore = columnChunkPageWriteStore;
        bloomFilterWriteStore = columnChunkPageWriteStore;

        columnStore = props.newColumnWriteStore(schema, pageStore, bloomFilterWriteStore);
        MessageColumnIO columnIO = new ColumnIOFactory(validating).getColumnIO(schema);
        this.recordConsumer = columnIO.getRecordWriter(columnStore);
        writeSupport.prepareForWrite(recordConsumer);
    }

    public void close() throws IOException, InterruptedException {
        if (!closed) {
            flushRowGroupToStore();
            FinalizedWriteContext finalWriteContext = writeSupport.finalizeWrite();
            Map<String, String> finalMetadata = new HashMap<String, String>(extraMetaData);
            String modelName = writeSupport.getName();
            if (modelName != null) {
                finalMetadata.put(ParquetWriter.OBJECT_MODEL_NAME_PROP, modelName);
            }
            finalMetadata.putAll(finalWriteContext.getExtraMetaData());
            parquetFileWriter.end(finalMetadata);
            closed = true;
        }
    }

    public void write(T value) {
        writeSupport.write(value);
        ++ recordCount;
    }

    /**
     * @return the total size of data written to the file and buffered in memory
     */
    public long getDataSize() {
        return lastRowGroupEndPos + columnStore.getBufferedSize();
    }

    // manually control blocks partitioning
    public void nextBlock() throws IOException {
        flushRowGroupToStore();
        initStore();
        this.lastRowGroupEndPos = parquetFileWriter.getPos();
    }

    private void flushRowGroupToStore() throws IOException {
        recordConsumer.flush();
        LOG.debug("Flushing mem columnStore to file. allocated memory: {}", columnStore.getAllocatedSize());

        if (recordCount > 0) {
            rowGroupOrdinal++;
            parquetFileWriter.startBlock(recordCount);
            columnStore.flush();
            pageStore.flushToFileWriter(parquetFileWriter);
            recordCount = 0;
            parquetFileWriter.endBlock();
        }

        columnStore = null;
        pageStore = null;
    }

    MessageType getSchema() {
        return this.schema;
    }
}
