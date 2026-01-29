/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.io;

import org.apache.paimon.append.ProjectedFileWriter;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.UriReader;
import org.apache.paimon.utils.UriReaderFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Blob writer for reference storage mode.
 *
 * <p>In reference mode, the catalog maps logical BLOB columns to {@code BINARY} in the physical
 * write schema. This writer adapts those binary columns as {@link Blob} values, appends blob bytes
 * into {@code .blob} files, and captures the corresponding {@link
 * org.apache.paimon.data.BlobDescriptor} as a lightweight reference stored in data files.
 */
public class BlobReferenceFileWriter implements Closeable {

    private final List<BlobProjectedFileWriter> blobWriters;
    @Nullable private final BinaryBlobRow blobRowWrapper;
    @Nullable private final DescriptorBlobRow descriptorRowWrapper;

    /**
     * Create a writer that persists blob bytes and captures reference descriptors.
     *
     * <p>The write schema must already map logical blob columns to {@code BINARY}.
     */
    public BlobReferenceFileWriter(
            FileIO fileIO,
            long schemaId,
            RowType writeSchema,
            DataFilePathFactory pathFactory,
            Supplier<LongCounter> seqNumCounterSupplier,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            long targetFileSize,
            @Nullable BlobConsumer blobConsumer,
            @Nullable BlobConsumer descriptorConsumer,
            List<String> blobFieldNames,
            boolean blobAsDescriptor,
            @Nullable UriReaderFactory uriReaderFactory) {
        RowType blobRowType = BlobType.splitBlob(writeSchema).getRight();
        checkArgument(
                blobRowType.getFieldCount() == 0,
                "Reference storage expects blob fields to be mapped to BINARY in the write schema.");
        this.blobRowWrapper = new BinaryBlobRow(writeSchema.getFieldCount());
        if (blobAsDescriptor) {
            checkArgument(
                    uriReaderFactory != null,
                    "uriReaderFactory is required when blob-as-descriptor is enabled.");
            this.descriptorRowWrapper =
                    new DescriptorBlobRow(writeSchema.getFieldCount(), uriReaderFactory);
        } else {
            this.descriptorRowWrapper = null;
        }
        BlobConsumer combinedConsumer = combineConsumers(blobConsumer, descriptorConsumer);

        this.blobWriters = new ArrayList<>();
        for (String blobFieldName : blobFieldNames) {
            BlobFileFormat blobFileFormat = new BlobFileFormat();
            blobFileFormat.setWriteConsumer(combinedConsumer);
            RowType writerRowType = writerRowType(writeSchema, blobFieldName);
            blobWriters.add(
                    new BlobProjectedFileWriter(
                            () ->
                                    new RowDataFileWriter(
                                            fileIO,
                                            RollingFileWriter.createFileWriterContext(
                                                    blobFileFormat,
                                                    writerRowType,
                                                    new SimpleColStatsCollector.Factory[] {
                                                        NoneSimpleColStatsCollector::new
                                                    },
                                                    "none"),
                                            pathFactory.newBlobPath(),
                                            writerRowType,
                                            schemaId,
                                            seqNumCounterSupplier,
                                            new FileIndexOptions(),
                                            fileSource,
                                            asyncFileWrite,
                                            statsDenseStore,
                                            pathFactory.isExternalPath(),
                                            singletonList(blobFieldName)),
                            targetFileSize,
                            writeSchema.projectIndexes(singletonList(blobFieldName))));
        }
    }

    public void write(InternalRow row) throws IOException {
        InternalRow toWrite = wrapRowIfNeeded(row);
        for (BlobProjectedFileWriter blobWriter : blobWriters) {
            blobWriter.write(toWrite);
        }
    }

    public void abort() {
        for (BlobProjectedFileWriter blobWriter : blobWriters) {
            blobWriter.abort();
        }
    }

    @Override
    public void close() throws IOException {
        for (BlobProjectedFileWriter blobWriter : blobWriters) {
            blobWriter.close();
        }
    }

    public List<DataFileMeta> result() throws IOException {
        List<DataFileMeta> results = new ArrayList<>();
        for (BlobProjectedFileWriter blobWriter : blobWriters) {
            results.addAll(blobWriter.result());
        }
        return results;
    }

    /**
     * Wraps the input row so blob fields can be accessed as {@link Blob} values during write.
     *
     * <p>If {@code blob-as-descriptor} is enabled, descriptor bytes are converted to {@link Blob}
     * references on the fly.
     */
    private InternalRow wrapRowIfNeeded(InternalRow row) {
        if (blobRowWrapper == null) {
            return row;
        }
        if (descriptorRowWrapper != null) {
            descriptorRowWrapper.replaceRow(row);
            return descriptorRowWrapper;
        }
        blobRowWrapper.replaceRow(row);
        return blobRowWrapper;
    }

    private RowType writerRowType(RowType writeSchema, String blobFieldName) {
        return new RowType(
                Collections.singletonList(toBlobField(writeSchema.getField(blobFieldName))));
    }

    private static DataField toBlobField(DataField field) {
        return new DataField(
                field.id(),
                field.name(),
                new BlobType(field.type().isNullable()),
                field.description());
    }

    private static BlobConsumer combineConsumers(
            @Nullable BlobConsumer blobConsumer, @Nullable BlobConsumer descriptorConsumer) {
        return (blobFieldName, blobDescriptor) -> {
            if (descriptorConsumer != null) {
                descriptorConsumer.accept(blobFieldName, blobDescriptor);
            }
            return blobConsumer != null && blobConsumer.accept(blobFieldName, blobDescriptor);
        };
    }

    private static class BlobProjectedFileWriter
            extends ProjectedFileWriter<
                    RollingFileWriterImpl<InternalRow, DataFileMeta>, List<DataFileMeta>> {
        public BlobProjectedFileWriter(
                Supplier<? extends SingleFileWriter<InternalRow, DataFileMeta>> writerFactory,
                long targetFileSize,
                int[] projection) {
            super(new RollingFileWriterImpl<>(writerFactory, targetFileSize), projection);
        }
    }

    private static class BinaryBlobRow extends ProjectedRow {

        private BinaryBlobRow(int fieldCount) {
            super(identity(fieldCount));
        }

        @Override
        public Blob getBlob(int pos) {
            byte[] bytes = row.getBinary(pos);
            return bytes == null ? null : new BlobData(bytes);
        }

        private static int[] identity(int fieldCount) {
            int[] mapping = new int[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                mapping[i] = i;
            }
            return mapping;
        }
    }

    private static class DescriptorBlobRow extends ProjectedRow {

        private final UriReaderFactory uriReaderFactory;

        private DescriptorBlobRow(int fieldCount, UriReaderFactory uriReaderFactory) {
            super(identity(fieldCount));
            this.uriReaderFactory = uriReaderFactory;
        }

        private static int[] identity(int fieldCount) {
            int[] mapping = new int[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                mapping[i] = i;
            }
            return mapping;
        }

        @Override
        public Blob getBlob(int pos) {
            // Interpret descriptor bytes as a BlobRef so the blob writer can stream external data.
            byte[] bytes = row.getBinary(pos);
            if (bytes == null) {
                return null;
            }
            BlobDescriptor descriptor = BlobDescriptor.deserialize(bytes);
            UriReader uriReader = uriReaderFactory.create(descriptor.uri());
            return Blob.fromDescriptor(uriReader, descriptor);
        }
    }
}
