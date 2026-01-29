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

package org.apache.paimon.operation;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.BlobReferenceFileWriter;
import org.apache.paimon.io.BlobReferenceValueAdapter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.UriReaderFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

/**
 * A {@link RecordWriter} wrapper that writes blob files and stores blob references in value
 * columns.
 *
 * <p>For primary-key tables, blob columns are stored as serialized {@link BlobDescriptor} bytes in
 * data files. This writer persists blob contents to {@code .blob} files and replaces blob values
 * with reference bytes before delegating to the underlying writer.
 */
class BlobReferenceKeyValueWriter implements RecordWriter<KeyValue>, MemoryOwner {

    private final RecordWriter<KeyValue> delegate;
    private final RowType valueType;
    private final List<String> blobFieldNames;
    private final boolean blobAsDescriptor;
    private final FileIO fileIO;
    private final long schemaId;
    private final DataFilePathFactory pathFactory;
    private final boolean asyncFileWrite;
    private final boolean statsDenseStore;
    private final long blobTargetFileSize;
    @Nullable private final UriReaderFactory uriReaderFactory;
    @Nullable private final BlobConsumer blobConsumer;

    private final Map<String, BlobDescriptor> descriptors = new HashMap<>();
    @Nullable private BlobReferenceValueAdapter valueAdapter;
    @Nullable private BlobReferenceFileWriter blobWriter;
    private final KeyValue reuseKeyValue = new KeyValue();

    BlobReferenceKeyValueWriter(
            RecordWriter<KeyValue> delegate,
            RowType valueType,
            List<String> blobFieldNames,
            boolean blobAsDescriptor,
            FileIO fileIO,
            long schemaId,
            DataFilePathFactory pathFactory,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            long blobTargetFileSize,
            @Nullable UriReaderFactory uriReaderFactory,
            @Nullable BlobConsumer blobConsumer) {
        this.delegate = delegate;
        this.valueType = valueType;
        this.blobFieldNames = blobFieldNames;
        this.blobAsDescriptor = blobAsDescriptor;
        this.fileIO = fileIO;
        this.schemaId = schemaId;
        this.pathFactory = pathFactory;
        this.asyncFileWrite = asyncFileWrite;
        this.statsDenseStore = statsDenseStore;
        this.blobTargetFileSize = blobTargetFileSize;
        this.uriReaderFactory = uriReaderFactory;
        this.blobConsumer = blobConsumer;
    }

    @Override
    public void write(KeyValue record) throws Exception {
        InternalRow valueRow = record.value();
        if (valueRow == null || blobFieldNames.isEmpty()) {
            delegate.write(record);
            return;
        }

        // Prepare adapters/writers lazily to avoid overhead when blob fields are not present.
        if (valueAdapter == null) {
            valueAdapter = new BlobReferenceValueAdapter(valueType, blobFieldNames);
        }

        if (blobWriter == null) {
            BlobConsumer descriptorConsumer =
                    (blobFieldName, blobDescriptor) -> {
                        descriptors.put(blobFieldName, blobDescriptor);
                        return false;
                    };
            Supplier<LongCounter> seqNumCounterSupplier = () -> new LongCounter(0);
            blobWriter =
                    new BlobReferenceFileWriter(
                            fileIO,
                            schemaId,
                            valueType,
                            pathFactory,
                            seqNumCounterSupplier,
                            FileSource.APPEND,
                            asyncFileWrite,
                            statsDenseStore,
                            blobTargetFileSize,
                            blobConsumer,
                            descriptorConsumer,
                            blobFieldNames,
                            blobAsDescriptor,
                            uriReaderFactory);
        }

        descriptors.clear();
        blobWriter.write(valueRow);
        byte[][] bytes = new byte[blobFieldNames.size()][];
        for (int i = 0; i < blobFieldNames.size(); i++) {
            int fieldIndex = valueType.getFieldIndex(blobFieldNames.get(i));
            if (fieldIndex < 0 || valueRow.isNullAt(fieldIndex)) {
                bytes[i] = null;
                continue;
            }
            BlobDescriptor descriptor = descriptors.get(blobFieldNames.get(i));
            bytes[i] = descriptor == null ? null : descriptor.serialize();
        }
        InternalRow replaced = valueAdapter.replaceWithReferences(valueRow, bytes);
        delegate.write(reuseKeyValue.replace(record.key(), record.valueKind(), replaced));
    }

    @Override
    public void compact(boolean fullCompaction) throws Exception {
        delegate.compact(fullCompaction);
    }

    @Override
    public void addNewFiles(List<DataFileMeta> files) {
        delegate.addNewFiles(files);
    }

    @Override
    public Collection<DataFileMeta> dataFiles() {
        return delegate.dataFiles();
    }

    @Override
    public long maxSequenceNumber() {
        return delegate.maxSequenceNumber();
    }

    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        CommitIncrement increment = delegate.prepareCommit(waitCompaction);
        List<DataFileMeta> blobFiles = closeBlobWriter();
        if (blobFiles.isEmpty()) {
            return increment;
        }

        DataIncrement oldIncrement = increment.newFilesIncrement();
        List<DataFileMeta> newFiles = new ArrayList<>(oldIncrement.newFiles());
        newFiles.addAll(blobFiles);

        DataIncrement newIncrement =
                new DataIncrement(
                        newFiles,
                        oldIncrement.deletedFiles(),
                        oldIncrement.changelogFiles(),
                        oldIncrement.newIndexFiles(),
                        oldIncrement.deletedIndexFiles());
        return new CommitIncrement(
                newIncrement, increment.compactIncrement(), increment.compactDeletionFile());
    }

    @Override
    public boolean compactNotCompleted() {
        return delegate.compactNotCompleted();
    }

    @Override
    public void sync() throws Exception {
        delegate.sync();
    }

    @Override
    public void close() throws Exception {
        try {
            closeBlobWriter();
        } finally {
            delegate.close();
        }
    }

    @Override
    public void setMemoryPool(MemorySegmentPool memoryPool) {
        if (delegate instanceof MemoryOwner) {
            ((MemoryOwner) delegate).setMemoryPool(memoryPool);
        }
    }

    @Override
    public long memoryOccupancy() {
        return delegate instanceof MemoryOwner ? ((MemoryOwner) delegate).memoryOccupancy() : 0;
    }

    @Override
    public void flushMemory() throws Exception {
        if (delegate instanceof MemoryOwner) {
            ((MemoryOwner) delegate).flushMemory();
        }
    }

    private List<DataFileMeta> closeBlobWriter() throws IOException {
        if (blobWriter == null) {
            return Collections.emptyList();
        }
        try {
            blobWriter.close();
            return blobWriter.result();
        } finally {
            blobWriter = null;
            descriptors.clear();
        }
    }
}
