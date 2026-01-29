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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.UriReaderFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BlobReferenceKeyValueWriterTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testWriteReplacesBlobWithReferenceBytes() throws Exception {
        CapturingWriter delegate = new CapturingWriter();
        BlobReferenceKeyValueWriter writer =
                newWriter(delegate, false, null);

        byte[] blobBytes = new byte[] {7, 8, 9};
        KeyValue kv =
                new KeyValue()
                        .replace(GenericRow.of(1), RowKind.INSERT, GenericRow.of(1, blobBytes));
        writer.write(kv);

        InternalRow stored = delegate.lastRecord.value();
        byte[] referenceBytes = stored.getBinary(1);
        BlobDescriptor descriptor = BlobDescriptor.deserialize(referenceBytes);
        assertDescriptorReads(descriptor, blobBytes, defaultUriReaderFactory());

        CommitIncrement increment = writer.prepareCommit(true);
        assertThat(increment.newFilesIncrement().newFiles()).isNotEmpty();
        assertThat(increment.newFilesIncrement().newFiles().get(0).fileName()).endsWith(".blob");
    }

    @Test
    void testWriteWithDescriptorInput() throws Exception {
        byte[] blobBytes = new byte[] {10, 11, 12};
        BlobDescriptor inputDescriptor = writeExternalBlob(blobBytes);
        UriReaderFactory uriReaderFactory = defaultUriReaderFactory();

        CapturingWriter delegate = new CapturingWriter();
        BlobReferenceKeyValueWriter writer =
                newWriter(delegate, true, uriReaderFactory);

        KeyValue kv =
                new KeyValue()
                        .replace(
                                GenericRow.of(1),
                                RowKind.INSERT,
                                GenericRow.of(1, inputDescriptor.serialize()));
        writer.write(kv);

        InternalRow stored = delegate.lastRecord.value();
        byte[] referenceBytes = stored.getBinary(1);
        BlobDescriptor descriptor = BlobDescriptor.deserialize(referenceBytes);
        assertDescriptorReads(descriptor, blobBytes, uriReaderFactory);
    }

    private BlobReferenceKeyValueWriter newWriter(
            CapturingWriter delegate,
            boolean blobAsDescriptor,
            @Nullable UriReaderFactory uriReaderFactory) {
        return new BlobReferenceKeyValueWriter(
                delegate,
                newValueType(),
                Collections.singletonList("picture"),
                blobAsDescriptor,
                LocalFileIO.create(),
                1L,
                newPathFactory(),
                false,
                false,
                1024 * 1024,
                uriReaderFactory,
                null);
    }

    private DataFilePathFactory newPathFactory() {
        return new DataFilePathFactory(
                new Path(tempDir + "/bucket-0"),
                "parquet",
                "data-",
                "changelog",
                false,
                null,
                null);
    }

    private RowType newValueType() {
        return RowType.of(
                new DataType[] {DataTypes.INT(), DataTypes.BYTES()},
                new String[] {"id", "picture"});
    }

    private UriReaderFactory defaultUriReaderFactory() {
        Options options = new Options();
        options.set("warehouse", tempDir.toString());
        return new UriReaderFactory(CatalogContext.create(options));
    }

    private BlobDescriptor writeExternalBlob(byte[] blobBytes) throws Exception {
        java.nio.file.Path external = tempDir.resolve("external_blob");
        try (OutputStream outputStream = Files.newOutputStream(external)) {
            outputStream.write(blobBytes);
        }
        String uri = "file://" + external.toAbsolutePath();
        return new BlobDescriptor(uri, 0, blobBytes.length);
    }

    private void assertDescriptorReads(
            BlobDescriptor descriptor, byte[] expected, UriReaderFactory uriReaderFactory) {
        Blob blob = Blob.fromDescriptor(uriReaderFactory.create(descriptor.uri()), descriptor);
        assertThat(blob.toData()).isEqualTo(expected);
    }

    private static class CapturingWriter implements RecordWriter<KeyValue> {

        @Nullable private KeyValue lastRecord;

        @Override
        public void write(KeyValue record) {
            this.lastRecord = record;
        }

        @Override
        public void compact(boolean fullCompaction) {}

        @Override
        public void addNewFiles(List<DataFileMeta> files) {}

        @Override
        public java.util.Collection<DataFileMeta> dataFiles() {
            return Collections.emptyList();
        }

        @Override
        public long maxSequenceNumber() {
            return 0;
        }

        @Override
        public CommitIncrement prepareCommit(boolean waitCompaction) {
            return new CommitIncrement(
                    DataIncrement.emptyIncrement(),
                    new CompactIncrement(
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.emptyList()),
                    null);
        }

        @Override
        public boolean compactNotCompleted() {
            return false;
        }

        @Override
        public void sync() {}

        @Override
        public void close() {}
    }
}
