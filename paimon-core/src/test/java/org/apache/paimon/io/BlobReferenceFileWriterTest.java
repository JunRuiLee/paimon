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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.UriReaderFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class BlobReferenceFileWriterTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testWriteBinaryBlobData() throws Exception {
        AtomicReference<BlobDescriptor> captured = new AtomicReference<>();
        BlobReferenceFileWriter writer =
                newWriter(captured, false, null);

        byte[] blobBytes = new byte[] {1, 2, 3, 4};
        InternalRow row = GenericRow.of(1, blobBytes);
        writer.write(row);
        writer.close();

        assertDescriptorReads(captured.get(), blobBytes, defaultUriReaderFactory());
        assertThat(writer.result()).isNotEmpty();
    }

    @Test
    void testWriteWithDescriptorInput() throws Exception {
        byte[] blobBytes = new byte[] {9, 8, 7};
        BlobDescriptor inputDescriptor = writeExternalBlob(blobBytes);
        UriReaderFactory uriReaderFactory = defaultUriReaderFactory();

        AtomicReference<BlobDescriptor> captured = new AtomicReference<>();
        BlobReferenceFileWriter writer =
                newWriter(captured, true, uriReaderFactory);

        InternalRow row = GenericRow.of(1, inputDescriptor.serialize());
        writer.write(row);
        writer.close();

        assertDescriptorReads(captured.get(), blobBytes, uriReaderFactory);
        assertThat(writer.result()).isNotEmpty();
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

    private RowType newRowType() {
        return RowType.of(
                new DataType[] {DataTypes.INT(), DataTypes.BYTES()},
                new String[] {"id", "picture"});
    }

    private BlobReferenceFileWriter newWriter(
            AtomicReference<BlobDescriptor> captured,
            boolean blobAsDescriptor,
            @Nullable UriReaderFactory uriReaderFactory) {
        return new BlobReferenceFileWriter(
                LocalFileIO.create(),
                1L,
                newRowType(),
                newPathFactory(),
                LongCounter::new,
                FileSource.APPEND,
                false,
                false,
                1024 * 1024,
                null,
                (name, descriptor) -> {
                    captured.set(descriptor);
                    return false;
                },
                Collections.singletonList("picture"),
                blobAsDescriptor,
                uriReaderFactory);
    }

    private BlobDescriptor writeExternalBlob(byte[] blobBytes) throws Exception {
        java.nio.file.Path external = tempDir.resolve("external_blob");
        try (OutputStream outputStream = Files.newOutputStream(external)) {
            outputStream.write(blobBytes);
        }
        String uri = "file://" + external.toAbsolutePath();
        return new BlobDescriptor(uri, 0, blobBytes.length);
    }

    private UriReaderFactory defaultUriReaderFactory() {
        Options options = new Options();
        options.set("warehouse", tempDir.toString());
        return new UriReaderFactory(CatalogContext.create(options));
    }

    private void assertDescriptorReads(
            BlobDescriptor descriptor, byte[] expected, UriReaderFactory uriReaderFactory) {
        assertThat(descriptor).isNotNull();
        Blob blob = Blob.fromDescriptor(uriReaderFactory.create(descriptor.uri()), descriptor);
        assertThat(blob.toData()).isEqualTo(expected);
    }
}
