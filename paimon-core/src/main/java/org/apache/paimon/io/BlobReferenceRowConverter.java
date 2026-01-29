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
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.UriReader;
import org.apache.paimon.utils.UriReaderFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Converts blob reference bytes (serialized {@link BlobDescriptor}) to blob data when reading rows.
 */
public class BlobReferenceRowConverter {

    private final int[] fieldIndexes;
    private final InternalRowSerializer serializer;
    private final UriReaderFactory uriReaderFactory;

    private BlobReferenceRowConverter(
            int[] fieldIndexes, RowType rowType, CatalogContext catalogContext) {
        this.fieldIndexes = fieldIndexes;
        this.serializer = new InternalRowSerializer(rowType);
        this.uriReaderFactory = new UriReaderFactory(catalogContext);
    }

    @Nullable
    public static BlobReferenceRowConverter create(
            RowType readRowType,
            List<String> blobFieldNames,
            boolean blobAsDescriptor,
            @Nullable CatalogContext catalogContext,
            Map<String, String> options) {
        return createWithOffset(
                readRowType,
                readRowType,
                0,
                blobFieldNames,
                blobAsDescriptor,
                catalogContext,
                options);
    }

    @Nullable
    public static BlobReferenceRowConverter createWithOffset(
            RowType indexRowType,
            RowType rowType,
            int fieldIndexOffset,
            List<String> blobFieldNames,
            boolean blobAsDescriptor,
            @Nullable CatalogContext catalogContext,
            Map<String, String> options) {
        // The offset accounts for cases like key-value sequence numbers prepended to the row.
        if (blobAsDescriptor || blobFieldNames.isEmpty()) {
            return null;
        }

        int[] indexes =
                blobFieldNames.stream()
                        .mapToInt(indexRowType::getFieldIndex)
                        .filter(index -> index >= 0)
                        .map(index -> index + fieldIndexOffset)
                        .toArray();
        if (indexes.length == 0) {
            return null;
        }

        CatalogContext context =
                catalogContext != null
                        ? catalogContext
                        : CatalogContext.create(new Options(options));
        return new BlobReferenceRowConverter(indexes, rowType, context);
    }

    public RecordReader<InternalRow> wrap(RecordReader<InternalRow> reader) {
        return new RecordReader<InternalRow>() {
            @Nullable
            @Override
            public RecordIterator<InternalRow> readBatch() throws IOException {
                RecordIterator<InternalRow> iterator = reader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return new RecordIterator<InternalRow>() {
                    @Nullable
                    @Override
                    public InternalRow next() throws IOException {
                        InternalRow row = iterator.next();
                        if (row == null) {
                            return null;
                        }
                        return convert(row);
                    }

                    @Override
                    public void releaseBatch() {
                        iterator.releaseBatch();
                    }
                };
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    public InternalRow convert(InternalRow row) throws IOException {
        if (fieldIndexes.length == 0) {
            return row;
        }

        boolean hasValue = false;
        for (int index : fieldIndexes) {
            if (index >= 0 && !row.isNullAt(index)) {
                hasValue = true;
                break;
            }
        }
        if (!hasValue) {
            return row;
        }

        GenericRow copied =
                (GenericRow) serializer.copyRowData(row, new GenericRow(row.getFieldCount()));
        for (int index : fieldIndexes) {
            if (index < 0 || row.isNullAt(index)) {
                copied.setField(index, null);
                continue;
            }
            byte[] descriptorBytes = row.getBinary(index);
            if (descriptorBytes == null) {
                copied.setField(index, null);
                continue;
            }
            BlobDescriptor descriptor = BlobDescriptor.deserialize(descriptorBytes);
            UriReader uriReader = uriReaderFactory.create(descriptor.uri());
            Blob blob = Blob.fromDescriptor(uriReader, descriptor);
            copied.setField(index, blob.toData());
        }
        return copied;
    }
}
