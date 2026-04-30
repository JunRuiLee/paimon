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

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;

/** {@link RecordReader} for reading {@link KeyValue} data files. */
public class KeyValueDataFileRecordReader implements FileRecordReader<KeyValue> {

    private final FileRecordReader<InternalRow> reader;
    private final KeyValueSerializer serializer;
    private final int level;

    private final long fileSnapshotId;

    public KeyValueDataFileRecordReader(
            FileRecordReader<InternalRow> reader,
            RowType keyType,
            RowType valueType,
            int level,
            long fileSnapshotId) {
        this.reader = reader;
        this.serializer = new KeyValueSerializer(keyType, valueType);
        this.level = level;
        this.fileSnapshotId = fileSnapshotId;
    }

    @Nullable
    @Override
    public FileRecordIterator<KeyValue> readBatch() throws IOException {
        FileRecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }

        return iterator.transform(
                internalRow -> {
                    if (internalRow == null) {
                        return null;
                    }
                    KeyValue kv = serializer.fromRow(internalRow).setLevel(level);
                    // Per-record fallback: when the per-record snapshotId is not set (UNKNOWN /
                    // non-positive), inherit the file-level id. File-level Long.MAX_VALUE
                    // (in-txn placeholder stamped by MergeTreeWriter) is intentionally propagated
                    // as-is — it ensures in-txn rows beat already-committed rows during in-txn
                    // compaction, and is rewritten to a real id at commit time.
                    if (!KeyValue.isCommittedSnapshotId(kv.snapshotId()) && fileSnapshotId > 0) {
                        kv.setSnapshotId(fileSnapshotId);
                    }
                    return kv;
                });
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
