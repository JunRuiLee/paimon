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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Targeted tests for the snapshot-id tiebreaker in {@link SortMergeReaderWithLoserTree} and {@link
 * SortMergeReaderWithMinHeap}. These exercise the comparator path directly without going through
 * the file-store stack so we can construct the exact (key, sequence, snapshot) tuples that are
 * interesting.
 */
public class SortMergeSnapshotOrderingTest {

    private static final Comparator<InternalRow> KEY_COMPARATOR =
            (a, b) -> Integer.compare(a.getInt(0), b.getInt(0));

    @ParameterizedTest
    @EnumSource(SortEngine.class)
    public void testLaterSnapshotWinsOverHigherSequence(SortEngine sortEngine) throws IOException {
        // Two writers commit to the same key. Writer A produces a record with sequence number 100
        // committed in snapshot 5. Writer B produces a record with the smaller sequence number 50
        // but commits later in snapshot 6. With snapshot-ordering enabled, B must win.
        KeyValue olderHigherSeq = kv(1, 100L, RowKind.INSERT, 100, 5L);
        KeyValue newerLowerSeq = kv(1, 50L, RowKind.INSERT, 200, 6L);

        List<KeyValue> result = runMerge(sortEngine, list(olderHigherSeq), list(newerLowerSeq));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).value().getInt(0)).isEqualTo(200);
        assertThat(result.get(0).snapshotId()).isEqualTo(6L);
    }

    @ParameterizedTest
    @EnumSource(SortEngine.class)
    public void testFallsBackToSequenceWhenSnapshotMissing(SortEngine sortEngine)
            throws IOException {
        // Records without snapshot ids (UNKNOWN) must continue to use sequence ordering.
        KeyValue seq50 = kv(1, 50L, RowKind.INSERT, 1, KeyValue.UNKNOWN_SNAPSHOT_ID);
        KeyValue seq100 = kv(1, 100L, RowKind.INSERT, 2, KeyValue.UNKNOWN_SNAPSHOT_ID);

        List<KeyValue> result = runMerge(sortEngine, list(seq50), list(seq100));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).value().getInt(0)).isEqualTo(2);
    }

    @Test
    public void testSameSnapshotFallsBackToSequence() throws IOException {
        // Two records share a snapshot id (same writer, same commit). Sequence number breaks the
        // tie just like before.
        KeyValue a = kv(1, 50L, RowKind.INSERT, 1, 7L);
        KeyValue b = kv(1, 100L, RowKind.INSERT, 2, 7L);

        List<KeyValue> result = runMerge(SortEngine.LOSER_TREE, list(a), list(b));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).value().getInt(0)).isEqualTo(2);
    }

    @ParameterizedTest
    @EnumSource(SortEngine.class)
    public void testStampedAlwaysBeatsUnstamped(SortEngine sortEngine) throws IOException {
        // An unstamped record (e.g. data written before snapshot-ordering was enabled) must lose
        // to any stamped record, regardless of sequence numbers. This also keeps the comparator
        // transitive — without it, mixed unstamped/stamped data could form ordering cycles that
        // confuse LoserTree / PriorityQueue.
        KeyValue unstampedHighSeq = kv(1, 999L, RowKind.INSERT, 1, KeyValue.UNKNOWN_SNAPSHOT_ID);
        KeyValue stampedLowSeq = kv(1, 1L, RowKind.INSERT, 2, 1L);

        List<KeyValue> result = runMerge(sortEngine, list(unstampedHighSeq), list(stampedLowSeq));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).value().getInt(0)).isEqualTo(2);
    }

    // ------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------

    private static List<KeyValue> list(KeyValue... kvs) {
        List<KeyValue> l = new ArrayList<>();
        for (KeyValue kv : kvs) {
            l.add(kv);
        }
        return l;
    }

    private static KeyValue kv(int key, long seq, RowKind kind, int value, long snapshotId) {
        KeyValue kv = new KeyValue();
        kv.replace(GenericRow.of(key), seq, kind, GenericRow.of(value)).setLevel(0);
        kv.setSnapshotId(snapshotId);
        return kv;
    }

    @SuppressWarnings("unchecked")
    private List<KeyValue> runMerge(SortEngine sortEngine, List<KeyValue>... readers)
            throws IOException {
        List<RecordReader<KeyValue>> wrapped = new ArrayList<>();
        for (List<KeyValue> data : readers) {
            wrapped.add(new InMemoryReader(data));
        }
        ReducerMergeFunctionWrapper wrapper =
                new ReducerMergeFunctionWrapper(DeduplicateMergeFunction.factory().create());
        SortMergeReader<KeyValue> reader =
                SortMergeReader.createSortMergeReader(
                        wrapped, KEY_COMPARATOR, null, wrapper, sortEngine, true);
        List<KeyValue> out = new ArrayList<>();
        RecordReader.RecordIterator<KeyValue> iter;
        while ((iter = reader.readBatch()) != null) {
            KeyValue r;
            while ((r = iter.next()) != null) {
                out.add(r);
            }
            iter.releaseBatch();
        }
        reader.close();
        return out;
    }

    /** Reader that returns the configured KVs as a single batch. */
    private static class InMemoryReader implements RecordReader<KeyValue> {
        private final Iterator<KeyValue> delegate;
        private boolean batchReturned = false;

        InMemoryReader(List<KeyValue> kvs) {
            this.delegate = kvs.iterator();
        }

        @Override
        public RecordIterator<KeyValue> readBatch() {
            if (batchReturned) {
                return null;
            }
            batchReturned = true;
            return new RecordIterator<KeyValue>() {
                @Override
                public KeyValue next() {
                    return delegate.hasNext() ? delegate.next() : null;
                }

                @Override
                public void releaseBatch() {}
            };
        }

        @Override
        public void close() {}
    }
}
