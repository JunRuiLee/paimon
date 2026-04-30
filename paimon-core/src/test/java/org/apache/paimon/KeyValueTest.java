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

package org.apache.paimon;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyValue}. */
public class KeyValueTest {

    @Test
    public void testIsCommittedSnapshotId() {
        assertThat(KeyValue.isCommittedSnapshotId(1L)).isTrue();
        assertThat(KeyValue.isCommittedSnapshotId(Long.MAX_VALUE - 1)).isTrue();

        assertThat(KeyValue.isCommittedSnapshotId(0L)).isFalse();
        assertThat(KeyValue.isCommittedSnapshotId(-1L)).isFalse();
        assertThat(KeyValue.isCommittedSnapshotId(KeyValue.UNKNOWN_SNAPSHOT_ID)).isFalse();
        assertThat(KeyValue.isCommittedSnapshotId(Long.MAX_VALUE)).isFalse();
    }

    /**
     * The ordering comparator used under {@code sequence.snapshot-ordering} must sort by per-record
     * snapshotId first, then by sequenceNumber.
     */
    @Test
    public void testSnapshotThenSequenceComparator() {
        Comparator<KeyValue> cmp = KeyValue.snapshotThenSequenceComparator();

        KeyValue older = kv(1L, 100L);
        KeyValue newer = kv(2L, 1L);

        // Later snapshot wins even when its sequence number is smaller than the older one's.
        assertThat(cmp.compare(older, newer)).isNegative();
        assertThat(cmp.compare(newer, older)).isPositive();

        // Same snapshot: tie broken by sequence number.
        KeyValue sameSnapEarly = kv(5L, 10L);
        KeyValue sameSnapLate = kv(5L, 20L);
        assertThat(cmp.compare(sameSnapEarly, sameSnapLate)).isNegative();

        // Identical (snapshotId, seq) compares equal.
        assertThat(cmp.compare(kv(7L, 3L), kv(7L, 3L))).isZero();
    }

    private static KeyValue kv(long snapshotId, long sequenceNumber) {
        return new KeyValue()
                .replace(GenericRow.of(1), sequenceNumber, RowKind.INSERT, GenericRow.of(1))
                .setSnapshotId(snapshotId);
    }

    /**
     * Per-record _COMMIT_SNAPSHOT_ID round-trip through {@link KeyValueSerializer}. Committed ids
     * must survive; UNKNOWN and MAX_VALUE are serialized as null and come back as UNKNOWN (on-disk
     * data files never carry MAX_VALUE — that only lives in the spill serializer).
     */
    @Test
    public void testKeyValueSerializerSnapshotIdRoundTrip() {
        RowType keyType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"k"});
        RowType valueType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"v"});
        KeyValueSerializer serializer = new KeyValueSerializer(keyType, valueType);

        // Committed positive id survives.
        KeyValue committed =
                new KeyValue()
                        .replace(GenericRow.of(1), 100L, RowKind.INSERT, GenericRow.of(42))
                        .setSnapshotId(7L);
        InternalRow committedRow = serializer.toRow(committed);
        assertThat(committedRow.isNullAt(keyType.getFieldCount() + 2)).isFalse();
        // Must deep-copy before calling fromRow again because the serializer reuses state.
        KeyValue restoredCommitted = serializer.fromRow(serializer.toRow(committed));
        assertThat(restoredCommitted.snapshotId()).isEqualTo(7L);

        // UNKNOWN → null → UNKNOWN.
        KeyValue unknown =
                new KeyValue()
                        .replace(GenericRow.of(1), 101L, RowKind.INSERT, GenericRow.of(42))
                        .setSnapshotId(KeyValue.UNKNOWN_SNAPSHOT_ID);
        InternalRow unknownRow = serializer.toRow(unknown);
        assertThat(unknownRow.isNullAt(keyType.getFieldCount() + 2)).isTrue();
        assertThat(serializer.fromRow(serializer.toRow(unknown)).snapshotId())
                .isEqualTo(KeyValue.UNKNOWN_SNAPSHOT_ID);

        // MAX_VALUE (in-txn placeholder) is intentionally written as null in data-file serializers
        // so on-disk files never carry MAX_VALUE; it comes back as UNKNOWN and is resolved via the
        // file-level fallback on read.
        KeyValue inTxn =
                new KeyValue()
                        .replace(GenericRow.of(1), 102L, RowKind.INSERT, GenericRow.of(42))
                        .setSnapshotId(Long.MAX_VALUE);
        InternalRow inTxnRow = serializer.toRow(inTxn);
        assertThat(inTxnRow.isNullAt(keyType.getFieldCount() + 2)).isTrue();
        assertThat(serializer.fromRow(serializer.toRow(inTxn)).snapshotId())
                .isEqualTo(KeyValue.UNKNOWN_SNAPSHOT_ID);
    }
}
