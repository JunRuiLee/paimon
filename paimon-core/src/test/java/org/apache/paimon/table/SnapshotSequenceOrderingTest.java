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

package org.apache.paimon.table;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.CoreOptions.BATCH_SCAN_MODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for snapshot-based sequence ordering feature. */
public class SnapshotSequenceOrderingTest extends TableTestBase {

    private Schema createSchema(boolean snapshotOrdering) {
        Schema.Builder builder = Schema.newBuilder();
        builder.column("pk", DataTypes.INT());
        builder.column("val", DataTypes.STRING());
        builder.primaryKey("pk");
        builder.option("bucket", "1");
        builder.option("merge-engine", "deduplicate");
        if (snapshotOrdering) {
            builder.option("sequence.snapshot-ordering", "true");
        }
        return builder.build();
    }

    private FileStoreTable createSnapshotOrderingTable() throws Exception {
        catalog.createTable(identifier(), createSchema(true), false);
        return getTableDefault();
    }

    private GenericRow row(int pk, String val) {
        return GenericRow.of(pk, BinaryString.fromString(val));
    }

    /**
     * Reads the table with compact batch-scan-mode so that level-0 files (which DV tables normally
     * skip in batch scan) are included.
     */
    @SuppressWarnings("unchecked")
    private List<InternalRow> readWithCompactMode(Table table) throws Exception {
        return read(table, Pair.of(BATCH_SCAN_MODE, "compact"));
    }

    private List<DataFileMeta> getAllDataFiles(FileStoreTable table) {
        List<DataSplit> dataSplits = table.newSnapshotReader().read().dataSplits();
        List<DataFileMeta> allFiles = new ArrayList<>();
        for (DataSplit split : dataSplits) {
            allFiles.addAll(split.dataFiles());
        }
        return allFiles;
    }

    // -----------------------------------------------------------------------
    // Test 1: snapshot ordering and sequence.field are mutually exclusive
    // -----------------------------------------------------------------------

    @Test
    public void testSnapshotOrderingRejectsSequenceField() {
        Schema.Builder builder = Schema.newBuilder();
        builder.column("pk", DataTypes.INT());
        builder.column("val", DataTypes.STRING());
        builder.column("seq", DataTypes.BIGINT());
        builder.primaryKey("pk");
        builder.option("bucket", "1");
        builder.option("merge-engine", "deduplicate");
        builder.option("sequence.snapshot-ordering", "true");
        builder.option("sequence.field", "seq");
        Schema schema = builder.build();

        assertThatThrownBy(() -> catalog.createTable(identifier(), schema, false))
                .hasMessageContaining("cannot be used together");
    }

    // -----------------------------------------------------------------------
    // Test 2: commit assigns snapshot id to data files
    // -----------------------------------------------------------------------

    @Test
    public void testCommitAssignsSnapshotId() throws Exception {
        FileStoreTable table = createSnapshotOrderingTable();

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser("writer1");
        try (StreamTableWrite write = writeBuilder.newWrite();
                StreamTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            write.write(row(1, "A"));
            List<CommitMessage> messages = write.prepareCommit(false, 0);
            commit.commit(0, messages);
        }

        // Reload the table to see the committed state
        table = getTableDefault();
        SnapshotManager snapshotManager = table.snapshotManager();
        long latestSnapshotId = snapshotManager.latestSnapshotId();
        assertThat(latestSnapshotId).isEqualTo(1L);

        List<DataFileMeta> files = getAllDataFiles(table);
        assertThat(files).isNotEmpty();
        for (DataFileMeta file : files) {
            assertThat(file.commitSnapshotId()).isNotNull();
            assertThat(file.commitSnapshotId()).isEqualTo(latestSnapshotId);
        }
    }

    // -----------------------------------------------------------------------
    // Test 3: later snapshot wins on merge read
    // -----------------------------------------------------------------------

    @Test
    public void testLaterSnapshotWinsOnMergeRead() throws Exception {
        FileStoreTable table = createSnapshotOrderingTable();

        // Snapshot 1: pk=1, val="A"
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser("writer1");
        try (StreamTableWrite write = writeBuilder.newWrite();
                StreamTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            write.write(row(1, "A"));
            commit.commit(0, write.prepareCommit(false, 0));
        }

        // Snapshot 2: pk=1, val="B"
        table = getTableDefault();
        writeBuilder = table.newStreamWriteBuilder().withCommitUser("writer1");
        try (StreamTableWrite write = writeBuilder.newWrite();
                StreamTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            write.write(row(1, "B"));
            commit.commit(1, write.prepareCommit(false, 1));
        }

        table = getTableDefault();
        List<InternalRow> rows = readWithCompactMode(table);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("B");
    }

    // -----------------------------------------------------------------------
    // Test 4: concurrent writers - later snapshot wins
    // -----------------------------------------------------------------------

    @Test
    public void testConcurrentWritersWithSnapshotOrdering() throws Exception {
        FileStoreTable table = createSnapshotOrderingTable();

        // Writer A prepares its data (pk=1, val="fromA")
        StreamWriteBuilder builderA = table.newStreamWriteBuilder().withCommitUser("jobA");
        StreamTableWrite writeA = builderA.newWrite();
        StreamTableCommit commitA = builderA.newCommit();
        writeA.withIOManager(ioManager);
        writeA.write(row(1, "fromA"));
        List<CommitMessage> messagesA = writeA.prepareCommit(false, 0);

        // Writer B prepares its data (pk=1, val="fromB")
        StreamWriteBuilder builderB = table.newStreamWriteBuilder().withCommitUser("jobB");
        StreamTableWrite writeB = builderB.newWrite();
        StreamTableCommit commitB = builderB.newCommit();
        writeB.withIOManager(ioManager);
        writeB.write(row(1, "fromB"));
        List<CommitMessage> messagesB = writeB.prepareCommit(false, 0);

        // Commit A first → snapshot 1, then B → snapshot 2
        commitA.commit(0, messagesA);
        commitB.commit(0, messagesB);

        writeA.close();
        commitA.close();
        writeB.close();
        commitB.close();

        // Read: B wins because snapshot 2 > snapshot 1
        table = getTableDefault();
        List<InternalRow> rows = readWithCompactMode(table);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("fromB");
    }

    // -----------------------------------------------------------------------
    // Test 5: concurrent writers + compaction produces correct result
    // -----------------------------------------------------------------------

    @Test
    public void testConcurrentWritersCompactionProducesCorrectResult() throws Exception {
        FileStoreTable table = createSnapshotOrderingTable();

        // Writer A: pk=1, val="fromA"
        StreamWriteBuilder builderA = table.newStreamWriteBuilder().withCommitUser("jobA");
        StreamTableWrite writeA = builderA.newWrite();
        StreamTableCommit commitA = builderA.newCommit();
        writeA.withIOManager(ioManager);
        writeA.write(row(1, "fromA"));
        List<CommitMessage> messagesA = writeA.prepareCommit(false, 0);

        // Writer B: pk=1, val="fromB"
        StreamWriteBuilder builderB = table.newStreamWriteBuilder().withCommitUser("jobB");
        StreamTableWrite writeB = builderB.newWrite();
        StreamTableCommit commitB = builderB.newCommit();
        writeB.withIOManager(ioManager);
        writeB.write(row(1, "fromB"));
        List<CommitMessage> messagesB = writeB.prepareCommit(false, 0);

        // Commit A → snapshot 1, B → snapshot 2
        commitA.commit(0, messagesA);
        commitB.commit(0, messagesB);

        writeA.close();
        commitA.close();
        writeB.close();
        commitB.close();

        // Trigger full compaction
        table = getTableDefault();
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // After compaction, B still wins
        table = getTableDefault();
        List<InternalRow> rows = read(table);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("fromB");
    }

    // -----------------------------------------------------------------------
    // Test 6: inline compaction with mixed files
    // -----------------------------------------------------------------------

    @Test
    public void testInlineCompactionWithMixedFiles() throws Exception {
        // Use small target file size to encourage compaction
        Schema.Builder builder = Schema.newBuilder();
        builder.column("pk", DataTypes.INT());
        builder.column("val", DataTypes.STRING());
        builder.primaryKey("pk");
        builder.option("bucket", "1");
        builder.option("merge-engine", "deduplicate");
        builder.option("sequence.snapshot-ordering", "true");
        builder.option("num-sorted-run.compaction-trigger", "2");
        Schema schema = builder.build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();

        // First commit: pk=1 → "first"
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser("writer1");
        try (StreamTableWrite write = writeBuilder.newWrite();
                StreamTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            write.write(row(1, "first"));
            commit.commit(0, write.prepareCommit(false, 0));
        }

        // Second commit with same key to trigger compaction: pk=1 → "second"
        table = getTableDefault();
        writeBuilder = table.newStreamWriteBuilder().withCommitUser("writer1");
        try (StreamTableWrite write = writeBuilder.newWrite();
                StreamTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            write.write(row(1, "second"));
            commit.commit(1, write.prepareCommit(true, 1));
        }

        // Read: "second" wins because it is from a later snapshot
        table = getTableDefault();
        List<InternalRow> rows = read(table);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("second");
    }

    // -----------------------------------------------------------------------
    // Test 7: compaction does not inflate snapshot id
    // -----------------------------------------------------------------------

    @Test
    public void testCompactionDoesNotInflateSnapshotId() throws Exception {
        FileStoreTable table = createSnapshotOrderingTable();

        // Writer A: pk=1, val="fromA" → commits to snapshot 1
        StreamWriteBuilder builderA = table.newStreamWriteBuilder().withCommitUser("jobA");
        StreamTableWrite writeA = builderA.newWrite();
        StreamTableCommit commitA = builderA.newCommit();
        writeA.withIOManager(ioManager);
        writeA.write(row(1, "fromA"));
        List<CommitMessage> messagesA = writeA.prepareCommit(false, 0);
        commitA.commit(0, messagesA);
        writeA.close();
        commitA.close();

        // Writer B: pk=1, val="fromB" → commits to snapshot 2
        table = getTableDefault();
        StreamWriteBuilder builderB = table.newStreamWriteBuilder().withCommitUser("jobB");
        StreamTableWrite writeB = builderB.newWrite();
        StreamTableCommit commitB = builderB.newCommit();
        writeB.withIOManager(ioManager);
        writeB.write(row(1, "fromB"));
        List<CommitMessage> messagesB = writeB.prepareCommit(false, 0);
        commitB.commit(0, messagesB);
        writeB.close();
        commitB.close();

        // Verify we have snapshots 1 and 2
        table = getTableDefault();
        SnapshotManager snapshotManager = table.snapshotManager();
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(2L);

        // Trigger full compaction → creates snapshot 3
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        table = getTableDefault();
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(3L);

        // Check compacted files: commitSnapshotId should be max(1, 2) = 2, NOT 3
        List<DataFileMeta> files = getAllDataFiles(table);
        assertThat(files).isNotEmpty();
        for (DataFileMeta file : files) {
            assertThat(file.commitSnapshotId()).isNotNull();
            assertThat(file.commitSnapshotId())
                    .as(
                            "Compacted file should preserve max input snapshot id (2), not compaction snapshot id (3)")
                    .isEqualTo(2L);
        }

        // Also verify the read result is correct: B still wins
        List<InternalRow> rows = read(table);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("fromB");
    }

    // -----------------------------------------------------------------------
    // Test 8: per-record snapshotId is materialized into compacted files
    // -----------------------------------------------------------------------
    //
    // Note: an end-to-end "hitchhiking" scenario (where per-record beats per-file) cannot be
    // constructed in this integration harness, because a single normal commit path always yields
    // monotonically increasing snapshot ids for newly-written records. The direct proof that the
    // merge comparator consults per-record ids (and would mis-order under hitchhiking) lives in
    // {@code KeyValueTest#testComparatorPreservesLineageAgainstHitchhiking}. Here we only verify
    // the file-level invariant: after compaction, commitSnapshotId equals max(inputs) rather than
    // the compaction's own commit id.

    @Test
    public void testCompactedFileInheritsMaxInputSnapshotId() throws Exception {
        FileStoreTable table = createSnapshotOrderingTable();

        // Snapshot 1: pk1="A1", pk2="A2" (commitSnapshotId=1)
        StreamWriteBuilder wb1 = table.newStreamWriteBuilder().withCommitUser("w1");
        try (StreamTableWrite w = wb1.newWrite();
                StreamTableCommit c = wb1.newCommit()) {
            w.withIOManager(ioManager);
            w.write(row(1, "A1"));
            w.write(row(2, "A2"));
            c.commit(0, w.prepareCommit(false, 0));
        }

        // Snapshot 2: pk3="B3" (commitSnapshotId=2)
        table = getTableDefault();
        StreamWriteBuilder wb2 = table.newStreamWriteBuilder().withCommitUser("w2");
        try (StreamTableWrite w = wb2.newWrite();
                StreamTableCommit c = wb2.newCommit()) {
            w.withIOManager(ioManager);
            w.write(row(3, "B3"));
            c.commit(0, w.prepareCommit(false, 0));
        }

        // Compact: the output file must carry file-level sid=max(1,2)=2, NOT the compaction
        // commit's own snapshot id (which would be 3).
        table = getTableDefault();
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        table = getTableDefault();
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(3L);
        List<DataFileMeta> files = getAllDataFiles(table);
        assertThat(files).isNotEmpty();
        for (DataFileMeta file : files) {
            assertThat(file.commitSnapshotId())
                    .as(
                            "File-level commitSnapshotId should be max of inputs (2), not compaction id (3)")
                    .isEqualTo(2L);
        }

        // And subsequent writes still win as expected.
        write(table, ioManager, row(1, "new"));
        table = getTableDefault();
        List<InternalRow> rows = read(table);
        for (InternalRow r : rows) {
            int pk = r.getInt(0);
            String val = r.getString(1).toString();
            if (pk == 1) {
                assertThat(val).isEqualTo("new");
            } else if (pk == 2) {
                assertThat(val).isEqualTo("A2");
            } else if (pk == 3) {
                assertThat(val).isEqualTo("B3");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Test 9: snapshotId survives multiple compaction rounds
    // -----------------------------------------------------------------------

    @Test
    public void testSnapshotIdSurvivesMultipleCompactionRounds() throws Exception {
        FileStoreTable table = createSnapshotOrderingTable();

        // Snapshot 1: pk=1 → "round1"
        write(table, ioManager, row(1, "round1"));

        // Compact → L0 to L1 (snapshot 2)
        table = getTableDefault();
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Snapshot 3: pk=2 → "round2"
        table = getTableDefault();
        write(table, ioManager, row(2, "round2"));

        // Compact again → L0+L1 to L2 (snapshot 4)
        table = getTableDefault();
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Snapshot 5: pk=3 → "round3"
        table = getTableDefault();
        write(table, ioManager, row(3, "round3"));

        // Compact again (snapshot 6)
        table = getTableDefault();
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Verify all data readable and correct
        table = getTableDefault();
        List<InternalRow> rows = read(table);
        assertThat(rows).hasSize(3);

        // Now write a new value for pk=1 (snapshot 7)
        write(table, ioManager, row(1, "latest"));

        // Read: pk=1 should be "latest" (snapshot 7 > snapshot 1's per-record sid)
        table = getTableDefault();
        rows = readWithCompactMode(table);
        assertThat(rows)
                .filteredOn(r -> r.getInt(0) == 1)
                .as("After 3 rounds of compaction, pk=1 must exist and reflect the latest write")
                .hasSize(1)
                .first()
                .satisfies(r -> assertThat(r.getString(1).toString()).isEqualTo("latest"));
    }

    // -----------------------------------------------------------------------
    // Test 10: MAX_VALUE marker is never persisted after commit
    // -----------------------------------------------------------------------

    @Test
    public void testMaxValueNotPersistedAfterCommit() throws Exception {
        FileStoreTable table = createSnapshotOrderingTable();

        write(table, ioManager, row(1, "hello"));

        // After commit, commitSnapshotId should be a real positive value, not MAX_VALUE
        table = getTableDefault();
        List<DataFileMeta> files = getAllDataFiles(table);
        assertThat(files).isNotEmpty();
        for (DataFileMeta file : files) {
            assertThat(file.commitSnapshotId()).isNotNull();
            assertThat(file.commitSnapshotId())
                    .as("commitSnapshotId should be a real value, not MAX_VALUE")
                    .isNotEqualTo(Long.MAX_VALUE);
            assertThat(file.commitSnapshotId())
                    .as("commitSnapshotId should be positive")
                    .isGreaterThan(0L);
        }
    }
}
