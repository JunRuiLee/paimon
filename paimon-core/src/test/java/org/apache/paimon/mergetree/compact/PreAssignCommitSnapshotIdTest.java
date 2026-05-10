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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestDataGenerator;
import org.apache.paimon.mergetree.SortedRun;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MergeTreeCompactRewriter#preAssignCommitSnapshotId}. */
public class PreAssignCommitSnapshotIdTest {

    private final DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().build();

    @Test
    public void testAllStampedTakesMax() {
        DataFileMeta f1 = gen.next().meta.assignCommitSnapshotId(5L);
        DataFileMeta f2 = gen.next().meta.assignCommitSnapshotId(10L);
        DataFileMeta output = gen.next().meta;

        List<List<SortedRun>> sections =
                Collections.singletonList(
                        Arrays.asList(SortedRun.fromSingle(f1), SortedRun.fromSingle(f2)));

        List<DataFileMeta> result =
                newRewriter(true)
                        .preAssignCommitSnapshotId(Collections.singletonList(output), sections);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).commitSnapshotId()).isEqualTo(10L);
    }

    @Test
    public void testAnyUnstampedLeavesOutputUnstamped() {
        DataFileMeta stamped = gen.next().meta.assignCommitSnapshotId(10L);
        DataFileMeta unstamped = gen.next().meta;
        DataFileMeta output = gen.next().meta;

        List<List<SortedRun>> sections =
                Collections.singletonList(
                        Arrays.asList(
                                SortedRun.fromSingle(stamped), SortedRun.fromSingle(unstamped)));

        List<DataFileMeta> result =
                newRewriter(true)
                        .preAssignCommitSnapshotId(Collections.singletonList(output), sections);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).commitSnapshotId()).isNull();
    }

    @Test
    public void testAllUnstampedLeavesOutputUnstamped() {
        DataFileMeta f1 = gen.next().meta;
        DataFileMeta f2 = gen.next().meta;
        DataFileMeta output = gen.next().meta;

        List<List<SortedRun>> sections =
                Collections.singletonList(
                        Arrays.asList(SortedRun.fromSingle(f1), SortedRun.fromSingle(f2)));

        List<DataFileMeta> result =
                newRewriter(true)
                        .preAssignCommitSnapshotId(Collections.singletonList(output), sections);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).commitSnapshotId()).isNull();
    }

    @Test
    public void testDisabledReturnsOutputUnchanged() {
        DataFileMeta stamped = gen.next().meta.assignCommitSnapshotId(10L);
        DataFileMeta output = gen.next().meta;

        List<List<SortedRun>> sections =
                Collections.singletonList(Collections.singletonList(SortedRun.fromSingle(stamped)));

        List<DataFileMeta> result =
                newRewriter(false)
                        .preAssignCommitSnapshotId(Collections.singletonList(output), sections);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).commitSnapshotId()).isNull();
    }

    private static MergeTreeCompactRewriter newRewriter(boolean snapshotSequenceOrdering) {
        return new MergeTreeCompactRewriter(
                null, null, null, null, null, null, snapshotSequenceOrdering);
    }
}
