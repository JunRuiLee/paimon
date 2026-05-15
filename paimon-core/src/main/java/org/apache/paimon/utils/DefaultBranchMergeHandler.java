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

package org.apache.paimon.utils;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Default implementation of {@link BranchMergeHandler} backed by {@link FileStoreTable}. */
public class DefaultBranchMergeHandler implements BranchMergeHandler {

    private final Function<String, FileStoreTable> branchTableFactory;

    public DefaultBranchMergeHandler(Function<String, FileStoreTable> branchTableFactory) {
        this.branchTableFactory = branchTableFactory;
    }

    @Override
    public Map<FileEntry.Identifier, ManifestEntry> readBranchFiles(String branch) {
        FileStoreTable branchTable = branchTableFactory.apply(branch);
        Snapshot snapshot = branchTable.snapshotManager().latestSnapshot();
        checkArgument(
                snapshot != null,
                "Cannot read branch '%s', because it does not have any snapshot.",
                branch);
        ManifestList manifestList = branchTable.store().manifestListFactory().create();
        ManifestFile manifestFile = branchTable.store().manifestFileFactory().create();
        Map<FileEntry.Identifier, ManifestEntry> files = new LinkedHashMap<>();
        FileEntry.mergeEntries(manifestFile, manifestList.readDataManifests(snapshot), files, null);
        return files;
    }

    @Override
    public void commit(String targetBranch, List<ManifestEntry> filesToMerge) {
        FileStoreTable branchTable = branchTableFactory.apply(targetBranch);
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> grouped = new HashMap<>();
        for (ManifestEntry entry : filesToMerge) {
            grouped.computeIfAbsent(entry.partition(), k -> new HashMap<>())
                    .computeIfAbsent(entry.bucket(), k -> new ArrayList<>())
                    .add(entry.file());
        }

        String commitUser = UUID.randomUUID().toString();
        ManifestCommittable committable = new ManifestCommittable(0);
        for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> partEntry :
                grouped.entrySet()) {
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry :
                    partEntry.getValue().entrySet()) {
                CommitMessageImpl message =
                        new CommitMessageImpl(
                                partEntry.getKey(),
                                bucketEntry.getKey(),
                                null,
                                new DataIncrement(
                                        bucketEntry.getValue(),
                                        Collections.emptyList(),
                                        Collections.emptyList()),
                                CompactIncrement.emptyIncrement());
                committable.addFileCommittable(message);
            }
        }

        try (FileStoreCommit commit = branchTable.store().newCommit(commitUser, branchTable)) {
            commit.appendCommitCheckConflict(true).commit(committable, true);
        }
    }
}
