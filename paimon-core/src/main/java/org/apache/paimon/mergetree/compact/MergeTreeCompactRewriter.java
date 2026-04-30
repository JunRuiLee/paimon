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

import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Default {@link CompactRewriter} for merge trees. */
public class MergeTreeCompactRewriter extends AbstractCompactRewriter {

    protected final FileReaderFactory<KeyValue> readerFactory;
    protected final KeyValueFileWriterFactory writerFactory;
    protected final Comparator<InternalRow> keyComparator;
    @Nullable protected final FieldsComparator userDefinedSeqComparator;
    protected final MergeFunctionFactory<KeyValue> mfFactory;
    protected final MergeSorter mergeSorter;
    protected final boolean snapshotSequenceOrdering;
    @Nullable private CompactionMetrics.Reporter metricsReporter;

    public MergeTreeCompactRewriter(
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            boolean snapshotSequenceOrdering) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.keyComparator = keyComparator;
        this.userDefinedSeqComparator = userDefinedSeqComparator;
        this.mfFactory = mfFactory;
        this.mergeSorter = mergeSorter;
        this.snapshotSequenceOrdering = snapshotSequenceOrdering;
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        return rewriteCompaction(outputLevel, dropDelete, sections);
    }

    protected CompactResult rewriteCompaction(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingMergeTreeFileWriter(outputLevel, FileSource.COMPACT);
        RecordReader<KeyValue> reader = null;
        Exception collectedExceptions = null;
        try {
            reader =
                    readerForMergeTree(
                            sections, new ReducerMergeFunctionWrapper(mfFactory.create()));
            if (dropDelete) {
                reader = new DropDeleteReader(reader);
            }
            writer.write(new RecordReaderIterator<>(reader));
        } catch (Exception e) {
            collectedExceptions = e;
        } finally {
            try {
                IOUtils.closeAll(reader, writer);
            } catch (Exception e) {
                collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
            }
        }

        if (null != collectedExceptions) {
            writer.abort();
            throw collectedExceptions;
        }

        List<DataFileMeta> before = extractFilesFromSections(sections);
        notifyRewriteCompactBefore(before);
        List<DataFileMeta> after = writer.result();
        after = preAssignCommitSnapshotId(after, sections);
        after = notifyRewriteCompactAfter(after);
        if (metricsReporter != null) {
            metricsReporter.reportSortBufferMetrics(
                    mergeSorter.sortBufferUsedBytes(), mergeSorter.sortBufferTotalBytes());
        }
        return new CompactResult(before, after);
    }

    protected <T> RecordReader<T> readerForMergeTree(
            List<List<SortedRun>> sections, MergeFunctionWrapper<T> mergeFunctionWrapper)
            throws IOException {
        return MergeTreeReaders.readerForMergeTree(
                sections,
                readerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mergeFunctionWrapper,
                mergeSorter);
    }

    protected List<DataFileMeta> preAssignCommitSnapshotId(
            List<DataFileMeta> outputFiles, List<List<SortedRun>> sections) {
        if (!snapshotSequenceOrdering) {
            return outputFiles;
        }
        // Correctness: a pure compaction output carries only historical data from its source
        // files; it must inherit max(sourceIds) so it does not get "promoted" to the current
        // commit's snapshotId (which would incorrectly make it beat concurrently-committed
        // newer data). If any source is still pending — null (legacy / feature-off input) or
        // Long.MAX_VALUE (in-txn placeholder from MergeTreeWriter) — the output is mixed with
        // in-txn new data, so we defer stamping to FileStoreCommitImpl#assignCommitSnapshotId.
        long maxSnapshotId = Long.MIN_VALUE;
        for (List<SortedRun> runs : sections) {
            for (SortedRun run : runs) {
                for (DataFileMeta file : run.files()) {
                    Long id = file.commitSnapshotId();
                    if (!DataFileMeta.isCommittedSnapshotId(id)) {
                        return outputFiles;
                    }
                    if (id > maxSnapshotId) {
                        maxSnapshotId = id;
                    }
                }
            }
        }
        if (maxSnapshotId == Long.MIN_VALUE) {
            // No source files: nothing to inherit from, leave outputs untouched and let
            // FileStoreCommitImpl stamp them at commit time.
            return outputFiles;
        }
        List<DataFileMeta> result = new ArrayList<>(outputFiles.size());
        for (DataFileMeta file : outputFiles) {
            result.add(file.assignCommitSnapshotId(maxSnapshotId));
        }
        return result;
    }

    protected void notifyRewriteCompactBefore(List<DataFileMeta> files) {}

    protected List<DataFileMeta> notifyRewriteCompactAfter(List<DataFileMeta> files) {
        return files;
    }

    public void setMetricsReporter(@Nullable CompactionMetrics.Reporter reporter) {
        this.metricsReporter = reporter;
    }
}
