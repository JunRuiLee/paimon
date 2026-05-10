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

    protected void notifyRewriteCompactBefore(List<DataFileMeta> files) {}

    protected List<DataFileMeta> notifyRewriteCompactAfter(List<DataFileMeta> files) {
        return files;
    }

    /**
     * When snapshot-ordering is enabled, propagate the maximum stamped commit-snapshot-id of the
     * input files to the rewritten outputs. This allows dedicated-compaction jobs (which see the
     * inputs already stamped at write time) to keep merge tiebreaks correct without depending on
     * which snapshot id is assigned at commit. If any input is unstamped (e.g. data written before
     * the option was enabled) we leave the outputs unstamped so commit-time stamping kicks in.
     *
     * <p>Correctness relies on the merge-tree invariant that each compaction section contains
     * <b>all</b> sorted runs whose key ranges overlap. This guarantees that for every key in the
     * output, the merge has already resolved all conflicting versions, so the max stamp accurately
     * represents the "winning" snapshot. If a partial compaction were to skip a sorted run
     * containing the same key, the elevated stamp could incorrectly shadow a newer version of that
     * key.
     */
    protected List<DataFileMeta> preAssignCommitSnapshotId(
            List<DataFileMeta> outputFiles, List<List<SortedRun>> sections) {
        if (!snapshotSequenceOrdering) {
            return outputFiles;
        }
        long maxSnapshotId = Long.MIN_VALUE;
        boolean foundStamped = false;
        for (List<SortedRun> runs : sections) {
            for (SortedRun run : runs) {
                for (DataFileMeta file : run.files()) {
                    Long id = file.commitSnapshotId();
                    if (id == null) {
                        return outputFiles;
                    }
                    foundStamped = true;
                    if (id > maxSnapshotId) {
                        maxSnapshotId = id;
                    }
                }
            }
        }
        if (!foundStamped) {
            return outputFiles;
        }
        List<DataFileMeta> result = new ArrayList<>(outputFiles.size());
        for (DataFileMeta file : outputFiles) {
            result.add(file.assignCommitSnapshotId(maxSnapshotId));
        }
        return result;
    }

    public void setMetricsReporter(@Nullable CompactionMetrics.Reporter reporter) {
        this.metricsReporter = reporter;
    }
}
