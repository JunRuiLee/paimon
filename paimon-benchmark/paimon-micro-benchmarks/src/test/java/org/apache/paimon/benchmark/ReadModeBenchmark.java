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

package org.apache.paimon.benchmark;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.Split;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;

/**
 * Benchmark for {@code scan.read-mode} (performance vs freshness) on DV-enabled primary key tables.
 *
 * <p>Compares two read modes across three data layouts:
 *
 * <ul>
 *   <li><b>performance</b>: skips level-0 files at scan time for best read throughput.
 *   <li><b>freshness</b>: includes level-0 via merge-on-read for maximum data visibility.
 * </ul>
 *
 * <h3>Data layouts</h3>
 *
 * <ul>
 *   <li>{@code no-l0}: all data compacted to L1. Both modes behave identically.
 *   <li>{@code l0-narrow}: small L0 batch overlapping ~8% of L1 files.
 *   <li>{@code l0-wide}: large scattered L0 batch overlapping ~83% of L1 files.
 * </ul>
 */
public class ReadModeBenchmark extends TableBenchmark {

    private static final int BASE_ROW_COUNT = 500_000;
    private static final int WARMUP_OVERWRITE_COUNT = 100_000;
    private static final int NARROW_L0_COUNT = 25_000;
    private static final int WIDE_L0_COUNT = 120_000;
    private static final int READ_ITERATIONS = 3;
    private static final int BENCHMARK_ITERS = 10;
    private static final int VALUE_COUNT = 20;
    private static final long WIDE_SAMPLE_SEED = 0xDEADBEEFL;

    private final RandomDataGenerator random = new RandomDataGenerator();

    @Test
    public void testReadNoL0() throws Exception {
        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_441-b07 on Mac OS X 15.6.1
        Apple M2 Pro
        read-mode-no-l0:                                      Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------
        OPERATORTEST_read-mode-no-l0_performance               279 /  432           5378.8            185.9       1.0X
        OPERATORTEST_read-mode-no-l0_freshness                 272 /  276           5512.4            181.4       1.0X
        */
        runCase("read-mode-no-l0", L0Plan.NONE);
    }

    @Test
    public void testReadWithL0Narrow() throws Exception {
        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_441-b07 on Mac OS X 15.6.1
        Apple M2 Pro
        read-mode-l0-narrow:                                           Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------
        OPERATORTEST_read-mode-l0-narrow_performance                    280 /  293           5360.5            186.6       1.0X
        OPERATORTEST_read-mode-l0-narrow_freshness                      329 /  729           4553.2            219.6       0.8X
        */
        runCase("read-mode-l0-narrow", L0Plan.NARROW);
    }

    @Test
    public void testReadWithL0Wide() throws Exception {
        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_441-b07 on Mac OS X 15.6.1
        Apple M2 Pro
        read-mode-l0-wide:                                                 Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------
        OPERATORTEST_read-mode-l0-wide_performance                          273 /  322           5493.8            182.0       1.0X
        OPERATORTEST_read-mode-l0-wide_freshness                            352 /  472           4265.3            234.5       0.8X
        */
        runCase("read-mode-l0-wide", L0Plan.WIDE);
    }

    private void runCase(String label, L0Plan plan) throws Exception {
        Table perfTable =
                prepareTable(dvOptions(CoreOptions.ReadMode.PERFORMANCE), label + "_perf", plan);
        Table freshTable =
                prepareTable(dvOptions(CoreOptions.ReadMode.FRESHNESS), label + "_fresh", plan);

        System.out.printf(
                "[%s] row counts: performance=%d, freshness=%d%n",
                label, countRows(perfTable), countRows(freshTable));

        Benchmark benchmark =
                new Benchmark(label, (long) READ_ITERATIONS * BASE_ROW_COUNT)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        addReadCase(benchmark, "performance", perfTable);
        addReadCase(benchmark, "freshness", freshTable);
        benchmark.run();
    }

    private Options dvOptions(CoreOptions.ReadMode readMode) {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_ORC);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.NUM_LEVELS, 2);
        options.set(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER, 999);
        options.set(CoreOptions.NUM_SORTED_RUNS_STOP_TRIGGER, 999);
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofMebiBytes(8));
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        options.set(CoreOptions.SCAN_READ_MODE, readMode);
        return options;
    }

    private InternalRow newRowWithKey(int key) {
        GenericRow row = new GenericRow(1 + VALUE_COUNT);
        row.setField(0, key);
        for (int i = 1; i <= VALUE_COUNT; i++) {
            row.setField(i, BinaryString.fromString(random.nextHexString(10)));
        }
        return row;
    }

    private Table prepareTable(Options options, String tableName, L0Plan plan) throws Exception {
        Table table = createTable(options, tableName, Collections.singletonList("k"));
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();
        write.withIOManager(new IOManagerImpl(tempFile.toString()));
        StreamTableCommit commit = writeBuilder.newCommit();

        // Stage 1: base data, then full compact to L1.
        for (int i = 0; i < BASE_ROW_COUNT; i++) {
            write.write(newRowWithKey(i));
        }
        List<CommitMessage> messages = write.prepareCommit(false, 1);
        commit.commit(1, messages);
        write.compact(EMPTY_ROW, 0, true);
        messages = write.prepareCommit(true, 2);
        commit.commit(2, messages);

        // Stage 2: overwrite a portion of keys, then full compact to produce DVs on L1.
        for (int i = 0; i < WARMUP_OVERWRITE_COUNT; i++) {
            write.write(newRowWithKey(i));
        }
        messages = write.prepareCommit(false, 3);
        commit.commit(3, messages);
        write.compact(EMPTY_ROW, 0, true);
        messages = write.prepareCommit(true, 4);
        commit.commit(4, messages);

        // Stage 3: optional L0 that stays uncompacted.
        int[] stage3Keys = stage3Keys(plan);
        if (stage3Keys.length > 0) {
            for (int key : stage3Keys) {
                write.write(newRowWithKey(key));
            }
            messages = write.prepareCommit(false, 5);
            commit.commit(5, messages);
        }

        write.close();
        commit.close();
        return table;
    }

    private int[] stage3Keys(L0Plan plan) {
        switch (plan) {
            case NONE:
                return new int[0];
            case NARROW:
                {
                    int[] keys = new int[NARROW_L0_COUNT];
                    for (int i = 0; i < NARROW_L0_COUNT; i++) {
                        keys[i] = WARMUP_OVERWRITE_COUNT + i;
                    }
                    return keys;
                }
            case WIDE:
                {
                    int pool = BASE_ROW_COUNT - WARMUP_OVERWRITE_COUNT;
                    int[] universe = new int[pool];
                    for (int i = 0; i < pool; i++) {
                        universe[i] = WARMUP_OVERWRITE_COUNT + i;
                    }
                    Random r = new Random(WIDE_SAMPLE_SEED);
                    for (int i = 0; i < WIDE_L0_COUNT; i++) {
                        int j = i + r.nextInt(pool - i);
                        int tmp = universe[i];
                        universe[i] = universe[j];
                        universe[j] = tmp;
                    }
                    int[] keys = new int[WIDE_L0_COUNT];
                    System.arraycopy(universe, 0, keys, 0, WIDE_L0_COUNT);
                    return keys;
                }
            default:
                throw new IllegalStateException("Unknown plan: " + plan);
        }
    }

    private long countRows(Table table) throws Exception {
        long count = 0;
        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
        for (Split split : splits) {
            try (RecordReader<InternalRow> reader =
                    table.newReadBuilder().newRead().createReader(split)) {
                RecordReader.RecordIterator<InternalRow> it;
                while ((it = reader.readBatch()) != null) {
                    while (it.next() != null) {
                        count++;
                    }
                    it.releaseBatch();
                }
            }
        }
        return count;
    }

    private void addReadCase(Benchmark benchmark, String name, Table table) {
        benchmark.addCase(
                name,
                BENCHMARK_ITERS,
                () -> {
                    try {
                        for (int i = 0; i < READ_ITERATIONS; i++) {
                            List<Split> splits = table.newReadBuilder().newScan().plan().splits();
                            AtomicLong count = new AtomicLong(0);
                            for (Split split : splits) {
                                RecordReader<InternalRow> reader =
                                        table.newReadBuilder().newRead().createReader(split);
                                reader.forEachRemaining(row -> count.incrementAndGet());
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private enum L0Plan {
        NONE,
        NARROW,
        WIDE
    }
}
