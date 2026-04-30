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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the missing-top-level-column → null property that {@code sequence.snapshot-ordering}
 * relies on for backward compatibility. Pre-patch Paimon files do not contain the {@code
 * _COMMIT_SNAPSHOT_ID} column; new readers always project it. If the format reader did not
 * gracefully fill missing top-level columns with null, reading any pre-patch file after upgrading
 * would fail or misread fields.
 *
 * <p>This test writes a file with a narrower schema (mimicking a pre-patch file) and reads it back
 * with a wider schema that contains an extra nullable column absent from the file.
 */
public class MissingColumnSchemaEvolutionTest {

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc"})
    public void readingFileWithMissingColumnYieldsNull(
            String formatIdentifier, @TempDir java.nio.file.Path tempDir) throws IOException {
        FileFormat format = createFileFormat(formatIdentifier);

        // File schema: 2 columns (mimics pre-patch _SEQUENCE_NUMBER, _VALUE_KIND meta).
        RowType writeSchema =
                new RowType(
                        false,
                        Arrays.asList(
                                new DataField(0, "seq", new BigIntType(false)),
                                new DataField(1, "kind", new IntType(false))));

        // Read schema: 3 columns (new meta layout; 3rd column is absent from the file).
        RowType readSchema =
                new RowType(
                        false,
                        Arrays.asList(
                                new DataField(0, "seq", new BigIntType(false)),
                                new DataField(1, "kind", new IntType(false)),
                                new DataField(2, "commit_snapshot_id", DataTypes.BIGINT())));

        Path path = new Path(tempDir.toUri().toString(), "test." + formatIdentifier);

        // Write with narrower schema.
        try (PositionOutputStream out = LocalFileIO.create().newOutputStream(path, false);
                FormatWriter writer =
                        format.createWriterFactory(writeSchema)
                                .create(out, CoreOptions.FILE_COMPRESSION.defaultValue())) {
            writer.addElement(GenericRow.of(100L, 0));
            writer.addElement(GenericRow.of(200L, 1));
        }

        // Read with wider schema.
        RecordReader<InternalRow> reader =
                format.createReaderFactory(readSchema, readSchema, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(
                                        LocalFileIO.create(),
                                        path,
                                        LocalFileIO.create().getFileSize(path)));

        List<Long> seqs = new ArrayList<>();
        List<Integer> kinds = new ArrayList<>();
        List<Boolean> missingIsNull = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    seqs.add(row.getLong(0));
                    kinds.add(row.getInt(1));
                    missingIsNull.add(row.isNullAt(2));
                });
        reader.close();

        assertThat(seqs).containsExactly(100L, 200L);
        assertThat(kinds).containsExactly(0, 1);
        assertThat(missingIsNull)
                .as("missing top-level column must read as null, not crash or misalign")
                .containsExactly(true, true);
    }

    private FileFormat createFileFormat(String identifier) {
        Options tableOptions = new Options();
        tableOptions.set(CoreOptions.FILE_FORMAT, identifier);
        return FileFormat.fromIdentifier(identifier, tableOptions);
    }
}
