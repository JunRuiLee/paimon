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

import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import static org.assertj.core.api.Assertions.assertThat;

class BlobReferenceRowConverterTest {

    @TempDir Path tempDir;

    @Test
    void testConvertReferenceBytesToData() throws Exception {
        byte[] data = new byte[] {1, 2, 3, 4};
        BlobDescriptor descriptor = writeBlob(data);
        InternalRow row = GenericRow.of(1, descriptor.serialize());

        BlobReferenceRowConverter converter =
                BlobReferenceRowConverter.create(
                        newRowType(), Arrays.asList("picture"), false, null, options().toMap());
        InternalRow converted = converter.convert(row);

        assertThat(converted.getBinary(1)).isEqualTo(data);
    }

    private RowType newRowType() {
        return RowType.of(
                new DataType[] {DataTypes.INT(), DataTypes.BYTES()},
                new String[] {"id", "picture"});
    }

    private Options options() {
        Options options = new Options();
        options.set("warehouse", tempDir.toString());
        return options;
    }

    private BlobDescriptor writeBlob(byte[] data) throws Exception {
        Path file = tempDir.resolve("blob.data");
        Files.write(file, data);
        String uri = "file://" + file.toAbsolutePath();
        return new BlobDescriptor(uri, 0, data.length);
    }
}
