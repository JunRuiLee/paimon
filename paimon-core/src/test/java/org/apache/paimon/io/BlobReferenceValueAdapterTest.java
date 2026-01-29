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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class BlobReferenceValueAdapterTest {

    @Test
    void testReplaceWithReferences() {
        BlobReferenceValueAdapter adapter =
                new BlobReferenceValueAdapter(newRowType(), Arrays.asList("pic1", "pic2"));

        byte[] pic1 = new byte[] {1, 2, 3};
        byte[] pic2 = new byte[] {4, 5, 6};
        InternalRow row = GenericRow.of(1, pic1, pic2);

        byte[] ref1 = new byte[] {9, 9};
        InternalRow replaced = adapter.replaceWithReferences(row, new byte[][] {ref1, null});

        assertThat(replaced.getInt(0)).isEqualTo(1);
        assertThat(replaced.getBinary(1)).isEqualTo(ref1);
        assertThat(replaced.getBinary(2)).isEqualTo(pic2);
    }

    private RowType newRowType() {
        return RowType.of(
                new DataType[] {DataTypes.INT(), DataTypes.BYTES(), DataTypes.BYTES()},
                new String[] {"id", "pic1", "pic2"});
    }
}
