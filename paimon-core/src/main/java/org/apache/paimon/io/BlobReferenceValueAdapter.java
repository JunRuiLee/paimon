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
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.RowType;

import java.util.List;

/**
 * Replaces blob fields with reference bytes for primary-key storage mode.
 *
 * <p>If reference bytes are available, they replace the original field bytes; otherwise the
 * original bytes are kept.
 */
public class BlobReferenceValueAdapter {

    private final int[] blobFieldIndexes;
    private final InternalRowSerializer valueSerializer;

    public BlobReferenceValueAdapter(RowType valueType, List<String> blobFieldNames) {
        this.blobFieldIndexes =
                blobFieldNames.stream().mapToInt(valueType::getFieldIndex).toArray();
        this.valueSerializer = new InternalRowSerializer(valueType);
    }

    public InternalRow replaceWithReferences(InternalRow valueRow, byte[][] referenceBytes) {
        if (blobFieldIndexes.length == 0) {
            return valueRow;
        }
        GenericRow copied =
                (GenericRow)
                        valueSerializer.copyRowData(
                                valueRow, new GenericRow(valueRow.getFieldCount()));
        for (int i = 0; i < blobFieldIndexes.length; i++) {
            int fieldIndex = blobFieldIndexes[i];
            if (fieldIndex < 0) {
                continue;
            }
            if (valueRow.isNullAt(fieldIndex)) {
                copied.setField(fieldIndex, null);
                continue;
            }
            byte[] bytes = valueRow.getBinary(fieldIndex);
            byte[] resolvedBytes =
                    referenceBytes == null || referenceBytes.length <= i ? null : referenceBytes[i];
            copied.setField(fieldIndex, resolvedBytes != null ? resolvedBytes : bytes);
        }
        return copied;
    }
}
