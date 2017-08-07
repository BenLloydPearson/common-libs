/**
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
package com.gravity.utilities.bytes;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The sort order of a <code>byte[]</code> or <code>HDataType</code> instance,
 * either ASCENDING or DESCENDING.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum Order {
    ASCENDING, DESCENDING;

    private static final byte mask = (byte) 0xff;

    /**
     * Returns the adjusted trichotomous value according to the ordering imposed
     * by this <code>Order</code>.
     */
    public int cmp(int cmp) {
        return cmp * (this == ASCENDING ? 1 : -1);
    }

    /**
     * Apply order to the byte <code>b</code>.
     */
    public byte apply(byte val) {
        return (byte) (this == ASCENDING ? val : val ^ mask);
    }

    /**
     * Apply order to the byte array <code>a</code>.
     */
    public void apply(byte[] val) {
        if (this != DESCENDING) return;
        for (int i = 0; i < val.length; i++) {
            val[i] ^= mask;
        }
    }

    /**
     * Apply order to the byte array <code>a</code> according to the Order.
     */
    public void apply(byte[] val, int offset, int length) {
        if (this != DESCENDING) return;
        for (int i = 0; i < length; i++) {
            val[offset + i] ^= mask;
        }
    }

    @Override
    public String toString() {
        return this == ASCENDING ? "asc" : "dsc";
    }
}