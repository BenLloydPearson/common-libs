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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * HNumeric represents a numeric value for use with {@link OrderedBytes}. This
 * is necessary because {@link BigDecimal} does not support a representation
 * for NaN or +/-Inf.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Numeric extends Number {
    private static final long serialVersionUID = -4488167747731287844L;

    /**
     * The context used for numerical operations.
     */
    public static final MathContext MATH_CONTEXT = new MathContext(31, RoundingMode.HALF_UP);

    public static final Numeric NaN = new Numeric(Double.NaN);
    public static final Numeric NEGATIVE_INFINITY = new Numeric(Double.NEGATIVE_INFINITY);
    public static final Numeric ZERO = new Numeric(0.0);
    public static final Numeric POSITIVE_INFINITY = new Numeric(Double.POSITIVE_INFINITY);

    private final boolean isR;
    private final boolean isZ;
    private final long z;
    private final double r;
    private final BigDecimal bd;

    private transient int hashCode = 0;

    /**
     * Create an <code>HNumeric</code> instance over a <code>double</code>.
     */
    public Numeric(double val) {
        isR = true;
        r = val;
        isZ = false;
        z = 0;
        bd = null;
    }

    /**
     * Create an <code>HNumeric</code> instance over a <code>long</code>.
     */
    public Numeric(long val) {
        isZ = true;
        z = val;
        isR = false;
        r = 0.0;
        bd = null;
    }

    /**
     * Create an <code>HNumeric</code> instance over a <code>BigDecimal</code> .
     */
    public Numeric(BigDecimal val) {
        if (null == val) throw new NullPointerException();

        // see if this can be a long instead
        boolean isLong = false;
        long lng = 0;
        try {
            lng = val.longValueExact();
            isLong = true;
        } catch (ArithmeticException e) {
        }

        if (isLong) {
            isZ = true;
            z = lng;
            isR = false;
            r = 0.0;
            bd = null;
        } else {
            // doesn't fit in a long, fall back to BD
            bd = val.round(MATH_CONTEXT);
            isZ = false;
            isR = false;
            z = 0;
            r = 0.0;
        }
    }

    /**
     * Returns <code>true</code> if the <code>Number</code> is an Integer and
     * fits in a <code>long</code>, false otherwise.
     */
    public boolean isInteger() {
        return isZ;
    }

    /**
     * Returns <code>true</code> if the <code>Number</code> is a Real and fits
     * in a <code>double</code>, false otherwise.
     */
    public boolean isReal() {
        return isR;
    }

    /**
     * Returns <code>true</code> if the <code>Number</code> is infinitely large
     * in magnitude, <code>false</code> otherwise.
     */
    public boolean isInfinite() {
        return isR && Double.isInfinite(r);
    }

    /**
     * Returns <code>true</code> if the <code>Number</code> is a Not-a-Number
     * (NaN) value, <code>false</code> otherwise.
     */
    public boolean isNaN() {
        return isR && Double.isNaN(r);
    }

    /**
     * Retrieve the value as a <code>BigDecimal</code>. This will silently
     * promote a <code>double</code> or <code>long</code> to a
     * <code>BigDecimal</code> when possible, so use it only if a primitive
     * value is not available. Check availability using {@link #isInteger()} and
     * {@link #isReal()}.
     *
     * @throws NumberFormatException if the
     *                               <code>Number<code> is infinite or NaN.
     */
    public BigDecimal exactValue() {
        return null == bd ? isR ? BigDecimal.valueOf(r) : BigDecimal.valueOf(z) : bd;
    }

    @Override
    public double doubleValue() {
        return isReal() ? r : isInteger() ? (double) z : bd.doubleValue();
    }

    @Override
    public int intValue() {
        return isInteger() ? (int) z : isReal() ? (int) r : bd.intValue();
    }

    @Override
    public long longValue() {
        return isInteger() ? z : isReal() ? (long) r : bd.longValue();
    }

    @Override
    public float floatValue() {
        return isReal() ? (float) r : isInteger() ? (float) z : bd.floatValue();
    }

    @Override
    public String toString() {
        return isReal() ? Double.toString(r) : isInteger() ? Long.toString(z) : bd.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (null == o) return false;
        if (!(o instanceof Numeric)) return false;
        Numeric that = (Numeric) o;
        if (this.isInteger() && that.isInteger()) return this.longValue() == that.longValue();
        if (this.isReal() && that.isReal()) return this.doubleValue() == that.doubleValue();
        return 0 == this.exactValue().compareTo(that.exactValue());
    }

    @Override
    public int hashCode() {
        if (0 != hashCode) return hashCode;
        int result = 1;
        if (isInteger()) {
            result = result * 23 + (int) (z ^ (z >>> 32));
        } else if (isReal()) {
            long bits = Double.doubleToLongBits(r);
            result = result * 13 + (int) (bits ^ (bits >>> 32));
        } else {
            result = result * 17 + bd.hashCode();
        }
        hashCode = result;
        return result;
    }
}