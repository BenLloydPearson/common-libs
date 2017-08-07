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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;

/**
 * Utility class that handles ordered byte arrays. That is, unlike
 * {@link org.apache.hadoop.hbase.util.Bytes}, these methods produce byte arrays which maintain the sort
 * order of the original values.
 * <h3>Encoding Format summary</h3>
 * <p>
 * Each value is encoded as one or more bytes. The first byte of the encoding,
 * its meaning, and a terse description of the bytes that follow is given by
 * the following table:
 * <table>
 * <tr><th>Content Type</th><th>Encoding</th></tr>
 * <tr><td>NULL</td><td>0x05</td></tr>
 * <tr><td>negative infinity</td><td>0x07</td></tr>
 * <tr><td>negative large</td><td>0x08, ~E, ~M</td></tr>
 * <tr><td>negative medium</td><td>0x13-E, ~M</td></tr>
 * <tr><td>negative small</td><td>0x14, -E, ~M</td></tr>
 * <tr><td>zero</td><td>0x15</td></tr>
 * <tr><td>positive small</td><td>0x16, ~-E, M</td></tr>
 * <tr><td>positive medium</td><td>0x17+E, M</td></tr>
 * <tr><td>positive large</td><td>0x22, E, M</td></tr>
 * <tr><td>positive infinity</td><td>0x23</td></tr>
 * <tr><td>NaN</td><td>0x25</td></tr>
 * <tr><td>fixed-length 32-bit integer</td><td>0x27, I</td></tr>
 * <tr><td>fixed-length 64-bit integer</td><td>0x28, I</td></tr>
 * <tr><td>fixed-length 32-bit float</td><td>0x30, F</td></tr>
 * <tr><td>fixed-length 64-bit float</td><td>0x31, F</td></tr>
 * <tr><td>text</td><td>0x33, T</td></tr>
 * <tr><td>variable binary</td><td>0x35, B</td></tr>
 * <tr><td>copy binary</td><td>0x36, X</td></tr>
 * </table>
 * </p>
 * <p/>
 * <h3>Null Encoding</h3>
 * <p>
 * Each value that is a NULL encodes as a single byte of 0x05. Since every
 * other value encoding begins with a byte greater than 0x05, this forces NULL
 * values to sort first.
 * </p>
 * <h3>Text Encoding</h3>
 * <p>
 * Each text value begins with a single byte of 0x33 and ends with a single
 * byte of 0x00. There are zero or more intervening bytes that encode the text
 * value. The intervening bytes are chosen so that the encoding will sort in
 * the desired collating order. The intervening bytes may not contain a 0x00
 * character; the only 0x00 byte allowed in a text encoding is the final byte.
 * </p>
 * <p>
 * The text encoding ends in 0x00 in order to ensure that when there are two
 * strings where one is a prefix of the other that the shorter string will
 * sort first.
 * </p>
 * <h3>Binary Encoding</h3>
 * <p>
 * There are two encoding strategies for binary fields, referred to as
 * "BlobVar" and "BlobCopy". BlobVar is less efficient in both space and
 * encoding time. It has no limitations on the range of encoded values.
 * BlobCopy is a byte-for-byte copy of the input data followed by a
 * termination byte. It is extremely fast to encode and decode. It carries the
 * restriction of not allowing a 0x00 value in the input byte[] as this value
 * is used as the termination byte.
 * </p>
 * <h4>BlobVar</h4>
 * <p>
 * "BlobVar" encodes the input byte[] in a manner similar to a variable length
 * integer encoding. As with the other <code>OrderedBytes</code> encodings,
 * the first encoded byte is used to indicate what kind of value follows. This
 * header byte is 0x35 for BlobVar encoded values. As with the traditional
 * varint encoding, the most significant bit of each subsequent encoded
 * <code>byte</code> is used as a continuation marker. The 7 remaining bits
 * contain the 7 most significant bits of the first unencoded byte. The next
 * encoded byte starts with a continuation marker in the MSB. The least
 * significant bit from the first unencoded byte follows, and the remaining 6
 * bits contain the 6 MSBs of the second unencoded byte. The encoding
 * continues, encoding 7 bytes on to 8 encoded bytes. The MSB of the final
 * encoded byte contains a termination marker rather than a continuation
 * marker, and any remaining bits from the final input byte. Any trailing bits
 * in the final encoded byte are zeros.
 * </p>
 * <h4>BlobCopy</h4>
 * <p>
 * "BlobCopy" is a simple byte-for-byte copy of the input data. It uses 0x36
 * as the header byte, and is terminated by 0x00. This alternative encoding is
 * more efficient, but it cannot accept values containing a 0x00 byte.
 * </p>
 * <h3>Variable-length Numeric Encoding</h3>
 * <p>
 * Numeric values must be coded so as to sort in numeric order. We assume that
 * numeric values can be both integer and floating point values. The wrapper
 * class {@link Numeric} is used to smooth over values decoded using this
 * scheme.
 * </p>
 * <p>
 * Simplest cases first: If the numeric value is a NaN, then the encoding is a
 * single byte of 0x25. This causes NaN values to sort after to every other
 * numeric value.
 * </p>
 * <p>
 * If the numeric value is a negative infinity then the encoding is a single
 * byte of 0x07. Since every other numeric value except NaN has a larger
 * initial byte, this encoding ensures that negative infinity will sort prior
 * to every other numeric value other than NaN.
 * </p>
 * <p>
 * If the numeric value is a positive infinity then the encoding is a single
 * byte of 0x23. Every other numeric value encoding begins with a smaller
 * byte, ensuring that positive infinity always sorts last among numeric
 * values. 0x23 is also smaller than 0x33, the initial byte of a text value,
 * ensuring that every numeric value sorts before every text value.
 * </p>
 * <p>
 * If the numeric value is exactly zero then it is encoded as a single byte of
 * 0x15. Finite negative values will have initial bytes of 0x08 through 0x14
 * and finite positive values will have initial bytes of 0x16 through 0x22.
 * </p>
 * <p>
 * For all numeric values, we compute a mantissa M and an exponent E. The
 * mantissa is a base-100 representation of the value. The exponent E
 * determines where to put the decimal point.
 * </p>
 * <p>
 * Each centimal digit of the mantissa is stored in a byte. If the value of
 * the centimal digit is X (hence X&ge;0 and X&le;99) then the byte value will
 * be 2*X+1 for every byte of the mantissa, except for the last byte which
 * will be 2*X+0. The mantissa must be the minimum number of bytes necessary
 * to represent the value; trailing X==0 digits are omitted. This means that
 * the mantissa will never contain a byte with the value 0x00.
 * </p>
 * <p>
 * If we assume all digits of the mantissa occur to the right of the decimal
 * point, then the exponent E is the power of one hundred by which one must
 * multiply the mantissa to recover the original value.
 * </p>
 * <p>
 * Values are classified as large, medium, or small according to the value of
 * E. If E is 11 or more, the value is large. For E between 0 and 10, the
 * value is medium. For E less than zero, the value is small.
 * </p>
 * <p>
 * Large positive values are encoded as a single byte 0x22 followed by E as a
 * varint and then M. Medium positive values are a single byte of 0x17+E
 * followed by M. Small positive values are encoded as a single byte 0x16
 * followed by the ones-complement of the varint for -E followed by M.
 * </p>
 * <p>
 * Small negative values are encoded as a single byte 0x14 followed by -E as a
 * varint and then the ones-complement of M. Medium negative values are
 * encoded as a byte 0x13-E followed by the ones-complement of M. Large
 * negative values consist of the single byte 0x08 followed by the
 * ones-complement of the varint encoding of E followed by the ones-complement
 * of M.
 * </p>
 * <h3>Fixed-length Integer Encoding</h3>
 * <p>
 * All 4-byte integers are serialized to a 5-byte, fixed-width, sortable byte
 * format. All 8-byte integers are serialized to the equivelant 9-byte format.
 * Serialization is performed by writing a header byte, inverting the integer
 * sign bit and writing the resulting bytes to the byte array in big endian
 * order.
 * </p>
 * <h3>Fixed-length Floating Point Encoding</h3>
 * <p>
 * 32-bit and 64-bit floating point numbers are encoded to a 5-byte and 9-byte
 * encoding format, respectively. The format is identical, save for the
 * precision respected in each step of the operation.
 * <p>
 * This format ensures the following total ordering of floating point values:
 * Float.NEGATIVE_INFINITY &lt; -Float.MAX_VALUE &lt; ... &lt;
 * -Float.MIN_VALUE &lt; -0.0 &lt; +0.0; &lt; Float.MIN_VALUE &lt; ... &lt;
 * Float.MAX_VALUE &lt; Float.POSITIVE_INFINITY &lt; Float.NaN
 * </p>
 * <p>
 * Floating point numbers are encoded as specified in IEEE 754. A 32-bit
 * single precision float consists of a sign bit, 8-bit unsigned exponent
 * encoded in offset-127 notation, and a 23-bit significand. The format is
 * described further in the <a
 * href="http://en.wikipedia.org/wiki/Single_precision"> Single Precision
 * Floating Point Wikipedia page</a>
 * </p>
 * <p>
 * The value of a normal float is -1 <sup>sign bit</sup> &times;
 * 2<sup>exponent - 127</sup> &times; 1.significand
 * </p>
 * <p>
 * The IEE754 floating point format already preserves sort ordering for
 * positive floating point numbers when the raw bytes are compared in most
 * significant byte order. This is discussed further at <a href=
 * "http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm"
 * > http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.
 * htm</a>
 * </p>
 * <p>
 * Thus, we need only ensure that negative numbers sort in the the exact
 * opposite order as positive numbers (so that say, negative infinity is less
 * than negative 1), and that all negative numbers compare less than any
 * positive number. To accomplish this, we invert the sign bit of all floating
 * point numbers, and we also invert the exponent and significand bits if the
 * floating point number was negative.
 * </p>
 * <p>
 * More specifically, we first store the floating point bits into a 32-bit int
 * <code>j</code> using {@link Float#floatToIntBits}. This method collapses
 * all NaNs into a single, canonical NaN value but otherwise leaves the bits
 * unchanged. We then compute
 * </p>
 * <p/>
 * <pre>
 * j &circ;= (j &gt;&gt; (Integer.SIZE - 1)) | Integer.MIN_SIZE
 * </pre>
 * <p>
 * which inverts the sign bit and XOR's all other bits with the sign bit
 * itself. Comparing the raw bytes of <code>j</code> in most significant byte
 * order is equivalent to performing a single precision floating point
 * comparison on the underlying bits (ignoring NaN comparisons, as NaNs don't
 * compare equal to anything when performing floating point comparisons).
 * </p>
 * <p>
 * The resulting integer is then converted into a byte array by serializing
 * the integer one byte at a time in most significant byte order. The
 * serialized integer is prefixed by a single header byte. All serialized
 * values are 5 bytes in length.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedBytes {

  /*
   * The following constant values are used by encoding implementations
   */

    public static final byte TERM = 0x00;
    public static final byte NULL = 0x05;
    public static final byte NEG_INF = 0x07;
    public static final byte NEG_LARGE = 0x08;
    public static final byte NEG_MED_MIN = 0x09;
    public static final byte NEG_MED_MAX = 0x13;
    public static final byte NEG_SMALL = 0x14;
    public static final byte ZERO = 0x15;
    public static final byte POS_SMALL = 0x16;
    public static final byte POS_MED_MIN = 0x17;
    public static final byte POS_MED_MAX = 0x21;
    public static final byte POS_LARGE = 0x22;
    public static final byte POS_INF = 0x23;
    public static final byte NAN = 0x25;
    public static final byte FIXED_INT32 = 0x27;
    public static final byte FIXED_INT64 = 0x28;
    public static final byte FIXED_FLOAT32 = 0x30;
    public static final byte FIXED_FLOAT64 = 0x31;
    public static final byte TEXT = 0x33;
    public static final byte BLOB_VAR = 0x35;
    public static final byte BLOB_COPY = 0x36;

    public static final Charset UTF8 = Charset.forName("UTF-8");

    // constants used for numeric {en,de}coding
    private static final BigDecimal NEG_ONE = BigDecimal.ONE.negate();
    private static final BigDecimal E8 = BigDecimal.valueOf(1e8);
    private static final BigDecimal E32 = BigDecimal.valueOf(1e32);
    private static final BigDecimal EN2 = BigDecimal.valueOf(1e-2);
    private static final BigDecimal EN10 = BigDecimal.valueOf(1e-10);

    /**
     * Perform unsigned comparison between two long values. Conforms to the same
     * interface as {@link Comparator#compare(Object, Object)}.
     */
    private static int unsignedCmp(long x1, long x2) {
        int cmp;
        if ((cmp = (x1 < x2 ? -1 : (x1 == x2 ? 0 : 1))) == 0) return 0;
        // invert the result when either value is negative
        if ((x1 < 0) != (x2 < 0)) return -cmp;
        return cmp;
    }

    /**
     * Write a 32-bit unsigned integer to <code>dst</code> as 4 big-endian
     * bytes.
     *
     * @return number of bytes written.
     */
    private static int putUint32(ByteBuffer dst, int val) {
        dst.put((byte) (val >>> 24))
                .put((byte) (val >>> 16))
                .put((byte) (val >>> 8))
                .put((byte) val);
        return 4;
    }

    /**
     * Encode an unsigned 64-bit integer <code>val</code> into <code>dst</code>.
     * Compliment the encoded value when <code>comp</code> is true.
     */
    @VisibleForTesting
    static int putVaruint64(ByteBuffer dst, long val, boolean comp) {
        int w, y, start = dst.position();
        byte[] a = dst.array();
        Order ord = comp ? Order.DESCENDING : Order.ASCENDING;
        if (-1 == unsignedCmp(val, 241L)) {
            dst.put((byte) val);
            ord.apply(a, start, 1);
            return 1;
        }
        if (-1 == unsignedCmp(val, 2288L)) {
            y = (int) (val - 240);
            dst.put((byte) (y / 256 + 241))
                    .put((byte) (y % 256));
            ord.apply(a, start, 2);
            return 2;
        }
        if (-1 == unsignedCmp(val, 67824L)) {
            y = (int) (val - 2288);
            dst.put((byte) 249)
                    .put((byte) (y / 256))
                    .put((byte) (y % 256));
            ord.apply(a, start, 3);
            return 3;
        }
        y = (int) (val & 0xffffffff);
        w = (int) (val >>> 32);
        if (w == 0) {
            if (-1 == unsignedCmp(y, 16777216L)) {
                dst.put((byte) 250)
                        .put((byte) (y >>> 16))
                        .put((byte) (y >>> 8))
                        .put((byte) y);
                ord.apply(a, start, 4);
                return 4;
            }
            dst.put((byte) 251);
            putUint32(dst, y);
            ord.apply(a, start, 5);
            return 5;
        }
        if (-1 == unsignedCmp(w, 256L)) {
            dst.put((byte) 252)
                    .put((byte) w);
            putUint32(dst, y);
            ord.apply(a, start, 6);
            return 6;
        }
        if (-1 == unsignedCmp(w, 65536L)) {
            dst.put((byte) 253)
                    .put((byte) (w >>> 8))
                    .put((byte) w);
            putUint32(dst, y);
            ord.apply(a, start, 7);
            return 7;
        }
        if (-1 == unsignedCmp(w, 16777216L)) {
            dst.put((byte) 254)
                    .put((byte) (w >>> 16))
                    .put((byte) (w >>> 8))
                    .put((byte) w);
            putUint32(dst, y);
            ord.apply(a, start, 8);
            return 8;
        }
        dst.put((byte) 255);
        putUint32(dst, w);
        putUint32(dst, y);
        ord.apply(a, start, 9);
        return 9;
    }

    /**
     * Inspect an encoded varu64 for it's encoded length. Does not modify
     * <code>src</code>'s state.
     *
     * @param src  source buffer
     * @param comp if true, parse the compliment of the value.
     * @return number of bytes consumed by this value
     */
    @VisibleForTesting
    static int lengthVaru64(ByteBuffer src, boolean comp) {
        byte[] a = src.array();
        int i = src.position();
        int a0 = (comp ? Order.DESCENDING : Order.ASCENDING).apply(a[i]) & 0xff;
        if (a0 <= 240) return 1;
        if (a0 >= 241 && a0 <= 248) return 2;
        if (a0 == 249) return 3;
        if (a0 == 250) return 4;
        if (a0 == 251) return 5;
        if (a0 == 252) return 6;
        if (a0 == 253) return 7;
        if (a0 == 254) return 8;
        if (a0 == 255) return 9;
        throw new IllegalArgumentException("unexpected value in first byte: 0x"
                + Long.toHexString(a[i]));
    }

    /**
     * Decode a sequence of bytes in <code>buff</code> as an unsigned 64-bit
     * integer. Compliment the encoded value when <code>comp</code> is true.
     */
    @VisibleForTesting
    static long getVaruint64(ByteBuffer buff, boolean comp) {
        assert buff.remaining() >= lengthVaru64(buff, comp);
        long ret;
        Order ord = comp ? Order.DESCENDING : Order.ASCENDING;
        byte x = buff.get();
        int a0 = ord.apply(x) & 0xff, a1, a2, a3, a4, a5, a6, a7, a8;
        if (-1 == unsignedCmp(a0, 241)) {
            return a0;
        }
        x = buff.get();
        a1 = ord.apply(x) & 0xff;
        if (-1 == unsignedCmp(a0, 249)) {
            return (a0 - 241) * 256 + a1 + 240;
        }
        x = buff.get();
        a2 = ord.apply(x) & 0xff;
        if (a0 == 249) {
            return 2288 + 256 * a1 + a2;
        }
        x = buff.get();
        a3 = ord.apply(x) & 0xff;
        if (a0 == 250) {
            return (a1 << 16) | (a2 << 8) | a3;
        }
        x = buff.get();
        a4 = ord.apply(x) & 0xff;
        ret = (((long) a1) << 24) | (a2 << 16) | (a3 << 8) | a4;
        if (a0 == 251) {
            return ret;
        }
        x = buff.get();
        a5 = ord.apply(x) & 0xff;
        if (a0 == 252) {
            return (ret << 8) | a5;
        }
        x = buff.get();
        a6 = ord.apply(x) & 0xff;
        if (a0 == 253) {
            return (ret << 16) | (a5 << 8) | a6;
        }
        x = buff.get();
        a7 = ord.apply(x) & 0xff;
        if (a0 == 254) {
            return (ret << 24) | (a5 << 16) | (a6 << 8) | a7;
        }
        x = buff.get();
        a8 = ord.apply(x) & 0xff;
        return (ret << 32) | (((long) a5) << 24) | (a6 << 16) | (a7 << 8) | a8;
    }

    /**
     * Skip <code>buff</code> over the encoded bytes.
     */
    static void skipVaruint64(ByteBuffer buff, boolean comp) {
        buff.position(buff.position() + lengthVaru64(buff, comp));
    }

    /**
     * Read significand digits from <code>buff</code> according to the magnitude
     * of <code>e</code>.
     *
     * @param buff The source from which to read encoded digits.
     * @param e    The magnitude of the first digit read.
     * @param comp Treat encoded bytes as compliments when <code>comp</code> is true.
     * @return The decoded value.
     */
    private static BigDecimal decodeSignificand(ByteBuffer buff, int e, boolean comp) {
        byte[] a = buff.array();
        BigDecimal m = BigDecimal.ZERO;
        e--;
        for (int i = buff.position(); ; i++) {
            // base-100 digits are encoded as val * 2 + 1 except for the termination digit.
            m = m.add( // m +=
                    new BigDecimal(BigInteger.ONE, e * -2).multiply( // 100 ^ p * [decoded digit]
                            BigDecimal.valueOf(((comp ? Order.DESCENDING : Order.ASCENDING).apply(a[i]) & 0xff) / 2)));
            e--;
            // detect termination digit
            if (((comp ? Order.DESCENDING : Order.ASCENDING).apply(a[i]) & 1) == 0) {
                buff.position(i + 1);
                break;
            }
        }
        return m;
    }

    /**
     * Skip <code>buff</code> over the significand bytes.
     */
    private static void skipSignificand(ByteBuffer buff, boolean comp) {
        byte[] a = buff.array();
        for (int i = buff.position(); ; i++) {
            if (((comp ? Order.DESCENDING : Order.ASCENDING).apply(a[i]) & 1) == 0) {
                buff.position(i + 1);
                break;
            }
        }
    }

    /**
     * Encode the small positive floating point number <code>val</code> using
     * the key encoding. The caller guarantees that <code>val</code> will be
     * less than 1.0 and greater than 0.0.
     * <p>
     * A floating point value is encoded as an integer exponent <code>E</code>
     * and a mantissa <code>M</code>. The original value is equal to
     * <code>(M * 100^E)</code>. <code>E</code> is set to the smallest value
     * possible without making <code>M</code> greater than or equal to 1.0.
     * </p>
     * <p>
     * For this routine, <code>E</code> will always be zero or negative, since
     * the original value is less than one. The encoding written by this routine
     * is the ones-complement of the varint of the negative of <code>E</code>
     * followed by the mantissa:
     * <p/>
     * <pre>
     *   Encoding:   ~-E  M
     * </pre>
     * <p/>
     * </p>
     *
     * @param buff  The destination to which encoded digits are written.
     * @param val   The value to encode.
     * @param ecomp Write the compliment of <code>e</code> to <code>buff</code>
     *              when <code>ecomp</code> is true.
     * @param mcomp Write the compliment of <code>M</code> to <code>buff</code>
     *              when <code>mcomp</code> is true.
     */
    private static void encodeNumericSmall(ByteBuffer buff, BigDecimal val, boolean ecomp,
                                           boolean mcomp) {
        // assert 0.0 < dec < 1.0
        assert BigDecimal.ZERO.compareTo(val) < 0 && BigDecimal.ONE.compareTo(val) > 0;
        int e = 0, d, startM;
        Order ord = mcomp ? Order.DESCENDING : Order.ASCENDING;
        while (val.compareTo(EN10) < 0) {
            val = val.movePointRight(8);
            e += 4;
        }
        while (val.compareTo(EN2) < 0) {
            val = val.movePointRight(2);
            e++;
        }
        putVaruint64(buff, e, ecomp);
        startM = buff.position();
        for (int i = 0; i < 18 && val.compareTo(BigDecimal.ZERO) != 0; i++) {
            val = val.movePointRight(2);
            d = val.intValue();
            buff.put((byte) ((2 * d + 1) & 0xff));
            val = val.subtract(BigDecimal.valueOf(d));
        }
        buff.array()[buff.position() - 1] &= 0xfe;
        ord.apply(buff.array(), startM, buff.position() - startM);
    }

    /**
     * Encode the large positive floating point number <code>val</code> using
     * the key encoding. The caller guarantees that <code>val</code> will be
     * finite and greater than or equal to 1.0.
     * <p>
     * A floating point value is encoded as an integer exponent <code>E</code>
     * and a mantissa <code>M</code>. The original value is equal to
     * <code>(M * 100^E)</code>. <code>E</code> is set to the smallest value
     * possible without making <code>M</code> greater than or equal to 1.0.
     * </p>
     * <p>
     * Each centimal digit of the mantissa is stored in a byte. If the value of
     * the centimal digit is <code>X</code> (hence <code>X>=0</code> and
     * <code>X<=99</code>) then the byte value will be <code>2*X+1</code> for
     * every byte of the mantissa, except for the last byte which will be
     * <code>2*X+0</code>. The mantissa must be the minimum number of bytes
     * necessary to represent the value; trailing <code>X==0</code> digits are
     * omitted. This means that the mantissa will never contain a byte with the
     * value <code>0x00</code>.
     * </p>
     * <p>
     * If <code>E > 10</code>, then this routine writes of <code>E</code> as a
     * varint followed by the mantissa as described above. Otherwise, if
     * <code>E <= 10</code>, this routine only writes the mantissa and leaves
     * the <code>E</code> value to be encoded as part of the opening byte of the
     * field by the calling function.
     * <p/>
     * <pre>
     *   Encoding:  M       (if E<=10)
     *              E M     (if E>10)
     * </pre>
     * </p>
     * <p>
     * This routine returns the value of <code>E</code>.
     * </p>
     *
     * @param buff  The destination to which encoded digits are written.
     * @param val   The value to encode.
     * @param ecomp Write the compliment of <code>e</code> to <code>buff</code>
     *              when <code>ecomp</code> is true.
     * @param mcomp Write the compliment of <code>M</code> to <code>buff</code>
     *              when <code>mcomp</code> is true.
     * @return E(xponent) in base-100.
     */
    private static int encodeNumericLarge(ByteBuffer buff, BigDecimal val, boolean ecomp,
                                          boolean mcomp) {
        // assert val >= 0.0
        assert BigDecimal.ONE.compareTo(val) <= 0;
        int e = 0, d, startM;
        Order ord = mcomp ? Order.DESCENDING : Order.ASCENDING;
        while (val.compareTo(E32) >= 0 && e <= 350) {
            val = val.movePointLeft(32);
            e += 16;
        }
        while (val.compareTo(E8) >= 0 && e <= 350) {
            val = val.movePointLeft(8);
            e += 4;
        }
        while (val.compareTo(BigDecimal.ONE) >= 0 && e <= 350) {
            val = val.movePointLeft(2);
            e++;
        }
        if (e > 10) putVaruint64(buff, e, ecomp);
        startM = buff.position();
        for (int i = 0; i < 18 && val.compareTo(BigDecimal.ZERO) != 0; i++) {
            val = val.movePointRight(2);
            d = val.intValue();
            buff.put((byte) (2 * d + 1));
            val = val.subtract(BigDecimal.valueOf(d));
        }
        buff.array()[buff.position() - 1] &= 0xfe;
        ord.apply(buff.array(), startM, buff.position() - startM);
        return e;
    }

    /**
     * Encode a numerical value using the variable-length encoding.
     */
    public static void encodeNumeric(ByteBuffer buff, long val, Order ord) {
        int e, i, start = buff.position();
        if (val == 0) {
            buff.put(ZERO);
        } else if (val <= -1) {
            i = buff.position();
            buff.put(NEG_LARGE); /* Large negative number: 0x08, ~E, ~M */
            e = encodeNumericLarge(buff, BigDecimal.valueOf(val).negate(), true, true);
            if (e <= 10) buff.put(i, (byte) (NEG_MED_MAX - e)); /* Medium negative number: 0x13-E, ~M */
        } else {
            i = buff.position();
            buff.put(POS_LARGE); /* Large positive number: 0x22, E, M */
            e = encodeNumericLarge(buff, BigDecimal.valueOf(val), false, false);
            if (e <= 10) buff.put(i, (byte) (POS_MED_MIN + e)); /* Medium positive number: 0x17+E, M */
        }
        ord.apply(buff.array(), start, buff.position() - start);
    }

    /**
     * Encode a numerical value using the variable-length encoding.
     */
    public static void encodeNumeric(ByteBuffer buff, double val, Order ord) {
        int start = buff.position();
        if (Double.isNaN(val)) {
            buff.put(NAN);
            ord.apply(buff.array(), start, buff.position() - start);
        } else if (val == Double.NEGATIVE_INFINITY) {
            buff.put(NEG_INF);
            ord.apply(buff.array(), start, buff.position() - start);
        } else if (val == Double.POSITIVE_INFINITY) {
            buff.put(POS_INF);
            ord.apply(buff.array(), start, buff.position() - start);
        } else if (val == 0.0) {
            buff.put(ZERO);
            ord.apply(buff.array(), start, buff.position() - start);
        } else {
            encodeNumeric(buff, BigDecimal.valueOf(val), ord);
        }
    }

    /**
     * Encode a numerical value using the variable-length encoding.
     */
    public static void encodeNumeric(ByteBuffer buff, BigDecimal val, Order ord) {
        int e, i, start = buff.position();
        if (null == val) {
            encodeNull(buff, ord);
            return;
        } else if (BigDecimal.ZERO.compareTo(val) == 0) {
            buff.put(ZERO);
        } else if (NEG_ONE.compareTo(val) >= 0) { // v <= -1.0
            i = buff.position();
            buff.put(NEG_LARGE); /* Large negative number: 0x08, ~E, ~M */
            e = encodeNumericLarge(buff, val.negate(), true, true);
            if (e <= 10) buff.put(i, (byte) (NEG_MED_MAX - e)); /* Medium negative number: 0x13-E, ~M */
        } else if (BigDecimal.ZERO.compareTo(val) > 0) { // v < 0.0
            buff.put(NEG_SMALL); /* Small negative number: 0x14, -E, ~M */
            encodeNumericSmall(buff, val.negate(), false, true);
        } else if (BigDecimal.ONE.compareTo(val) > 0) { // v < 1.0
            buff.put(POS_SMALL); /* Small positive number: 0x16, ~-E, M */
            encodeNumericSmall(buff, val, true, false);
        } else {
            i = buff.position();
            buff.put(POS_LARGE); /* Large positive number: 0x22, E, M */
            e = encodeNumericLarge(buff, val, false, false);
            if (e <= 10) buff.put(i, (byte) (POS_MED_MIN + e)); /* Medium positive number: 0x17+E, M */
        }
        ord.apply(buff.array(), start, buff.position() - start);
    }

    /**
     * Encode a numerical value using the variable-length encoding.
     */
    public static void encodeNumeric(ByteBuffer buff, Numeric val, Order ord) {
        if (null == val) {
            encodeNull(buff, ord);
        } else if (val.isInteger()) {
            encodeNumeric(buff, val.longValue(), ord);
        } else if (val.isReal()) {
            encodeNumeric(buff, val.doubleValue(), ord);
        } else {
            encodeNumeric(buff, val.exactValue(), ord);
        }
    }

    /**
     * Decode a Numerical value from the variable-length encoding. The backing
     * array is not modified through use of this method.
     */
    public static Numeric decodeNumeric(ByteBuffer buff) {
        byte header = buff.get();
        if (header == NULL || header == Order.DESCENDING.apply(NULL))
            return null;
        int e = 0;
        boolean dsc = (-1 == Integer.signum(header));
        if (dsc) header = Order.DESCENDING.apply(header);

        if (header == NAN) {
            return Numeric.NaN;
        } else if (header == NEG_INF) {
            return Numeric.NEGATIVE_INFINITY;
        } else if (header == NEG_LARGE) { /* Large negative number: 0x08, ~E, ~M */
            e = (int) getVaruint64(buff, !dsc);
            return new Numeric(decodeSignificand(buff, e, !dsc).negate());
        } else if (header >= NEG_MED_MIN && header <= NEG_MED_MAX) {
      /* Medium negative number: 0x13-E, ~M */
            e = NEG_MED_MAX - header;
            return new Numeric(decodeSignificand(buff, e, !dsc).negate());
        } else if (header == NEG_SMALL) { /* Small negative number: 0x14, -E, ~M */
            e = (int) -getVaruint64(buff, dsc);
            return new Numeric(decodeSignificand(buff, e, !dsc).negate());
        } else if (header == ZERO) {
            return Numeric.ZERO;
        } else if (header == POS_SMALL) { /* Small positive number: 0x16, ~-E, M */
            e = (int) -getVaruint64(buff, !dsc);
            return new Numeric(decodeSignificand(buff, e, dsc));
        } else if (header >= POS_MED_MIN && header <= POS_MED_MAX) {
      /* Medium positive number: 0x17+E, M */
            e = header - POS_MED_MIN;
            return new Numeric(decodeSignificand(buff, e, dsc));
        } else if (header == POS_LARGE) { /* Large positive number: 0x22, E, M */
            e = (int) getVaruint64(buff, dsc);
            return new Numeric(decodeSignificand(buff, e, dsc));
        } else if (header == POS_INF) {
            return Numeric.POSITIVE_INFINITY;
        } else {
            throw new IllegalArgumentException("unexpected value in first byte: 0x"
                    + Long.toHexString(header));
        }
    }

    /**
     * Encode a String value.
     */
    public static void encodeString(ByteBuffer buff, String val, Order ord) {
        if (null == val) {
            encodeNull(buff, ord);
            return;
        }
        if (val.contains("\u0000"))
            throw new IllegalArgumentException("Cannot encode String values containing '\\u0000'");
        int start = buff.position();
        buff.put(TEXT);
        buff.put(val.getBytes(UTF8));
        buff.put(TERM);
        ord.apply(buff.array(), start, buff.position() - start);
    }

    /**
     * Decode a String value. The backing array is not modified through use of
     * this method.
     */
    public static String decodeString(ByteBuffer buff) {
        byte header = buff.get();
        if (header == NULL || header == Order.DESCENDING.apply(NULL))
            return null;
        assert header == TEXT || header == Order.DESCENDING.apply(TEXT);
        Order ord = header == TEXT ? Order.ASCENDING : Order.DESCENDING;
        byte[] a = buff.array();
        int start = buff.position(), i = start;
        byte terminator = ord.apply(TERM);
        while (a[i] != terminator) i++;
        buff.position(++i);
        if (Order.DESCENDING == ord) {
            byte[] copy = Arrays.copyOfRange(a, start, i - 1);
            ord.apply(copy);
            return new String(copy, UTF8);
        } else {
            return new String(a, start, i - start - 1, UTF8);
        }
    }

    /**
     * Calculate the expected BlobVar encoded length based on unencoded length.
     */
    public static int blobVarEncodedLength(int len) {
        if (0 == len)
            return 2; // 1-byte header + 1-byte terminator
        else
            return (int)
                    Math.ceil(
                            (len * 8) // 8-bits per input byte
                                    / 7.0)    // 7-bits of input data per encoded byte, rounded up
                    + 1;        // + 1-byte header
    }

    /**
     * Calculate the expected BlobVar decoded length based on encoded length.
     */
    @VisibleForTesting
    static int blobVarDecodedLength(int len) {
        return
                ((len
                        - 1) // 1-byte header
                        * 7) // 7-bits of payload per encoded byte
                        / 8; // 8-bits per byte
    }

    /**
     * Encode a BLOB value using a modified varint encoding scheme.
     * <p>
     * This format encodes a byte[] value such that no limitations on the input
     * value are imposed. The first byte encodes the encoding scheme that
     * follows, 0x35. Each encoded byte thereafter has a header bit indicating
     * whether there is another encoded byte following. A header bit of '1'
     * indicates continuation of the encoding. A header bit of '0' indicates
     * this byte encodes the final byte. An empty input value is a special case,
     * wherein a NULL byte is used as a termination byte. The remaining 7 bits
     * on each encoded byte carry the value payload.
     * </p>
     */
    public static void encodeBlobVar(ByteBuffer buff, byte[] val, Order ord) {
        if (null == val) {
            encodeNull(buff, ord);
            return;
        }
        // Empty value is null-terminated. All other values are encoded as 7-bits per byte.
        assert buff.remaining() >= blobVarEncodedLength(val.length) : "buffer overflow expected.";
        int start = buff.position();
        buff.put(BLOB_VAR);
        if (0 == val.length) {
            buff.put(TERM);
        } else {
            byte s = 1, t = 0;
            for (int i = 0; i < val.length; i++) {
                buff.put((byte) (0x80 | t | ((val[i] & 0xff) >>> s)));
                if (s < 7) {
                    t = (byte) (val[i] << (7 - s));
                    s++;
                } else {
                    buff.put((byte) (0x80 | val[i]));
                    s = 1;
                    t = 0;
                }
            }
            if (s > 1) {
                buff.put((byte) (0x7f & t));
            } else {
                buff.array()[buff.position() - 1] &= 0x7f;
            }
        }
        ord.apply(buff.array(), start, buff.position() - start);
    }

    /**
     * Decode a blob value that was encoded using BlobVar encoding. The backing
     * array is not modified through use of this method.
     */
    public static byte[] decodeBlobVar(ByteBuffer buff) {
        byte header = buff.get();
        if (header == NULL || header == Order.DESCENDING.apply(NULL))
            return null;
        assert header == BLOB_VAR || header == Order.DESCENDING.apply(BLOB_VAR);
        Order ord = BLOB_VAR == header ? Order.ASCENDING : Order.DESCENDING;
        byte[] a = buff.array();
        int start = buff.position(), end;
        if (a[start] == ord.apply(TERM)) {
            // skip empty input buffer.
            buff.get();
            return new byte[0];
        }
        for (end = start; (byte) (ord.apply(a[end]) & 0x80) != TERM; end++) ;
        end++; // increment end to 1-past last byte
        // create ret buffer using length of encoded data + 1 (header byte)
        ByteBuffer ret = ByteBuffer.allocate(blobVarDecodedLength(end - start + 1));
        int s = 6;
        byte t = (byte) ((ord.apply(a[start]) << 1) & 0xff);
        for (int i = start + 1; i < end; i++) {
            if (s == 7) {
                ret.put((byte) (t | (ord.apply(a[i]) & 0x7f)));
                i++;
            } else {
                ret.put((byte) (t | ((ord.apply(a[i]) & 0x7f) >>> s)));
            }
            if (i == end) break;
            t = (byte) ((ord.apply(a[i]) << 8 - s) & 0xff);
            s = s == 1 ? 7 : s - 1;
        }
        buff.position(end);
        assert t == 0 : "Unexpected bits remaining after decoding blob.";
        return ret.array();
    }

    /**
     * Encode a Blob value as a byte-for-byte copy.
     */
    public static void encodeBlobCopy(ByteBuffer buff, byte[] val, int offset, int len, Order ord) {
        if (null == val) {
            encodeNull(buff, ord);
            if (Order.DESCENDING == ord) {
                // DESCENDING ordered BlobCopy requires a termination bit to preserve
                // sort-order semantics of null values.
                buff.put(ord.apply(TERM));
            }
            return;
        }
        // Blobs as final entry in a compound key are written unencoded.
        int overhead = Order.ASCENDING == ord ? 1 : 2;
        assert buff.remaining() >= len + overhead;
        for (int i = offset; i < offset + len; i++) {
            if (val[i] == 0x00)
                throw new IllegalArgumentException("0x00 bytes not permitted in value.");
        }
        int start = buff.position();
        buff.put(BLOB_COPY);
        buff.put(val, offset, len);
        // DESCENDING ordered BlobCopy requires a termination bit to preserve
        // sort-order semantics of null values.
        if (Order.DESCENDING == ord) buff.put(TERM);
        ord.apply(buff.array(), start, buff.position() - start);
    }

    /**
     * Encode a Blob value as a byte-for-byte copy.
     */
    public static void encodeBlobCopy(ByteBuffer buff, byte[] val, Order ord) {
        encodeBlobCopy(buff, val, 0, null != val ? val.length : 0, ord);
    }

    /**
     * Decode a Blob value, byte-for-byte copy. The backing array is not
     * modified through use of this method.
     */
    public static byte[] decodeBlobCopy(ByteBuffer buff) {
        byte header = buff.get();
        if (header == NULL) {
            return null;
        } else if (header == Order.DESCENDING.apply(NULL)) {
            buff.get(); // read DESCENDING order termination bit.
            return null;
        }

        assert header == BLOB_COPY || header == Order.DESCENDING.apply(BLOB_COPY);
        Order ord = header == BLOB_COPY ? Order.ASCENDING : Order.DESCENDING;
        int length = buff.limit() - buff.position() - (Order.ASCENDING == ord ? 0 : 1);
        byte[] ret = new byte[length];
        buff.get(ret);
        if (Order.DESCENDING == ord) buff.get(); // throw away the termination marker.
        ord.apply(ret, 0, ret.length);
        return ret;
    }

    /**
     * Encode a null value.
     */
    public static void encodeNull(ByteBuffer buff, Order ord) {
        buff.put(ord.apply(NULL));
    }

    /**
     * Encode an <code>int32</code> value using the fixed-length encoding.
     */
    public static void encodeInt32(ByteBuffer buff, int val, Order ord) {
        int start = buff.position();
        buff.put(FIXED_INT32);
        buff.put((byte) ((val >> 24) ^ 0x80))
                .put((byte) (val >> 16))
                .put((byte) (val >> 8))
                .put((byte) val);
        ord.apply(buff.array(), start, buff.position() - start);
    }

    /**
     * Decode an <code>int32</code> value. The backing array is not modified
     * through use of this method.
     */
    public static int decodeInt32(ByteBuffer buff) {
        byte header = buff.get();
        assert header == FIXED_INT32 || header == Order.DESCENDING.apply(FIXED_INT32);
        Order ord = header == 0x27 ? Order.ASCENDING : Order.DESCENDING;
        int val = (ord.apply(buff.get()) ^ 0x80) & 0xff;
        for (int i = 1; i < 4; i++) {
            val = (val << 8) + (ord.apply(buff.get()) & 0xff);
        }
        return val;
    }

    /**
     * Encode an <code>int64</code> value using the fixed-length encoding.
     */
    public static void encodeInt64(ByteBuffer buff, long val, Order ord) {
        int start = buff.position();
        buff.put(FIXED_INT64);
        buff.put((byte) ((val >> 56) ^ 0x80))
                .put((byte) (val >> 48))
                .put((byte) (val >> 40))
                .put((byte) (val >> 32))
                .put((byte) (val >> 24))
                .put((byte) (val >> 16))
                .put((byte) (val >> 8))
                .put((byte) val);
        ord.apply(buff.array(), start, buff.position() - start);
    }

    /**
     * Decode an <code>int64</code> value. The backing array is not modified
     * through use of this method.
     */
    public static long decodeInt64(ByteBuffer buff) {
        byte header = buff.get();
        assert header == FIXED_INT64 || header == Order.DESCENDING.apply(FIXED_INT64);
        Order ord = header == FIXED_INT64 ? Order.ASCENDING : Order.DESCENDING;
        long val = (ord.apply(buff.get()) ^ 0x80) & 0xff;
        for (int i = 1; i < 8; i++) {
            val = (val << 8) + (ord.apply(buff.get()) & 0xff);
        }
        return val;
    }

    /**
     * Encode a 32-bit floating point value using the fixed-length encoding.
     *
     * @see #decodeFloat32(ByteBuffer)
     */
    public static void encodeFloat32(ByteBuffer buff, float val, Order ord) {
        int start = buff.position();
        int i = Float.floatToIntBits(val);
        i ^= ((i >> Integer.SIZE - 1) | Integer.MIN_VALUE);
        buff.put(FIXED_FLOAT32);
        buff.put((byte) (i >> 24))
                .put((byte) (i >> 16))
                .put((byte) (i >> 8))
                .put((byte) i);
        ord.apply(buff.array(), start, buff.position() - start);
    }

    /**
     * Decode a 32-bit floating point value using the fixed-length encoding.
     *
     * @see #encodeFloat32(ByteBuffer, float, Order)
     */
    public static float decodeFloat32(ByteBuffer buff) {
        byte header = buff.get();
        assert header == FIXED_FLOAT32 || header == Order.DESCENDING.apply(FIXED_FLOAT32);
        Order ord = header == FIXED_FLOAT32 ? Order.ASCENDING : Order.DESCENDING;
        int val = ord.apply(buff.get()) & 0xff;
        for (int i = 1; i < 4; i++) {
            val = (val << 8) + (ord.apply(buff.get()) & 0xff);
        }
        val ^= (~val >> Integer.SIZE - 1) | Integer.MIN_VALUE;
        return Float.intBitsToFloat(val);
    }

    /**
     * Encode a 64-bit floating point value using the fixed-length encoding.
     * <p>
     * This format ensures the following total ordering of floating point
     * values: Double.NEGATIVE_INFINITY &lt; -Double.MAX_VALUE &lt; ... &lt;
     * -Double.MIN_VALUE &lt; -0.0 &lt; +0.0; &lt; Double.MIN_VALUE &lt; ...
     * &lt; Double.MAX_VALUE &lt; Double.POSITIVE_INFINITY &lt; Double.NaN
     * </p>
     * Floating point numbers are encoded as specified in IEEE 754. A 64-bit
     * double precision float consists of a sign bit, 11-bit unsigned exponent
     * encoded in offset-1023 notation, and a 52-bit significand. The format is
     * described further in the <a
     * href="http://en.wikipedia.org/wiki/Double_precision"> Double Precision
     * Floating Point Wikipedia page</a> </p>
     * <p>
     * The value of a normal float is -1 <sup>sign bit</sup> &times;
     * 2<sup>exponent - 1023</sup> &times; 1.significand
     * </p>
     * <p>
     * The IEE754 floating point format already preserves sort ordering for
     * positive floating point numbers when the raw bytes are compared in most
     * significant byte order. This is discussed further at <a href=
     * "http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm"
     * > http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.
     * htm</a>
     * </p>
     * <p>
     * Thus, we need only ensure that negative numbers sort in the the exact
     * opposite order as positive numbers (so that say, negative infinity is
     * less than negative 1), and that all negative numbers compare less than
     * any positive number. To accomplish this, we invert the sign bit of all
     * floating point numbers, and we also invert the exponent and significand
     * bits if the floating point number was negative.
     * </p>
     * <p>
     * More specifically, we first store the floating point bits into a 64-bit
     * long <code>l</code> using {@link Double#doubleToLongBits}. This method
     * collapses all NaNs into a single, canonical NaN value but otherwise
     * leaves the bits unchanged. We then compute
     * </p>
     * <p/>
     * <pre>
     * l &circ;= (l &gt;&gt; (Long.SIZE - 1)) | Long.MIN_SIZE
     * </pre>
     * <p>
     * which inverts the sign bit and XOR's all other bits with the sign bit
     * itself. Comparing the raw bytes of <code>l</code> in most significant
     * byte order is equivalent to performing a double precision floating point
     * comparison on the underlying bits (ignoring NaN comparisons, as NaNs
     * don't compare equal to anything when performing floating point
     * comparisons).
     * </p>
     * <p>
     * The resulting long integer is then converted into a byte array by
     * serializing the long one byte at a time in most significant byte order.
     * The serialized integer is prefixed by a single header byte. All
     * serialized values are 9 bytes in length.
     * </p>
     */
    public static void encodeFloat64(ByteBuffer buff, double val, Order ord) {
        int start = buff.position();
        long lng = Double.doubleToLongBits(val);
        lng ^= ((lng >> Long.SIZE - 1) | Long.MIN_VALUE);
        buff.put(FIXED_FLOAT64);
        buff.put((byte) (lng >> 56))
                .put((byte) (lng >> 48))
                .put((byte) (lng >> 40))
                .put((byte) (lng >> 32))
                .put((byte) (lng >> 24))
                .put((byte) (lng >> 16))
                .put((byte) (lng >> 8))
                .put((byte) lng);
        ord.apply(buff.array(), start, buff.position() - start);
    }

    /**
     * Decode a 64-bit floating point value using the fixed-length encoding.
     *
     * @see #encodeFloat64(ByteBuffer, double, Order)
     */
    public static double decodeFloat64(ByteBuffer buff) {
        byte header = buff.get();
        assert header == FIXED_FLOAT64 || header == Order.DESCENDING.apply(FIXED_FLOAT64);
        Order ord = header == FIXED_FLOAT64 ? Order.ASCENDING : Order.DESCENDING;
        long val = ord.apply(buff.get()) & 0xff;
        for (int i = 1; i < 8; i++) {
            val = (val << 8) + (ord.apply(buff.get()) & 0xff);
        }
        val ^= (~val >> Long.SIZE - 1) | Long.MIN_VALUE;
        return Double.longBitsToDouble(val);
    }

    /**
     * Skip <code>buff</code>'s cursor forward one encoded value.
     */
    public static void skip(ByteBuffer buff) {
        byte x = buff.get();
        Order ord = (-1 == Integer.signum(x)) ? Order.DESCENDING : Order.ASCENDING;
        x = ord.apply(x);

        switch (x) {
            case NULL:
            case NEG_INF:
                return;
            case NEG_LARGE: /* Large negative number: 0x08, ~E, ~M */
                skipVaruint64(buff, Order.DESCENDING != ord);
                skipSignificand(buff, Order.DESCENDING != ord);
                return;
            case NEG_MED_MIN: /* Medium negative number: 0x13-E, ~M */
            case NEG_MED_MIN + 0x01:
            case NEG_MED_MIN + 0x02:
            case NEG_MED_MIN + 0x03:
            case NEG_MED_MIN + 0x04:
            case NEG_MED_MIN + 0x05:
            case NEG_MED_MIN + 0x06:
            case NEG_MED_MIN + 0x07:
            case NEG_MED_MIN + 0x08:
            case NEG_MED_MIN + 0x09:
            case NEG_MED_MAX:
                skipSignificand(buff, Order.DESCENDING != ord);
                return;
            case NEG_SMALL: /* Small negative number: 0x14, -E, ~M */
                skipVaruint64(buff, Order.DESCENDING == ord);
                skipSignificand(buff, Order.DESCENDING != ord);
                return;
            case ZERO:
                return;
            case POS_SMALL: /* Small positive number: 0x16, ~-E, M */
                skipVaruint64(buff, Order.DESCENDING != ord);
                skipSignificand(buff, Order.DESCENDING == ord);
                return;
            case POS_MED_MIN: /* Medium positive number: 0x17+E, M */
            case POS_MED_MIN + 0x01:
            case POS_MED_MIN + 0x02:
            case POS_MED_MIN + 0x03:
            case POS_MED_MIN + 0x04:
            case POS_MED_MIN + 0x05:
            case POS_MED_MIN + 0x06:
            case POS_MED_MIN + 0x07:
            case POS_MED_MIN + 0x08:
            case POS_MED_MIN + 0x09:
            case POS_MED_MAX:
                skipSignificand(buff, Order.DESCENDING == ord);
                return;
            case POS_LARGE: /* Large positive number: 0x22, E, M */
                skipVaruint64(buff, Order.DESCENDING == ord);
                skipSignificand(buff, Order.DESCENDING == ord);
                return;
            case POS_INF:
                return;
            case NAN:
                return;
            case FIXED_INT32:
                buff.position(buff.position() + 4);
                return;
            case FIXED_INT64:
                buff.position(buff.position() + 8);
                return;
            case FIXED_FLOAT32:
                buff.position(buff.position() + 4);
                return;
            case FIXED_FLOAT64:
                buff.position(buff.position() + 8);
                return;
            case TEXT:
                // for null-terminated values, skip to the end.
                do {
                    x = ord.apply(buff.get());
                } while (x != TERM);
                return;
            case BLOB_VAR:
                // read until we find a 0 in the MSB
                do {
                    x = ord.apply(buff.get());
                } while ((byte) (x & 0x80) != TERM);
                return;
            case BLOB_COPY:
                if (Order.DESCENDING == ord) {
                    // if descending, read to termination byte.
                    do {
                        x = ord.apply(buff.get());
                    } while (x != TERM);
                } else {
                    // otherwise, just skip to the end.
                    buff.position(buff.limit());
                }
                return;
            default:
                throw new IllegalArgumentException("unexpected value in first byte: 0x"
                        + Long.toHexString(x));
        }
    }

    /**
     * Return the number of encoded entries remaining in <code>buff</code>. The
     * state of <code>buff</code> is not modified through use of this method.
     */
    public static int length(ByteBuffer buff) {
        ByteBuffer b = buff.duplicate();
        int cnt = 0;
        for (cnt = 0; b.position() != b.limit(); cnt++) {
            skip(b);
        }
        return cnt;
    }
}