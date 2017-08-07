package com.gravity.utilities

import java.util.zip.CRC32
import java.security.MessageDigest
import scala.util.Random
import java.util.UUID

/**
 * Created by Jim Plush
 * User: jim
 * Date: 8/31/11
 */

object HashUtils {
  private val md5Digest = MessageDigest.getInstance("MD5")
  private val rand = new Random()

  private def genSaltBytes: Array[Byte] = {
    val buf = Array.ofDim[Byte](8)
    rand.synchronized(rand.nextBytes(buf))
    buf
  }

  private def digestBytes(bytes: Array[Byte]): Array[Byte] = {
    md5Digest.synchronized {
      md5Digest.reset()
      md5Digest.digest(bytes)
    }
  }

  private val eightIndexes = Seq(7, 6, 5, 4, 3, 2, 1, 0)

  /**
   * Convert a long value to a byte array using big-endian.
   * @param value the value to convert
   * @return
   */
  def longBytes(value: Long): Array[Byte] = {
    val bytes = Array.ofDim[Byte](8)
    var shifter = value

    for (i <- eightIndexes) {
      bytes(i) = shifter.toByte
      if (i > 0) shifter >>>= 8
    }

    bytes
  }

  private val fourIndexes = Seq(3, 2, 1, 0)

  /**
   * Convert an int value to a byte array using big-endian.
   * @param value the value to convert
   * @return
   */
  def intBytes(value: Int): Array[Byte] = {
    val bytes = Array.ofDim[Byte](4)
    var shifter = value

    for (i <- fourIndexes) {
      bytes(i) = shifter.toByte
      if (i > 0) shifter >>>= 8
    }

    bytes
  }

  def shortBytes(value: Short): Array[Byte] = {
    Array((value >> 8).toByte, value.toByte)
  }

  def doubleBytes(value: Double): Array[Byte] = {
    longBytes(java.lang.Double.doubleToRawLongBits(value))
  }

  def floatBytes(value: Float): Array[Byte] = {
    intBytes(java.lang.Float.floatToRawIntBits(value))
  }

  private val trueBytes = Array(-1.toByte)
  private val falseBytes = Array(0.toByte)

  def booleanBytes(value: Boolean): Array[Byte] = if (value) trueBytes else falseBytes

  def generateHashCode(product: Product): Int = {
    val arr =  product.productArity

    if (arr == 0) return product.productPrefix.hashCode

    val buf = Array.ofDim[Array[Byte]](arr)

    var i = 0
    while (i < arr) {
      buf(i) = product.productElement(i) match {
        case x: Int => intBytes(x)
        case x: Long => longBytes(x)
        case x: Short => shortBytes(x)
        case x: Boolean => booleanBytes(x)
        case x: Double => doubleBytes(x)
        case x: Float => floatBytes(x)
        case x: String => x.getBytes
        case other => intBytes(other.hashCode())
      }
      i += 1
    }

    generateHashCode(buf: _*)
  }

  def generateHashCode(bytes: Array[Byte]*): Int = {
    val sizeOf = bytes.foldLeft(0)(_ + _.length)
    val buf = Array.ofDim[Byte](sizeOf)

    var pos = 0
    for (arr <- bytes) {
      val len = arr.length
      System.arraycopy(arr, 0, buf, pos, len)
      pos += len
    }

    hashedBytesToMurmurLong(buf).toInt
  }

  def hashedBytesToString(bytes: Array[Byte]): String = {
    val buf = new StringBuilder

    def appendNibble(nibble: Int) {
      if ((0 <= nibble) && (nibble <= 9)) {
        buf.append(('0' + nibble).toChar)
      } else {
        buf.append(('a' + (nibble - 10)).toChar)
      }
    }

    for (byte <- bytes) {
      appendNibble((byte >>> 4) & 0x0F)
      appendNibble(byte & 0x0F)
    }

    buf.toString()
  }

  def murmurHashStringToLong(input: String): Long = MurmurHash.hash64(input)

  def hashedBytesToMurmurLong(bytes: Array[Byte]): Long = MurmurHash.hash64(bytes, bytes.length)

  /** Generates a hex encoded MD5 hash for an input string using platform default charset */
  def md5(s:String): String = hashedBytesToString(md5bytes(s))

  def randomMd5: String = md5(UUID.randomUUID().toString)

  def md5bytes(s: String): Array[Byte] = digestBytes(s.getBytes)

  def generateSaltedMd5(): String = generateSaltedMd5(Array.empty[Byte])

  def generateSaltedMd5(s: String): String = generateSaltedMd5(s.getBytes)

  def generateSaltedMd5(withBytes: Array[Byte]): String = hashedBytesToString(generateSaltedMd5bytes(withBytes))

  /** Generates a hex encoded MD5 hash for an input string using specified charset */
  def md5(s:String, charset: String): String = hashedBytesToString(md5bytes(s, charset))
  def md5bytes(s: String, charset: String): Array[Byte] = digestBytes(s.getBytes(charset))

  /**
   * we generate an MD5 array of bytes based on the:
   *   1) specified withBytes
   *   2) current milliseconds.
   *   3) and a salt of 8 random bytes.
   *
   * @param withBytes Bytes specific to your own usage
   * @return
   */
  def generateSaltedMd5bytes(withBytes: Array[Byte]): Array[Byte] = {

    val nowBytes = longBytes(grvtime.currentMillis)
    val saltBytes = genSaltBytes

    val userLen = withBytes.length
    val nowLen = nowBytes.length
    val saltLen = saltBytes.length

    val bytes = Array.ofDim[Byte](userLen + nowLen + saltLen)

    if (userLen > 0) System.arraycopy(withBytes, 0, bytes, 0, userLen)
    System.arraycopy(nowBytes, 0, bytes, userLen, nowLen)
    System.arraycopy(saltBytes, 0, bytes, userLen + nowLen, saltLen)

    digestBytes(bytes)
  }

  /**
  * generates a crc32 hash given your input string
  */
  def crc32(str: String): Long = {
    val bytes = str.getBytes
    val checksum = new CRC32()
    checksum.update(bytes, 0, bytes.length)
    checksum.getValue
  }
}