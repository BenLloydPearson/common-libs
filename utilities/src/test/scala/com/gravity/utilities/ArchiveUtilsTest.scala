package com.gravity.utilities

import org.scalatest.Matchers

import scala.util.Random

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 * Jul 10, 2014
 */
class ArchiveUtilsTest extends BaseScalaTest with Matchers {

  test ("compress and decompress should be equal (2k length - unicode chars)") {
    val string = new String(Random.nextString(2048).toArray)
    val compressed = ArchiveUtils.compressString(string)
    val decompressed = ArchiveUtils.decompressString(compressed)
    decompressed should be (string)
  }

  test ("compress and decompress should be equal (0 length - alphanumeric chars)") {
    val string = new String(Random.alphanumeric.take(0).toArray)
    val compressed = ArchiveUtils.compressString(string)
    val decompressed = ArchiveUtils.decompressString(compressed)
    decompressed should be (string)
  }

  test ("compress and decompress should be equal (2k length - alphanumeric chars)") {
    val string = new String(Random.alphanumeric.take(1024 * 2).toArray)
    val compressed = ArchiveUtils.compressString(string)
    val decompressed = ArchiveUtils.decompressString(compressed)
    decompressed should be (string)
  }

  test ("compress and decompress should be equal (16k length - alphanumeric chars)") {
    val string = new String(Random.alphanumeric.take(1024 * 16).toArray)
    val compressed = ArchiveUtils.compressString(string)
    val decompressed = ArchiveUtils.decompressString(compressed)
    decompressed should be (string)
  }
  test ("compress and decompress should be equal (headerless, 2k length - unicode chars)") {
    val string = new String(Random.nextString(2048).toArray)
    val compressed = ArchiveUtils.compressString(string, withHeader = false)
    val decompressed = ArchiveUtils.decompressString(compressed, withHeader = false)
    decompressed should be (string)
  }

  test ("compress and decompress should be equal (headerless, 0 length - alphanumeric chars)") {
    val string = new String(Random.alphanumeric.take(0).toArray)
    val compressed = ArchiveUtils.compressString(string, withHeader = false)
    val decompressed = ArchiveUtils.decompressString(compressed, withHeader = false)
    decompressed should be (string)
  }

  test ("compress and decompress should be equal (headerless, 2k length - alphanumeric chars)") {
    val string = new String(Random.alphanumeric.take(1024 * 2).toArray)
    val compressed = ArchiveUtils.compressString(string, withHeader = false)
    val decompressed = ArchiveUtils.decompressString(compressed, withHeader = false)
    decompressed should be (string)
  }

  test ("compress and decompress should be equal (headerless, 16k length - alphanumeric chars)") {
    val string = new String(Random.alphanumeric.take(1024 * 16).toArray)
    val compressed = ArchiveUtils.compressString(string, withHeader = false)
    val decompressed = ArchiveUtils.decompressString(compressed, withHeader = false)
    decompressed should be (string)
  }
}
