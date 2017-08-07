package com.gravity.utilities

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import com.gravity.hbase.schema.{PrimitiveInputStream, PrimitiveOutputStream}
import org.junit.Test
import org.junit.Assert._
import VariableLengthEncoding._

/**
 * Created by tbenavides14 on 7/1/15.
 *
 * Author:
 */
class VariableLengthEncodingTest {
  val intMin: Int = -528
  val intMax: Int = intMin + 128000
  val intMin2: Int = Int.MaxValue - 128000
  val intMax2: Int = Int.MaxValue
  val intMin3: Int = Int.MinValue
  val intMax3: Int = intMin3 + 128000
  val longMin: Long = (Int.MaxValue.toLong + 1)
  val longMax: Long = longMin + 128000

  @Test def intStreamTest() {
    val bos = new ByteArrayOutputStream()
    val output = new PrimitiveOutputStream(bos)
    for(i <- intMin to intMax) {
      writeToStream(i, output)
    }
    val writtenBytes = bos.toByteArray
    val bis = new ByteArrayInputStream(writtenBytes)
    val input = new PrimitiveInputStream(bis)

    for(i <- intMin to intMax) {
      assertEquals(i, readIntFromStream(input))
    }
  }

  @Test def intStreamTest2() {
    val bos = new ByteArrayOutputStream()
    val output = new PrimitiveOutputStream(bos)
    for(i <- intMin2 to intMax2) {
      writeToStream(i, output)
    }
    val writtenBytes = bos.toByteArray
    val bis = new ByteArrayInputStream(writtenBytes)
    val input = new PrimitiveInputStream(bis)

    for(i <- intMin2 to intMax2) {
      assertEquals(i, readIntFromStream(input))
    }
  }

  @Test def intStreamTest3() {
    val bos = new ByteArrayOutputStream()
    val output = new PrimitiveOutputStream(bos)
    for(i <- intMin3 to intMax3) {
      writeToStream(i, output)
    }
    val writtenBytes = bos.toByteArray
    val bis = new ByteArrayInputStream(writtenBytes)
    val input = new PrimitiveInputStream(bis)

    for(i <- intMin3 to intMax3) {
      assertEquals(i, readIntFromStream(input))
    }
  }

  @Test def longStreamTest() {
    val bos = new ByteArrayOutputStream()
    val output = new PrimitiveOutputStream(bos)
    for(i <- longMin until longMax) {
      writeToStream(i, output)
    }
    val writtenBytes = bos.toByteArray
    val bis = new ByteArrayInputStream(writtenBytes)
    val input = new PrimitiveInputStream(bis)

    for(i <- longMin until longMax) {
      assertEquals(i, readLongFromStream(input))
    }
  }

  @Test def intTest() {
    for (i <- intMin until intMax) {
      val encoded = encode(i)
      //println(i.toString + " -> " + toHexString(encoded))
      val decoded = decodeToInt(encoded)
      assertEquals(i, decoded)
    }

    for (i <- intMin2 until intMax2) {
      val encoded = encode(i)
      //println(i.toString + " -> " + toHexString(encoded))
      val decoded = decodeToInt(encoded)
      assertEquals(i, decoded)
    }

    for (i <- intMin3 until intMax3) {
      val encoded = encode(i)
      //println(i.toString + " -> " + toHexString(encoded))
      val decoded = decodeToInt(encoded)
      assertEquals(i, decoded)
    }
  }

  @Test def longTest() {
    for(i <- (Int.MaxValue.toLong + 1) until (Int.MaxValue.toLong + 128000L)) {
      val encoded = encode(i)
      val decoded = decodeToLong(encoded)
      //println(i.toString + " -> " + toHexString(encoded) + " -> " + decoded)
      assertEquals(i, decoded)
    }
  }

}

//object VLEPerfTest extends App {
//  val testItem : Int = 42
//
//  readLine("ready")
//  VariableLengthEncoding.encode(testItem)
//  readLine("done")
//
//}