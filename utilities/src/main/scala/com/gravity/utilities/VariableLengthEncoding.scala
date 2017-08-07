package com.gravity.utilities

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream}
import com.gravity.hbase.schema.{PrimitiveInputStream, PrimitiveOutputStream}


/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/30/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object VariableLengthEncoding {
  def encode(x:Long) : Array[Byte] = {
    var l = x >>> 7

    if(l > 0) {
      val result = scala.collection.mutable.Stack[Byte]()
      result.push((x & 0x7f).toByte)
      while (l > 0) {
        result.push(((l & 0x7f) | 0x80).toByte)
        l >>>= 7
      }
      result.toArray
    }
    else {
      Array((x & 0x7f).toByte)
    }
  }

  def encode(x:Int) : Array[Byte] = {
    var l = x >>> 7

    if(l > 0) {
      val result = scala.collection.mutable.Stack[Byte]()
      result.push((x & 0x7f).toByte)
      while (l > 0) {
        result.push(((l & 0x7f) | 0x80).toByte)
        l >>>= 7
      }
      result.toArray
    }
    else {
      Array((x & 0x7f).toByte)
    }
  }

  def writeToStream(x: Int, output: PrimitiveOutputStream) {
    output.write(encode(x))
  }

  def writeToStream(x: Long, output: PrimitiveOutputStream) {
    output.write(encode(x))
  }

  def writeToStream(x: Int, output: ByteArrayOutputStream) {
    output.write(encode(x))
  }

  def writeToStream(x: Long, output: ByteArrayOutputStream) {
    output.write(encode(x))
  }

  def writeToStream(x: Int, output: ObjectOutputStream) {
    output.write(encode(x))
  }

  def writeToStream(x: Long, output: ObjectOutputStream) {
    output.write(encode(x))
  }

  def decodeToInt(bytes: Array[Byte]) : Int = {
    bytes.foldLeft(0)((r, b) => r<<7|b&0x7f)
  }

  def decodeToLong(bytes: Array[Byte]) : Long = {
    bytes.foldLeft(0L)((r, b) => r<<7|b&0x7f)
  }

  def decodeToInt(bytes: Array[Byte], start: Int) : (Int, Int) = {
    var index = start
    var readByte = bytes(index)
    var numBytes = 1
    var sum: Int = readByte & 0x7f
    while((readByte & 0x80) == 0x80) {
      index += 1
      numBytes += 1
      readByte = bytes(index)
      sum = (sum << 7) | (readByte & 0x7f)
    }
    (sum, numBytes)
  }

  def readIntFromStream(input: ByteArrayInputStream) : Int = {
    var readByte = input.read().asInstanceOf[Byte]
    var sum : Int = readByte & 0x7f //the int that is the read byte, ignoring the first bit.
    while((readByte & 0x80) == 0x80) { //first bit is 1
      readByte = input.read().asInstanceOf[Byte]
      sum = (sum << 7) | (readByte & 0x7f) //add the next byte
    } //todo watch for too big numbers
    sum
  }

  def readIntFromStream(input: PrimitiveInputStream) : Int = {
    var readByte = input.readByte()
    var sum : Int = readByte & 0x7f //the int that is the read byte, ignoring the first bit.
    while((readByte & 0x80) == 0x80) { //first bit is 1
      readByte = input.readByte()
      sum = (sum << 7) | (readByte & 0x7f) //add the next byte
    } //todo watch for too big numbers
    sum
  }

  def readLongFromStream(input: PrimitiveInputStream) : Long = {
    var readByte = input.readByte()
    var sum : Long = readByte & 0x7f //the int that is the read byte, ignoring the first bit.
    while((readByte & 0x80) == 0x80) { //first bit is 1
      readByte = input.readByte()
      sum = (sum << 7) | (readByte & 0x7f) //add the next byte
    }
    sum
  }

  def toHexString(bytes :Array[Byte]) : String = {
    bytes.map("%02x".format(_)) mkString("[", ", ", "]")
  }
}
