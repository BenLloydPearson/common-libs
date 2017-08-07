package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{PrimitiveOutputStream, ByteConverter}

/**
 * User: mtrelinski
 * Date: 12/26/12
 * Time: 6:47 PM
 */

object BytesConverterLoader {
  implicit lazy val converter: BytesConverter = new BytesConverter
}

class BytesConverter extends ByteConverter[Array[Byte]] {

  override def toBytes(t: Array[Byte]): Array[Byte] = t

  def write(data: Array[Byte], output: PrimitiveOutputStream): Unit = output.write(data)

  override def fromBytes(bytes: Array[Byte], offset: Int, length: Int): Array[Byte] = bytes.drop(offset).take(length)

  override def fromBytes(bytes: Array[Byte]): Array[Byte] = bytes

}
