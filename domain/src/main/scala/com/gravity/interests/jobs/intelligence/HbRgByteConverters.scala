package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.operations.HbRgTagRef
import com.gravity.interests.jobs.intelligence.schemas.byteconverters.UnsupportedVersionExcepion

trait HbRgByteConverters {
  implicit object HbRgTagRefConverter extends ComplexByteConverter[HbRgTagRef] {
    val writingVer     = 1
    val minReadableVer = 1
    val maxReadableVer = 1

    override def write(data: HbRgTagRef, output: PrimitiveOutputStream): Unit = {
      output.writeByte(writingVer)
      output.writeUTF(data.name)
      output.writeDouble(data.score)
    }

    override def read(input: PrimitiveInputStream): HbRgTagRef = {
      input.readByte() match {
        case gotVersion if gotVersion >= minReadableVer && gotVersion <= maxReadableVer =>
          HbRgTagRef(
            input.readUTF,
            input.readDouble
          )

        case unsupportedVer =>
          throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("HbRgTagRef", unsupportedVer, minReadableVer, maxReadableVer))
      }
    }
  }
}
