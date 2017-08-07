package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.hbase.schema.{PrimitiveInputStream, PrimitiveOutputStream, ComplexByteConverter}
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters.ScopedKeyConverter

object ScopedMetricsConverters {

  implicit object ScopedToMetricsKeyConverter extends ComplexByteConverter[ScopedToMetricsKey]  {

    val version = 1
    override def write(data: ScopedToMetricsKey, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeObj(data.to)(ScopedKeyConverter)
      output.writeLong(- data.dateHourMs)
      output.writeLong(data.bucketId)
      output.writeShort(data.placementId.toShort)
      output.writeInt(data.algoId)
      output.writeByte(data.countBy)

    }

    override def read(input: PrimitiveInputStream): ScopedToMetricsKey = {
      val version = input.readByte()
      ScopedToMetricsKey(
        to = input.readObj[ScopedKey],
        dateHourMs = - input.readLong(),
        bucketId = input.readInt,
        placementId = input.readShort().toInt,
        algoId = input.readInt,
        countBy = input.readByte()
      )
    }
  }


  implicit object ScopedMetricsKeyConverter extends ComplexByteConverter[ScopedMetricsKey]  {

    val version = 3
    override def write(data: ScopedMetricsKey, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeLong(- data.dateTimeMs)
      output.writeByte(data.countBy)

    }

    override def read(input: PrimitiveInputStream): ScopedMetricsKey = {
      val version = input.readByte()

      version match {
        case 1 => {
          val dateHourMs = - input.readLong()
          val bucketIdNoLongerUsed = input.readInt()
          val placementId = input.readShort().toInt
          val algoIdNoLongerUsed = input.readInt()
          val countBy = input.readByte()
          ScopedMetricsKey(
            dateTimeMs = dateHourMs,
            countBy = countBy
          )
        }
        case 2 => {
          //throw new Exception("ignore version 2 of scoped metrics")
          val dateHourMs = - input.readLong()
          val sitePlacementId = input.readShort().toInt
          val countBy = input.readByte()

          ScopedMetricsKey(
            dateHourMs,
            countBy
          )
        }
        case 3 => {
          ScopedMetricsKey(
            dateTimeMs = - input.readLong(),
            countBy = input.readByte()
          )
        }
      }
    }
  }

}
