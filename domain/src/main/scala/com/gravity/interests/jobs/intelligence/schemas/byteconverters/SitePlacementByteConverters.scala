package com.gravity.interests.jobs.intelligence.schemas.byteconverters

import com.gravity.hbase.schema.{ComplexByteConverter, PrimitiveInputStream, PrimitiveOutputStream}
import com.gravity.interests.jobs.intelligence._
import com.gravity.valueclasses.ValueClassesForDomain._

trait SitePlacementByteConverters {
  this: SchemaTypes.type =>

  implicit object SitePlacementBucketKeyConverter extends ComplexByteConverter[SitePlacementBucketKey] {
    override def write(key: SitePlacementBucketKey, output: PrimitiveOutputStream) {
      output.writeInt(key.bucketId)
      output.writeLong(key.placement)
      output.writeLong(key.siteId)
    }

    override def read(input: PrimitiveInputStream): SitePlacementBucketKey = {
      val bucket = input.readInt()
      val placement = input.readLong()
      val siteId = input.readLong()
      SitePlacementBucketKey(bucket, placement, siteId)
    }
  }

  implicit object SitePlacementBucketSlotKeyConverter extends ComplexByteConverter[SitePlacementBucketSlotKey] {
    override def write(key: SitePlacementBucketSlotKey, output: PrimitiveOutputStream) {
      output.writeInt(key.slotIndex)
      output.writeInt(key.bucketId)
      output.writeLong(key.placement)
      output.writeLong(key.siteId)
    }

    override def read(input: PrimitiveInputStream): SitePlacementBucketSlotKey = {
      val slotIndex = input.readInt()
      val bucket = input.readInt()
      val placement = input.readLong()
      val siteId = input.readLong()
      SitePlacementBucketSlotKey(slotIndex, bucket, placement, siteId)
    }
  }

  implicit object SitePlacementKeyConverter extends ComplexByteConverter[SitePlacementKey] {
    override def write(key: SitePlacementKey, output: PrimitiveOutputStream) {
      output.writeLong(key.placementIdOrSitePlacementId)
      output.writeLong(key.siteId)
    }

    override def read(input: PrimitiveInputStream): SitePlacementKey = {
      val placement = input.readLong()
      val siteId = input.readLong()
      SitePlacementKey(placement, siteId)
    }
  }

  implicit object SitePlacementIdKeyConverter extends ComplexByteConverter[SitePlacementIdKey] {
    override def write(data: SitePlacementIdKey, output: PrimitiveOutputStream): Unit = {
      output.writeLong(data.id.raw)
    }

    override def read(input: PrimitiveInputStream): SitePlacementIdKey = {
      val id = input.readLong()
      SitePlacementIdKey(SitePlacementId(id))
    }
  }

  implicit object SitePlacementIdBucketKeyConverter extends ComplexByteConverter[SitePlacementIdBucketKey] {
    override def write(data: SitePlacementIdBucketKey, output: PrimitiveOutputStream): Unit = {
      output.writeLong(data.sitePlacementId.raw)
      output.writeInt(data.bucketId.raw)
    }

    override def read(input: PrimitiveInputStream): SitePlacementIdBucketKey = {
      val spid = input.readLong()
      val bucket = input.readInt()
      SitePlacementIdBucketKey(spid.asSitePlacementId, bucket.asBucketId)
    }
  }

  implicit object SitePlacementWhyKeyConverter extends ComplexByteConverter[SitePlacementWhyKey] {
    val version = 1

    override def write(key: SitePlacementWhyKey, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeLong(key.placement)
      output.writeLong(key.siteId)
      output.writeUTF(key.why)
    }

    override def read(input: PrimitiveInputStream): SitePlacementWhyKey = {

      input.readByte() match {
        case currentOrNewer if currentOrNewer >= version => {
          val placement = input.readLong()
          val siteId = input.readLong()
          val why = input.readUTF()
          SitePlacementWhyKey(why, placement, siteId)
        }
        case unsupported =>
          throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("SitePlacementWhyKey", unsupported.toInt, version))
      }
    }
  }
}