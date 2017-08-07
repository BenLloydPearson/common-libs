package com.gravity.interests.jobs.intelligence.schemas.byteconverters

import com.gravity.hbase.schema.{ComplexByteConverter, PrimitiveInputStream, PrimitiveOutputStream}
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import org.joda.time.DateTime

trait AuditLogByteConverters {
  this: SchemaTypes.type =>

  implicit object AuditEventsConverter extends ComplexByteConverter[AuditEvents.Type] {
    def write(data: AuditEvents.Type, output: PrimitiveOutputStream) {
      output.writeShort(data.id)
    }

    def read(input: PrimitiveInputStream): AuditEvents.Type = AuditEvents.parseOrDefault(input.readShort())
  }

  implicit object AuditStatusConverter extends ComplexByteConverter[AuditStatus.Type] {
    def write(data: AuditStatus.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): AuditStatus.Type = AuditStatus.parseOrDefault(input.readByte())
  }

  implicit object AuditKey2Converter extends ComplexByteConverter[AuditKey2] {

    override def write(data: AuditKey2, output: PrimitiveOutputStream) {
      output.writeObj(data.scopedKey)
      output.writeLong(-data.dateTime.getMillis)
      output.writeLong(data.userId)
      output.writeLong(data.auditId)
    }

    override def read(input: PrimitiveInputStream): AuditKey2 = AuditKey2(input.readObj[ScopedKey], new DateTime(-input.readLong()), input.readLong(), input.readLong())
  }

  implicit object AuditMessageConverter extends ComplexByteConverter[AuditMessage] {
    def write(data: AuditMessage, output: PrimitiveOutputStream) {
      output.writeUTF(data.userName)
      output.writeObj(data.eventType)
      output.writeUTF(data.oldValueString)
      output.writeUTF(data.newValueString)
    }

    def read(input: PrimitiveInputStream): AuditMessage = AuditMessage(input.readUTF(), input.readObj[AuditEvents.Type], input.readUTF(), input.readUTF())
  }

  implicit object CampaignStatusChangeConverter extends ComplexByteConverter[CampaignStatusChange] {
    def write(data: CampaignStatusChange, output: PrimitiveOutputStream) {
      output.writeLong(data.userId)
      output.writeObj(data.changedFromStatus)
      output.writeObj(data.changedToStatus)
    }

    def read(input: PrimitiveInputStream): CampaignStatusChange = {
      CampaignStatusChange(
        input.readLong(),
        input.readObj[CampaignStatus.Type],
        input.readObj[CampaignStatus.Type]
      )
    }
  }
}
