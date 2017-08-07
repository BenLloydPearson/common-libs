package com.gravity.interests.jobs.intelligence.schemas.byteconverters

import com.gravity.domain.aol.{AolLink}
import com.gravity.domain.gms.GmsArticleStatus
import com.gravity.hbase.schema.{SeqConverter, PrimitiveInputStream, PrimitiveOutputStream, ComplexByteConverter}
import com.gravity.interests.jobs.intelligence.SchemaTypes

trait AolByteConverters {
  this: SchemaTypes.type =>

  implicit object AolLinkConverter extends ComplexByteConverter[AolLink] {

    def write(data: AolLink, output: PrimitiveOutputStream) {
      output.writeUTF(data.url)
      output.writeUTF(data.text)
      output.writeBoolean(data.showVideoIcon)
    }

    def read(input: PrimitiveInputStream): AolLink = {
      AolLink(input.readUTF(), input.readUTF(), input.readBoolean())
    }
  }

  implicit object AolLinkSeqConverter extends SeqConverter[AolLink]

  implicit object GmsArticleStatusConverter extends ComplexByteConverter[GmsArticleStatus.Type] {
    def write(data: GmsArticleStatus.Type, output: PrimitiveOutputStream): Unit = {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): GmsArticleStatus.Type = {
      GmsArticleStatus.get(input.readByte()).getOrElse(GmsArticleStatus.defaultValue)
    }
  }
}
