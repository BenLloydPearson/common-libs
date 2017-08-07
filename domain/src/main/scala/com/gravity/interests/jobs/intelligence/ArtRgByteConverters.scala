package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.operations.{ArtRgSubject, CRRgNodeType, ArtRgEntity}
import com.gravity.interests.jobs.intelligence.schemas.byteconverters.UnsupportedVersionExcepion

import scala.collection._


trait ArtRgByteConverters {

  implicit object ArtRgEntityConverter extends ComplexByteConverter[ArtRgEntity] {
    val writingVer     = 1
    val minReadableVer = 1
    val maxReadableVer = 1

    implicit object StringIntMapConverter extends MapConverter[String, Int]

    override def write(data: ArtRgEntity, output: PrimitiveOutputStream): Unit = {
      output.writeByte(writingVer)
      output.writeUTF(data.name)
      output.writeLong(data.id)
      output.writeDouble(data.score)

      // data.disambiguator -- Option[String]
      data.disambiguator match {
        case Some(str) =>
          output.writeBoolean(true)
          output.writeUTF(str)

        case None =>
          output.writeBoolean(false)
      }

      output.writeObj(scala.collection.Map(data.hitCounts.toSeq:_*))
      output.writeBoolean(data.in_headline)

      // data.node_types -- Seq[(String, Int)]
      output.writeInt(data.node_types.size)
      data.node_types.foreach { node_type =>
        output.writeUTF(node_type.name)
        output.writeInt(node_type.id)
      }
    }

    override def read(input: PrimitiveInputStream): ArtRgEntity = {
      def readNodeTypes: Seq[CRRgNodeType] = (for (idx <- 1 to input.readInt) yield CRRgNodeType(input.readUTF, input.readInt)).toSeq

      input.readByte() match {
        case gotVersion if gotVersion >= minReadableVer && gotVersion <= maxReadableVer =>
          ArtRgEntity(
            input.readUTF,
            input.readLong,
            input.readDouble,
            if (input.readBoolean) Some(input.readUTF) else None,
            input.readObj[scala.collection.Map[String, Int]].toMap,
            input.readBoolean,
            readNodeTypes
          )

        case unsupportedVer =>
          throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ArtRgEntity", unsupportedVer, minReadableVer, maxReadableVer))
      }
    }
  }

  implicit object ArtRgSubjectConverter extends ComplexByteConverter[ArtRgSubject] {
    val writingVer     = 1
    val minReadableVer = 1
    val maxReadableVer = 1

    override def write(data: ArtRgSubject, output: PrimitiveOutputStream): Unit = {
      output.writeByte(writingVer)
      output.writeUTF(data.name)
      output.writeLong(data.id)
      output.writeDouble(data.score)

      // data.disambiguator -- Option[String]
      data.disambiguator match {
        case Some(str) =>
          output.writeBoolean(true)
          output.writeUTF(str)

        case None =>
          output.writeBoolean(false)
      }

      output.writeBoolean(data.most_granular)
    }

    override def read(input: PrimitiveInputStream): ArtRgSubject = {
      input.readByte() match {
        case gotVersion if gotVersion >= minReadableVer && gotVersion <= maxReadableVer =>
          ArtRgSubject(
            input.readUTF,
            input.readLong,
            input.readDouble,
            if (input.readBoolean) Some(input.readUTF) else None,
            input.readBoolean
          )

        case unsupportedVer =>
          throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ArtRgSubject", unsupportedVer, minReadableVer, maxReadableVer))
      }
    }
  }
}
