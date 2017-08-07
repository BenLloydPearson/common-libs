package com.gravity.interests.jobs.intelligence.schemas.byteconverters

import java.net.{URI, URL}

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.SchemaTypes
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.analytics.{DateHourRange, DateMidnightRange, IncExcUrlStrings}
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}
import org.joda.time.DateTime

trait GenericByteConverters {
  this: SchemaTypes.type =>

  implicit object ByteConverter extends ComplexByteConverter[Byte] {
    def write(data: Byte, output: PrimitiveOutputStream) {
      output.writeByte(data)
    }

    def read(input: PrimitiveInputStream): Byte = input.readByte()
  }

  implicit object ByteArrayConverter extends ComplexByteConverter[Array[Byte]] {
    def write(data: Array[Byte], output: PrimitiveOutputStream) {
      output.writeInt(data.length)
      output.write(data)
    }

    def read(input: PrimitiveInputStream): Array[Byte] = {
      val len = input.readInt()
      val arr = new Array[Byte](len)
      input.read(arr)
      arr
    }
  }

  implicit object OptionStringConverter extends ComplexByteConverter[Option[String]] {
    def write(data: Option[String], output: PrimitiveOutputStream) {
      data match {
        case Some(s) =>
          output.writeBoolean(true)
          output.writeUTF(s)
        case None => output.writeBoolean(false)
      }
    }

    def read(input: PrimitiveInputStream): Option[String] = {
      if (input.readBoolean()) Some(input.readUTF()) else None
    }
  }

  implicit object DateHourConverter extends ComplexByteConverter[DateHour] {
    override def write(date: DateHour, output: PrimitiveOutputStream) {
      output.writeLong(date.getMillis)
    }

    override def read(input: PrimitiveInputStream): DateHour = DateHour(new DateTime(input.readLong()))
  }

  implicit object DateHourRangeConverter extends ComplexByteConverter[DateHourRange] {
    override def write(data: DateHourRange, output: PrimitiveOutputStream) {
      output.writeObj(data.fromHour)
      output.writeObj(data.toHour)
    }

    override def read(input: PrimitiveInputStream): DateHourRange = {
      DateHourRange(input.readObj[DateHour], input.readObj[DateHour])
    }
  }

  implicit object DateMidnightRangeConverter extends ComplexByteConverter[DateMidnightRange] {
    override def write(data: DateMidnightRange, output: PrimitiveOutputStream) {
      output.writeObj(data.fromInclusive)
      output.writeObj(data.toInclusive)
    }

    override def read(input: PrimitiveInputStream): DateMidnightRange = {
      DateMidnightRange(input.readObj[GrvDateMidnight], input.readObj[GrvDateMidnight])
    }
  }

  implicit object URLConverter extends ComplexByteConverter[URL] {
    def write(data: URL, output: PrimitiveOutputStream) {
      output.writeUTF(data.toString)
    }

    def read(input: PrimitiveInputStream): URL = new URL(input.readUTF())
  }

  implicit object URIConverter extends ComplexByteConverter[URI] {
    def write(data: URI, output: PrimitiveOutputStream) {
      output.writeUTF(data.toString)
    }

    def read(input: PrimitiveInputStream): URI = new URI(input.readUTF())
  }

  implicit object UriSet extends SetConverter[URI]

  implicit object ScopedKeySetConverter extends SetConverter[ScopedKey]

  implicit object IncExcUrlStringsConverter extends ComplexByteConverter[IncExcUrlStrings] {
    val version = 1

    def write(data: IncExcUrlStrings, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeObj(data.includeAuths)
      output.writeObj(data.includePaths)
      output.writeObj(data.excludeAuths)
      output.writeObj(data.excludePaths)
    }

    def read(input: PrimitiveInputStream): IncExcUrlStrings = {
      input.readByte() match {
        case currentOrNewer if currentOrNewer >= version => {
          IncExcUrlStrings(input.readObj(StringSeqConverter), input.readObj(StringSeqConverter), input.readObj(StringSeqConverter), input.readObj(StringSeqConverter))
        }
        case unsupported => throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("IncExcUrlStrings", unsupported.toInt, version))
      }
    }
  }

  implicit object IncExcUrlStringsSeqConverter extends SeqConverter[IncExcUrlStrings]

  implicit object DollarValueConverter extends ComplexByteConverter[DollarValue] {
    override def write(data: DollarValue, output: PrimitiveOutputStream) {
      output.writeLong(data.pennies)
    }

    override def read(input: PrimitiveInputStream): DollarValue = DollarValue(input.readLong)
  }
}
