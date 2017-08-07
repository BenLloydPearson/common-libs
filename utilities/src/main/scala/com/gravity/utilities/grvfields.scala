package com.gravity.utilities

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import cascading.tuple.Fields
import com.gravity.hbase.schema.{PrimitiveInputStream, PrimitiveOutputStream}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.DecoderFactory

import scala.collection.{mutable, _}
import scalaz.ValidationNel
import scalaz.syntax.validation._

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/16/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object grvfields {
  def toDelimitedFieldString[T](o: T)(implicit ev: FieldConverter[T]): String = ev.toDelimitedFieldString(o)
  def toValueRegistry[T](o:T)(implicit ev: FieldConverter[T]): FieldValueRegistry = ev.toValueRegistry(o)
  def toBytes[T](o: T)(implicit ev: FieldConverter[T]): Array[Byte] = ev.toBytes(o)
  def toAvroSchema[T](implicit ev: FieldConverter[T]): Schema = ev.avroSchema
  def toAvroRecord[T](o: T)(implicit ev: FieldConverter[T]): Record = ev.toAvroRecord(o)

  def seqToBytes[T](o:Seq[T])(implicit ev:FieldConverter[T]) : Array[Byte] = ev.seqToBytes(o)

  def getSequenceFromBytes[T](bytes:Array[Byte])(implicit ev:FieldConverter[T]) : Seq[T] = ev.getInstancesFromBytes(bytes)

  implicit class fieldConverterImplicit[T](val o: T) extends AnyVal {
    def toDelimitedFieldString()(implicit ev: FieldConverter[T]): String = grvfields.toDelimitedFieldString(o)
    def toValueRegistry()(implicit ev: FieldConverter[T]): FieldValueRegistry = grvfields.toValueRegistry(o)
    def toBytes()(implicit ev: FieldConverter[T]): Array[Byte] = grvfields.toBytes(o)
    def toAvroRecord()(implicit ev: FieldConverter[T]): Record = grvfields.toAvroRecord(o)
  }

  def getInstanceFromString[T](line:String)(implicit ev: FieldConverter[T]) : ValidationNel[FailureResult, T] =
    if(line.startsWith(ev.getCategoryName)) FieldValueRegistry.getInstanceFromString[T](line)(ev)
    else FailureResult("Line is not " + ev.getCategoryName + " : " + line).failureNel

  def getInstanceFromJson[T](json: String)(implicit tFieldConverter: FieldConverter[T]): ValidationNel[FailureResult, T] = {
    val schema = tFieldConverter.avroSchema
    val decoder = DecoderFactory.get().jsonDecoder(schema, json)
    val reader = new GenericDatumReader[GenericData.Record](schema)
    val record = reader.read(null, decoder)
    getInstanceFromAvroRecord[T](record)
  }

  def getInstanceFromBytes[T](bytes:Array[Byte])(implicit ev: FieldConverter[T]) : ValidationNel[FailureResult, T] =
    FieldValueRegistry.getInstanceFromBytes[T](bytes)(ev)

  def getInstanceFromAvroRecord[T](record: GenericData.Record)(implicit ev: FieldConverter[T]): ValidationNel[FailureResult, T] =
    FieldValueRegistry.getInstanceFromAvroRecord[T](record)(ev)

  def getInstanceFromSparkRow[T](row: org.apache.spark.sql.Row)(implicit ev: FieldConverter[T]): ValidationNel[FailureResult, T] =
    FieldValueRegistry.getInstanceFromSparkRow[T](row)(ev)


  trait FieldConverter[T] extends Serializable {
    val compressBytes : Boolean = false
    def toValueRegistry(o: T) : FieldValueRegistry
    def toValueRegistryDangerous(o: Any): FieldValueRegistry = toValueRegistry(o.asInstanceOf[T]) //I hate you, type erasure. This was the only way I could figure out how to support EventViewerPage
    def toDelimitedFieldString(o: T): String = toValueRegistry(o).toDelimitedFieldString
    def toDelimitedFieldStringDangerous(o: Any): String = toValueRegistry(o.asInstanceOf[T]).toDelimitedFieldString //god-DAMMIT type erasure
    def toBytes(o:T): Array[Byte] = toValueRegistry(o).toBytes(compressBytes)

    def avroSchema: Schema = fields.avroSchema

    def hiveColumnSchema(sb: StringBuilder = new StringBuilder, asStruct: Boolean = false): StringBuilder = {
      val typeDelim = if (asStruct) {
        sb.append("`categoryName` : STRING, `version` : INT")
        " : "
      }
      else {
        sb.append("`categoryName` STRING, `version` INT")
        " "
      }

      fields.sortedFields.foreach { field =>
        sb.append(", ")

        sb.append('`').append(field.name).append('`').append(typeDelim).append(field.serializer.hiveTypeName)
      }

      sb
    }

    def hiveCreateTableScript(tableName: String, hdfsLocation: String): String = {
      val sb: StringBuilder = new StringBuilder
      sb.append("create external table ").append(tableName).append('\n')
      sb.append('(').append('\n')

      hiveColumnSchema(sb)

      sb.append('\n').append(')').append('\n')
      sb.append("PARTITIONED BY (ds string)").append('\n')
      sb.append(" STORED AS parquet").append('\n')
      sb.append("location '").append(hdfsLocation).append(''').append('\n')
      sb.append("TBLPROPERTIES('serialization.null.format'='')").append('\n')

      sb.toString()
    }

    def toAvroRecord(o:T ): Record = toValueRegistry(o).toAvroRecord
    def toAvroRecordDangerous(o: Any ): Record = toValueRegistry(o.asInstanceOf[T]).toAvroRecord //god-DAMMIT type erasure

    def cascadingFields: Fields = fields.cascadingFields

    def appendDelimitedFieldString(o: T, sb: StringBuilder): StringBuilder = toValueRegistry(o).appendDelimitedFieldString(sb)

    def fromValueRegistry(reg: FieldValueRegistry) : T
    val fields : FieldRegistry[T]
    def getCategoryName: String = fields.getCategoryName
    def serializationVersion: Int = fields.getVersion
    def getInstanceFromString(line: String): ValidationNel[FailureResult, T] = grvfields.getInstanceFromString(line)(this)
    def getInstanceFromBytes(bytes: Array[Byte]): ValidationNel[FailureResult, T] = grvfields.getInstanceFromBytes(bytes)(this)
    def getInstanceFromAvroRecord(record: GenericData.Record): ValidationNel[FailureResult, T] = grvfields.getInstanceFromAvroRecord(record)(this)
    def getInstanceFromSparkRow(row: org.apache.spark.sql.Row): ValidationNel[FailureResult, T] = grvfields.getInstanceFromSparkRow(row)(this)

    //Byte sequence handling
    def seqToBytes(oseq:Seq[T])(implicit fc: FieldConverter[T]) : Array[Byte] = {
      val bos = new ByteArrayOutputStream()
      val pos = new PrimitiveOutputStream(bos)
      pos.writeInt(oseq.length)
      for{
        o <- oseq
      } {
        val bytes = grvfields.toBytes(o)(fc)
        pos.writeInt(bytes.length)
        pos.write(bytes)
      }
      bos.toByteArray
    }

    def getInstancesFromBytes(bytes:Array[Byte])(implicit fc:FieldConverter[T]) : Seq[T] = {
      val bis = new ByteArrayInputStream(bytes)
      val input = new PrimitiveInputStream(bis)

      val length = input.readInt()

      val buffer = mutable.Buffer[T]()

      for(i <- 0 until length) {
        val byteLength = input.readInt()
        val byteResult = new Array[Byte](byteLength)
        input.readFully(byteResult)
        val res = grvfields.getInstanceFromBytes(byteResult)

        buffer += res.toOption.get
      }

      buffer
    }
  }

  trait OptionalCategoryVersionLogLineConverter[T] extends FieldConverter[T] {
    def getDelimiter: String = FieldValueRegistry.FIELD_DELIM

    def parse(line: String): ValidationNel[FailureResult, T] = {
      // Currently the FieldValueRegistry expects log lines to have the category name and version as the first 2.
      // Until data is written into RDS using this same framework, this is being hijacked here
      val category = getCategoryName
      val delim = getDelimiter
      val version = serializationVersion

      val withCategoryAndVersionPrefix = category + delim + version

      val lineToRead = if (line.startsWith(withCategoryAndVersionPrefix)) line else withCategoryAndVersionPrefix + delim + line

      getInstanceFromString(lineToRead)
    }
  }

}
