package com.gravity.utilities.eventlogging

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.gravity.hbase.schema.{PrimitiveInputStream, PrimitiveOutputStream}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.{ArchiveUtils, VariableLengthEncoding}
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}
import org.joda.time.DateTime

import scala.collection.mutable
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 3/31/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object FieldValueRegistry {
 import com.gravity.logging.Logging._
  val FIELD_DELIM = "^"
  val LIST_DELIM = "|"
  val encoding = "UTF-8"

  def liftBetter(arr: Array[String], index: Int): Option[String] = {
    if (index < arr.length) Some(arr(index)) else None
  }

  private def fromTokens(tokens: Array[String], origString: String, fieldDelimiter: String, listDelimiter: String, fields: FieldRegistry[_]): ValidationNel[FailureResult, FieldValueRegistry] = {

    val fails = mutable.Buffer[FailureResult]()

    val tokensLength = tokens.length

    if(tokensLength < 2) {
      return FailureResult("String did not have enough tokens to be field serialized: " + origString).failureNel
    }

    val version = tokens(1).tryToInt.getOrElse(0)
    val fieldValues = new FieldValueRegistry(fields, version, fieldDelimiter, listDelimiter)
    fields.getFields.foreach(field => {
      val tokenIndex = field.index + 2 //+2 is for the name and version.

      if (tokenIndex < tokensLength) {
        field.insertValue(fieldValues, tokens(tokenIndex)) match {
          case Success(_) =>
          case Failure(newFails) =>
            newFails.foreach(fails += _)
        }
      }
      else {
        if (field.required) {
          trace {
            val indexLabeledTokens = tokens.zipWithIndex.map{case (tok: String, idx: Int) => s"$idx:`$tok`"}
            s"Could not get required field `${field.name}` from index `${field.index}` of string `$origString` and tokens: ${indexLabeledTokens.mkString(", ")}"
          }
          fails += FailureResult(s"Could not get required field `${field.name}` from index `${field.index}` of string `$origString`")
        }
        else {
          field.insertDefaultValue(fieldValues)
        }
      }
    })

    fails.toNel match {
      case Some(failsNel) => failsNel.failure
      case None => fieldValues.successNel
    }
  }

  def fromString[T](line: String, fieldDelimiter : String = FIELD_DELIM, listDelimiter : String = LIST_DELIM)
                                  (implicit ev: FieldConverter[T]): ValidationNel[FailureResult, FieldValueRegistry] = {
    fromTokens(tokenize(line, fieldDelimiter), line, fieldDelimiter, listDelimiter, ev.fields)
  }

  def fromPrimitiveInputStream[T](input: PrimitiveInputStream)(implicit ev: FieldConverter[T]): ValidationNel[FailureResult, FieldValueRegistry] = {

    //Improvement: change this to a proper header. use 7 bit lengths. track consumed length for backwards/forwards compatibility and error recovery
    val length = VariableLengthEncoding.readIntFromStream(input) //dummy for length
    val version = VariableLengthEncoding.readIntFromStream(input)
    val flags = input.readByte() //dummy for flags
    val fieldValues = new FieldValueRegistry(ev.fields, version)
    for(index <- 0 to ev.fields.getMaxIndex) {
      ev.fields.getFieldOption(index).foreach(field => {
        field.insertValue(fieldValues, input, version)
      })
    }
    fieldValues.successNel
  }

  def fromAvroRecord[T](record: GenericData.Record)(implicit ev: FieldConverter[T]): ValidationNel[FailureResult, FieldValueRegistry] = {
    try {
      val version = record.get("version").asInstanceOf[Int]
      val fieldValues = new FieldValueRegistry(ev.fields, version)
      for (index <- 0 to ev.fields.getMaxIndex) {
        ev.fields.getFieldOption(index).foreach(field => field.insertValueFromAvro(fieldValues, record, version))
      }
      fieldValues.successNel
    }
    catch {
      case e: Exception => FailureResult("Failed to read avro record in category " + ev.getCategoryName, e).failureNel
    }
  }

  def fromSparkRow[T](row: org.apache.spark.sql.Row)(implicit ev: FieldConverter[T]): ValidationNel[FailureResult, FieldValueRegistry] = {
    try {
      val data = row.getValuesMap(ev.fields.sortedFields.map(_.name) ++ List("version", "categoryName"))
      val version = data("version").asInstanceOf[Int]
      val fieldValues = new FieldValueRegistry(ev.fields, version)
      for (index <- 0 to ev.fields.getMaxIndex) {
        ev.fields.getFieldOption(index).foreach(field => field.insertValueFromSparkRow(fieldValues, row, version))
      }
      fieldValues.successNel
    }
    catch {
      case e: Exception => FailureResult("Failed to read spark row in category " + ev.getCategoryName, e).failureNel
    }
  }

  def getInstanceFromString[T](line:String)(implicit ev: FieldConverter[T]) : ValidationNel[FailureResult, T] = {
    fromString(line)(ev) match {
      case Success(thing) => ev.fromValueRegistry(thing).success
      case Failure(fails) => fails.failure
    }
  }

  def getInstanceFromPrimitiveInputStream[T](input: PrimitiveInputStream)(implicit ev: FieldConverter[T]): ValidationNel[FailureResult,T] = {
    fromPrimitiveInputStream(input)(ev) match {
      case Success(thing) => ev.fromValueRegistry(thing).success
      case Failure(fails) => fails.failure
    }
  }

  def getInstanceFromBytes[T](bytes:Array[Byte])(implicit ev: FieldConverter[T]) : ValidationNel[FailureResult, T] = {
    val bis = if(ev.compressBytes)
      new ByteArrayInputStream(ArchiveUtils.decompressBytes(bytes, withHeader = true))
    else
      new ByteArrayInputStream(bytes)

    val input = new PrimitiveInputStream(bis)

    getInstanceFromPrimitiveInputStream(input)(ev)
  }

  def getInstanceFromAvroRecord[T](record: GenericData.Record)(implicit ev: FieldConverter[T]) : ValidationNel[FailureResult, T] = {
    fromAvroRecord(record)(ev) match {
      case Success(thing) => ev.fromValueRegistry(thing).success
      case Failure(fails) => fails.failure
    }
  }

  def getInstanceFromSparkRow[T](row: org.apache.spark.sql.Row)(implicit ev: FieldConverter[T]) : ValidationNel[FailureResult, T] = {
    fromSparkRow(row)(ev) match {
      case Success(thing) => ev.fromValueRegistry(thing).success
      case Failure(fails) => fails.failure
    }
  }
}

class FieldValueRegistry(val fieldRegistry: FieldRegistry[_], val version: Int = 0, fieldDelimiter: String = FieldValueRegistry.FIELD_DELIM, listDelimiter : String = FieldValueRegistry.LIST_DELIM) {
  private val fieldValues = new scala.collection.mutable.HashMap[Int, SerializationFieldValue[_]]()
  private lazy val fieldNameIndexes = fieldRegistry.buildFieldNameIndexes

  def getFieldDelimiter: String = fieldDelimiter
  def getListDelimiter: String = listDelimiter
  def getCategoryName: String = fieldRegistry.getCategoryName

  //def create: FieldWriter = fieldRegistry.create(this)

  def registerFieldValue(index:Int, value: DateTime) : FieldValueRegistry = {
    registerFieldValue[DateTime](SerializationFieldValue(fieldRegistry.getTypedField[DateTime](index), value))
    this
  }

  def registerFieldValue(index:Int, value: Int) : FieldValueRegistry = {
    registerFieldValue[Int](SerializationFieldValue(fieldRegistry.getTypedField[Int](index), value))
    this
  }

  def registerFieldValue(index:Int, value: Long) : FieldValueRegistry = {
    registerFieldValue[Long](SerializationFieldValue(fieldRegistry.getTypedField[Long](index), value))
    this
  }

  def registerFieldValue(index:Int, value: Boolean) : FieldValueRegistry= {
    registerFieldValue[Boolean](SerializationFieldValue(fieldRegistry.getTypedField[Boolean](index), value))
    this
  }

  def registerFieldValue(index:Int, value: Float) : FieldValueRegistry = {
    registerFieldValue[Float](SerializationFieldValue(fieldRegistry.getTypedField[Float](index), value))
    this
  }

  def registerFieldValue(index:Int, value: Double) : FieldValueRegistry = {
    registerFieldValue[Double](SerializationFieldValue(fieldRegistry.getTypedField[Double](index), value))
    this
  }

  def registerFieldValue(index: Int, value: String) : FieldValueRegistry = {
    registerFieldValue[String](SerializationFieldValue(fieldRegistry.getTypedField[String](index), value))
    this
  }

  def registerFieldValue(index: Int, value: Option[String]) : FieldValueRegistry = {
    registerFieldValue[Option[String]](SerializationFieldValue(fieldRegistry.getTypedField[Option[String]](index), value))
    this
  }

  def registerFieldValue(index: Int, value: Array[Byte]) : FieldValueRegistry = {
    registerFieldValue[Array[Byte]](SerializationFieldValue(fieldRegistry.getTypedField[Array[Byte]](index), value))
    this
  }

  def registerFieldValue[T](index: Int, value: T)(implicit m: Manifest[T], ev: FieldConverter[T]) : FieldValueRegistry = {
    registerFieldValue[T](SerializationFieldValue(fieldRegistry.getTypedField[T](index), value))
    this
  }

  //i hate you, type erasure.
  def registerFieldValueDangerous[T](index: Int, value: Any)(implicit m: Manifest[T], ev: FieldConverter[T]) : FieldValueRegistry = {
    registerFieldValue[T](SerializationFieldValue(fieldRegistry.getTypedField[T](index), value.asInstanceOf[T]))
    this
  }

  def registerFieldValue[T](index: Int, value: Seq[T])(implicit m: Manifest[T]): FieldValueRegistry = {
   registerFieldValue[Seq[T]](SerializationFieldValue(fieldRegistry.getTypedField[Seq[T]](index), value))
   this
  }

  def registerFieldValue[T](index: Int, value: Option[T])(implicit m: Manifest[T]): FieldValueRegistry = {
    registerFieldValue[Option[T]](SerializationFieldValue(fieldRegistry.getTypedField[Option[T]](index), value))
    this
  }

  //i hate you, type erasure.
  def registerFieldListValueDangerous[T](index: Int, value: Any)(implicit m: Manifest[T], ev: FieldConverter[T]) : FieldValueRegistry = {
    registerFieldValue[Seq[T]](SerializationFieldValue(fieldRegistry.getTypedField[Seq[T]](index), value.asInstanceOf[Seq[T]]))
    this
  }

  /**
   * Preemptively removes all substrings that would be disallowed by this FieldValueRegistry, to avoid
   * emitting warnings later.
   */
  def cleanString(value: String): String = {
    value
      .replace(fieldDelimiter, "")
      .replace(listDelimiter, "")
  }

  def registerFieldValues(otherValues : FieldValueRegistry) : FieldValueRegistry = {
    otherValues.sortedFields.foreach(field => registerFieldValue(field))
    this
  }

  def registerFieldValue[T](field: SerializationFieldValue[T]) {
    val index = field.field.index

    fieldValues.update(index, field)

    if(index > maxIndex) maxIndex = index
  }

  lazy val sortedFields: Array[SerializationFieldValue[_]] = {
    fieldValues.keys.toArray.sorted.map(key => fieldValues(key))
  }

  def getValue[T](name: String) : T = {
    fieldNameIndexes.get(name) match {
      case Some(index) => getValue(index)
      case None => throw new IllegalArgumentException("Could not find field " + name + " in " + fieldRegistry.getCategoryName)
    }
  }

  def getValue[T](index: Int): T = {
    fieldValues.get(index) match {
      case Some(fieldValue) =>
        fieldValue.asInstanceOf[SerializationFieldValue[T]].value
      case None =>
        fieldRegistry.getFieldOption(index) match {
          case Some(field) =>
            if(field.required)
              throw new Exception("Field " + field.name + " in object " + fieldRegistry.getCategoryName + " is required but no value for it was found.")
            else
              field.asInstanceOf[SerializationField[T]].defaultValue
          case None =>
            throw new Exception("Could not find value or field definition for field index " + index)
        }
    }
  }

  def getValueIfPresent[T](index: Int): Option[T] = {
    fieldValues.get(index) match {
      case Some(value) => Some(value.asInstanceOf[SerializationFieldValue[T]].value)
      case None => None
    }
  }

  private var maxIndex: Int = 0

  def appendAvroField(rb: GenericRecordBuilder): GenericRecordBuilder = {
    rb.set("categoryName", fieldRegistry.getCategoryName)
    rb.set("version", fieldRegistry.getVersion)

    for (i <- 0 to maxIndex) {
      fieldValues.get(i) match {
        case Some(fv) => fv.field.appendAvroFieldValueTo(fv.value, rb)
        case None => fieldRegistry.getFieldOption(i).map(fieldDef => {
          if (fieldDef.required)
            throw new Exception(s"Value for required field ${fieldDef.name} in object $getCategoryName is missing")
          else
            fieldDef.appendAvroFieldValueTo(fieldDef.defaultValue, rb)
        })
      }
    }

    rb
  }

  def appendDelimitedFieldString(sb: StringBuilder): StringBuilder = {
    sb.append(fieldRegistry.getCategoryName).append(fieldDelimiter)
    sb.append(fieldRegistry.getVersion).append(fieldDelimiter)

    for (i <- 0 to maxIndex) {
      fieldValues.get(i) match {
        case Some(fv) =>
          fv.field.appendValueStringTo(fv.value, sb, listDelimiter)
        case None => fieldRegistry.getFieldOption(i).map(fieldDef => {
          if (fieldDef.required)
            throw new Exception(s"Value for required field ${fieldDef.name} in object $getCategoryName is missing")
          else
            fieldDef.appendValueStringTo(fieldDef.defaultValue, sb, listDelimiter)
        })
      }
      sb.append(fieldDelimiter)
    }

    sb
  }

  def appendBytes(o: PrimitiveOutputStream) {
    //Improvement: make this a header. use a length. track the consumed bytes, fast forward to # bytes in the event of an error
    val bos = new ByteArrayOutputStream()
    val tempOutput = new PrimitiveOutputStream(bos)

    //write everything to a temp stream so we can prepend a length
    VariableLengthEncoding.writeToStream(fieldRegistry.getVersion, tempOutput)
    tempOutput.writeByte(0) //dummy value for flags. can use this for a "lengths prepended?" value

    for (i <- 0 to maxIndex) {
      fieldValues.get(i) match {
        case Some(fv) => fv.field.writeBytesTo(fv.value, tempOutput)
        case None => fieldRegistry.getFieldOption(i).foreach(fieldDef => {
          if (fieldDef.required)
            throw new Exception(s"Value for required field ${fieldDef.name} in object $getCategoryName is missing")
          else
            fieldDef.writeBytesTo(fieldDef.defaultValue, tempOutput)
        })
      }
    }
    val length = tempOutput.size()
    VariableLengthEncoding.writeToStream(length, o)
    o.write(bos.toByteArray)

  }

  def toBytes(compress: Boolean): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val o = new PrimitiveOutputStream(bos)
    appendBytes(o)
    val bytes = bos.toByteArray
    if(compress)
      ArchiveUtils.compressBytes(bytes, withHeader = true)
    else
      bytes
  }

  def toDelimitedFieldString: String = {
    val sb = new StringBuilder()
    appendDelimitedFieldString(sb)
    sb.toString()
  }

  def toAvroRecord: GenericData.Record = {
    val rb = new GenericRecordBuilder(fieldRegistry.avroSchema)
    appendAvroField(rb)
    rb.build()
  }
}
