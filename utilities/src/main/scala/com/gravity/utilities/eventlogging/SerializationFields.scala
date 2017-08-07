package com.gravity.utilities.eventlogging

import java.io.UTFDataFormatException
import java.nio.ByteBuffer

import com.gravity.hbase.schema.{PrimitiveInputStream, PrimitiveOutputStream}
import com.gravity.utilities.VariableLengthEncoding
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import org.apache.avro.SchemaBuilder._
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}
import org.apache.avro.util.Utf8
import org.apache.avro.{Schema, SchemaBuilder}
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.AnyValManifest
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, NonEmptyList, Success, ValidationNel}

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

trait Serializer[T] extends Serializable {

  //string
  //why is this not parameterized? i'm sure there was a reason but now i don't know what it was
  def doAppendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String) : StringBuilder// = sb.append(value.toString)
  def doParseString(reg: FieldValueRegistry, str: String) : ValidationNel[FailureResult, T]

  //bytes
  def doWriteBytesTo(data: Any, output: PrimitiveOutputStream)
  def doReadBytesFrom(input: PrimitiveInputStream): ValidationNel[FailureResult, T]

  //avro
  def toAvroValue(value: Any) : Any = value
  //this will either be a SchemaBuilder.FieldDefault[_, _] or a SchemaBuilder.GenericDefault[Schema] which very irritatingly do not share any inheritance.
  def applyAvroFieldType(fieldBuilder: SchemaBuilder.FieldBuilder[Schema]) : Any
  def doReadAvroField(name: String, record: GenericData.Record): ValidationNel[FailureResult, T]
  def doReadSparkRow(name: String, row: org.apache.spark.sql.Row): ValidationNel[FailureResult, T] = row.getAs[T](name).successNel

  def hiveTypeName: String
}

trait SerializationField[T] {
  import com.gravity.logging.Logging._

  val name : String
  val index: Int
  val defaultValue: T
  val description: String
  val required : Boolean
  val minVersion : Int
  val serializer : Serializer[T]
  implicit val m: Manifest[T]

  import serializer._

  private def insufficientStreamVersion(streamVersion: Int) = {
    FailureResult("Stream is version " + streamVersion + " which is less than field minimum version " + minVersion).failureNel[T]
  }

  final def appendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String): StringBuilder = serializer.doAppendValueStringTo(value, sb, listDelimiter)

  final def typeString: String = m.toString() //this doesn't actually work as well as it ought to, with the specified manifests. either the manifests need to be adjusted or this does

  final def parseString(reg: FieldValueRegistry, str: String) : ValidationNel[FailureResult, T] = {
    try {
      if (str.isEmpty && !required)
        defaultValue.successNel
      else
        doParseString(reg, str) match {
          case Success(thing) => thing.successNel
          case Failure(fails) => NonEmptyList(FailureResult(getErrorMessage(reg, str))).append(fails).failure
        }
    }
    catch {
      case e: Exception => FailureResult(getErrorMessage(reg, str), e).failureNel
    }
  }

  final def writeBytesTo(data: Any, output: PrimitiveOutputStream): Unit = serializer.doWriteBytesTo(data, output)

  final def readBytesFrom(input: PrimitiveInputStream, streamVersion: Int): ValidationNel[FailureResult, T] = {
    if (streamVersion >= minVersion) {
      try {
        doReadBytesFrom(input) match {
          case Success(thing) => thing.successNel
          case Failure(fails) => NonEmptyList(FailureResult(getBytesErrorMessage)).append(fails).failure
        }
      }
      catch {
        case e: Exception =>  FailureResult(getBytesErrorMessage, e).failureNel
      }
    }
    else {
      //if the field isn't required, the default value will be used. but we dont want to read into the stream if it predates the field!
      insufficientStreamVersion(streamVersion)
    }
  }

  //thanks, avro, for having type return three kinds of things that don't share any inheritance. argh.
  //only one of these will be called, the output from the other will be ignored.
  //but it needs to return something, so the unused one should return none
  def applyAvroDefaultValue(schemaBuilder : SchemaBuilder.FieldDefault[_, _]) : Option[SchemaBuilder.FieldAssembler[_]]
  def applyAvroDefaultValue(schemaBuilder : SchemaBuilder.GenericDefault[_]) : Option[SchemaBuilder.FieldAssembler[_]] = None

  final def appendAvroFieldTo(sb: SchemaBuilder.FieldAssembler[Schema]) : SchemaBuilder.FieldAssembler[_] = {
    val schemaBuilder = sb.name(name)
    val returnOption = applyAvroFieldType(schemaBuilder) match {
      case fieldDefault:SchemaBuilder.FieldDefault[_,_] => applyAvroDefaultValue(fieldDefault)
      case fieldGeneric:SchemaBuilder.GenericDefault[_] => applyAvroDefaultValue(fieldGeneric)
      case fa: SchemaBuilder.FieldAssembler[_] => Some(fa) //the optional types do not give you a default back
      case _ => None
    }
    returnOption match {
      case Some(ret) => ret
      case None => throw new Exception("Nothing returned by avro default value setter. Something is wrong with the " + name + " implementation.")
    }
  }

  def appendAvroFieldValueTo(value: Any, rb: GenericRecordBuilder) : GenericRecordBuilder = rb.set(name, toAvroValue(value))

  final def readAvroField(record: GenericData.Record, streamVersion: Int): ValidationNel[FailureResult, T] = {
    if (streamVersion >= minVersion) {
      try {
        doReadAvroField(name, record) match {
          case Success(thing) => thing.successNel
          case Failure(fails) => NonEmptyList(FailureResult(getAvroErrorMessage)).append(fails).failure
        }
      }
      catch {
        case e: Exception => FailureResult(getAvroErrorMessage, e).failureNel
      }
    }
    else {
      //if the field isn't required, the default value will be used. but we dont want to read into the stream if it predates the field!
      insufficientStreamVersion(streamVersion)
    }
  }
  final def readSparkRow(row: org.apache.spark.sql.Row, streamVersion: Int): ValidationNel[FailureResult, T] = {
    if (streamVersion >= minVersion) {
      try {
        doReadSparkRow(name, row) match {
          case Success(thing) => thing.successNel
          case Failure(fails) => NonEmptyList(FailureResult(getSparkRowErrorMessage)).append(fails).failure
        }
      }
      catch {
        case e: Exception => FailureResult(getSparkRowErrorMessage, e).failureNel
      }
    }
    else {
      //if the field isn't required, the default value will be used. but we dont want to read into the stream if it predates the field!
      insufficientStreamVersion(streamVersion)
    }
  }

  protected def getErrorMessage(reg: FieldValueRegistry, str: String): String = {
    s"String '$str' could not be parsed as a $typeString for field '$name' at index $index in category ${reg.getCategoryName}"
  }

  protected def getBytesErrorMessage: String = {
    s"Bytes could not be parsed as a $typeString for field '$name' at index $index"
  }

  protected def getAvroErrorMessage: String = {
    s"Avro record could not be parsed as a $typeString for field '$name' at index $index"
  }
  protected def getSparkRowErrorMessage: String = {
    s"Spark row could not be parsed as a $typeString for field '$name' at index $index"
  }

  protected def getListErrorMessage(reg: FieldValueRegistry, str: String, item: String): String = {
    s"Item '$item' in list string '$str' of type $typeString could not be parsed for field '$name' at index $index in category ${reg.getCategoryName}"
  }

  def insertValue(reg: FieldValueRegistry, str: String) : ValidationNel[FailureResult, T] = {
    val thingNel : ValidationNel[FailureResult, T] = parseString(reg, str)

    processInsertValidation(reg, thingNel)
  }

  def insertValue(reg: FieldValueRegistry, input: PrimitiveInputStream, streamVersion: Int) : ValidationNel[FailureResult, T] = {
    val thingNel : ValidationNel[FailureResult, T] = readBytesFrom(input, streamVersion)

    processInsertValidation(reg, thingNel)
  }

  def insertValueFromAvro(reg: FieldValueRegistry, record: GenericData.Record, streamVersion: Int) : ValidationNel[FailureResult, T] = {
    val thingNel : ValidationNel[FailureResult, T] = readAvroField(record, streamVersion)

    processInsertValidation(reg, thingNel)
  }

  def insertValueFromSparkRow(reg: FieldValueRegistry, row: org.apache.spark.sql.Row, streamVersion: Int) : ValidationNel[FailureResult, T] = {
    val thingNel : ValidationNel[FailureResult, T] = readSparkRow(row, streamVersion)

    processInsertValidation(reg, thingNel)
  }

  private def processInsertValidation(reg: FieldValueRegistry, insertNel: ValidationNel[FailureResult, T]): ValidationNel[FailureResult, T] = {
    insertNel match {
      case Success(thing) => insertValue(reg, thing)
      case Failure(fails) =>
        val failsStr = fails.toString()
        if(!failsStr.contains("Stream is version"))
          warn("Error reading field " + fails.toString())
    }
    insertNel
  }

  def insertDefaultValue(reg:FieldValueRegistry) {
    insertValue(reg, defaultValue)
  }

  def insertValue(reg: FieldValueRegistry, value: T) {
    reg.registerFieldValue[T](SerializationFieldValue(this, value))
  }

}

trait BuiltinField
trait BuiltinSeqField
trait BuiltinOptionField

trait OptionField[T] extends SerializationField[Option[T]] with Serializer[Option[T]] {
  val baseSerializer : Serializer[T]
  val serializer: OptionField[T] = this

  override def hiveTypeName = baseSerializer.hiveTypeName

  override def doAppendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String): StringBuilder = {
    val opt = value.asInstanceOf[Option[T]]
    opt match {
      case Some(actual) => baseSerializer.doAppendValueStringTo(actual, sb.append("1,"), listDelimiter)
      case None => sb.append("0")
    }
    sb
  }

  override def doParseString(reg: FieldValueRegistry, str: String): ValidationNel[FailureResult, Option[T]] = {
    if (str == "0")
      None.successNel
    else if (str.startsWith("1,")) {
      val value = str.substring(2)

      baseSerializer.doParseString(reg, value) match {
        case Success(read) => Some(read).successNel
        case Failure(fails) => fails.failure

      }
    }
    else {
      FailureResult("Could not parse value as an Option[String]: " + str).failureNel
    }
  }

  //bytes
  override def doWriteBytesTo(data: Any, output: PrimitiveOutputStream): Unit = {
    val opt = data.asInstanceOf[Option[T]]
    //using VLE because it produces smaller output (the built in bool or byte uses an Int so i'm pretty sure it outputs 4 bytes),
    //and is compatible with the hacks that use 0 or 1 item lists for options that are already in use
    if (opt.isDefined) {
      output.writeByte(1)
      baseSerializer.doWriteBytesTo(opt.get, output)
    }
    else {
      output.write(VariableLengthEncoding.encode(0))
    }
  }

  override def doReadBytesFrom(input: PrimitiveInputStream): ValidationNel[FailureResult, Option[T]] = {
    val hasValue = input.readByte() == 1
    if(hasValue) {
      baseSerializer.doReadBytesFrom(input) match {
        case Success(item) => Some(item).successNel
        case Failure(fails) => fails.failure
      }
    }
    else {
      None.successNel[FailureResult]
    }
  }

  override def doReadSparkRow(name: String, row: org.apache.spark.sql.Row): ValidationNel[FailureResult, Option[T]] = {
    if (row.isNullAt(row.fieldIndex(name))) {
      None.successNel
    } else {
      Some(row.getAs[T](name)).successNel
    }
  }

  //  override def toAvroValue(value: Any) : Any = {
////    val opt = value.asInstanceOf[Option[T]]
////    seqAsJavaList{if (opt.isDefined) {
////      List[T](opt.get)
////    }
////    else {
////      List.empty[T]
////    }}
////  }

}

trait SeqField[T] extends SerializationField[Seq[T]] with Serializer[Seq[T]]{
  val baseSerializer : Serializer[T]
  val serializer: SeqField[T] = this

  override def hiveTypeName = "ARRAY<" + baseSerializer.hiveTypeName + ">"

  override def doAppendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String): StringBuilder = {
    val list = value.asInstanceOf[Seq[T]]
    var first = true
    for (x <- list) {
      if (first) {
        baseSerializer.doAppendValueStringTo(x, sb, listDelimiter)
        first = false
      }
      else {
        sb.append(listDelimiter)
        baseSerializer.doAppendValueStringTo(x, sb, listDelimiter)
      }
    }
    sb
  }

  override def doParseString(reg: FieldValueRegistry, str: String): ValidationNel[FailureResult, Seq[T]] = {
    listFromString[T](str, reg.getListDelimiter)(s => baseSerializer.doParseString(reg, s))
  }

  //bytes
  override def doWriteBytesTo(data: Any, output: PrimitiveOutputStream): Unit = {
    val list = data.asInstanceOf[Seq[T]]
    val size = list.size
    output.write(VariableLengthEncoding.encode(size))
    for (i <- 0 until size) {
      baseSerializer.doWriteBytesTo(list(i), output)
    }
  }

  override def doReadBytesFrom(input: PrimitiveInputStream): ValidationNel[FailureResult, Seq[T]] = {
    listFromBytes[T](input)(input => baseSerializer.doReadBytesFrom(input))
  }

  override def appendAvroFieldValueTo(value: Any, rb: GenericRecordBuilder) : GenericRecordBuilder = {
    val avroList = value.asInstanceOf[Seq[T]].map(baseSerializer.toAvroValue)
    rb.set(name, seqAsJavaList(avroList))
    rb
  }

  override def doReadAvroField(name: String, record: GenericData.Record): ValidationNel[FailureResult, Seq[T]] = {
    try {
      record.get(name).asInstanceOf[java.util.List[T]].toBuffer.successNel
    }
    catch {
      case e:Exception => FailureResult("Exception reading " + typeString + " from Avro field", e).failureNel
    }
  }

  override def doReadSparkRow(name: String, row: org.apache.spark.sql.Row): ValidationNel[FailureResult, Seq[T]] = {
    try {
      row.getAs[java.util.List[T]](name).toBuffer.successNel
    }
    catch {
      case e:Exception => FailureResult("Exception reading " + typeString + " from Spark Row", e).failureNel
    }
  }

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[_]] =
    schemaBuilder.asInstanceOf[ArrayDefault[_]].arrayDefault(seqAsJavaList(defaultValue)).some

  private def listFromString[I](listString: String, listDelimiter : String)(convert: String => ValidationNel[FailureResult,I]): ValidationNel[FailureResult, Seq[I]] = {
    val tokens = tokenize(listString, listDelimiter)
    val failBuf = scala.collection.mutable.Buffer[FailureResult]()
    val valueBuf = scala.collection.mutable.Buffer[I]()

    for (token <- tokens) {
      try {
        convert(token) match {
          case Success(value) => valueBuf += value
          case Failure(fails) => failBuf ++= fails.list
        }
      }
      catch {
        case e:Exception => failBuf += FailureResult("Could not convert list token " + token, e)
      }
    }

    failBuf.toNel match {
      case Some(failNel) => failNel.failure[Seq[I]]
      case None => valueBuf.successNel[FailureResult]
    }
  }

  private def listFromBytes[I](input: PrimitiveInputStream)(convert: PrimitiveInputStream => ValidationNel[FailureResult,I]) : ValidationNel[FailureResult, Seq[I]] = {
    try {
      val length = VariableLengthEncoding.readIntFromStream(input)
      val array = new scala.collection.mutable.ArrayBuffer[I](length)
      for (i <- 0 until length) {
        convert(input) match {
          case Success(item) => array += item
          case Failure(fails) => println(fails) //this is not correct. no it is not.
        }
      }
      array.successNel
    }
    catch {
      case e: Exception => FailureResult("Exception reading list from bytes", e).failureNel
    }
  }

  protected def listFromAvroRecord[I](record: GenericData.Record)(convert: GenericData.Record => ValidationNel[FailureResult,I]): ValidationNel[FailureResult, Seq[I]] = {
    try {
      val failBuf = scala.collection.mutable.Buffer[FailureResult]()
      val listBuff = mutable.Buffer[I]()
      val list = record.get(name).asInstanceOf[java.util.List[I]]

      list.foreach { value =>
        convert(value.asInstanceOf[GenericData.Record]) match {
          case Success(item) => listBuff += item
          case Failure(fails) => failBuf ++= fails.list
        }
      }

      failBuf.toNel match {
        case Some(failNel) => failNel.failure[Seq[I]]
        case None => listBuff.successNel[FailureResult]
      }
    }
    catch {
      case e: Exception => FailureResult("Exception reading Seq[T] from Avro field", e).failureNel
    }
  }


}

object IntSerializer extends Serializer[Int] {
  def doAppendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String) : StringBuilder = sb.append(value.toString)

  def doParseString(reg: FieldValueRegistry, str: String) : ValidationNel[FailureResult, Int] = str.toInt.successNel

  def doReadBytesFrom(input: PrimitiveInputStream): ValidationNel[FailureResult, Int] = input.readInt().successNel

  def doWriteBytesTo(data: Any, output: PrimitiveOutputStream) {
    output.writeInt(data.asInstanceOf[Int])
  }

  def doReadAvroField(name: String, record: GenericData.Record): ValidationNel[FailureResult, Int] = record.get(name).asInstanceOf[Int].successNel

  def applyAvroFieldType(fieldBuilder: SchemaBuilder.FieldBuilder[Schema]): FieldDefault[_, _] = fieldBuilder.`type`().intType()

  def hiveTypeName: String = "INT"
}

case class IntSerializationField(name: String, index: Int, defaultValue: Int, description: String, required : Boolean, minVersion : Int)
  extends SerializationField[Int] with BuiltinField {
  val serializer = IntSerializer

  val m: AnyValManifest[Int] = Manifest.Int

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[Schema]] = schemaBuilder.asInstanceOf[IntDefault[Schema]].intDefault(defaultValue).some
}


case class IntOptionSerializationField(name: String, index: Int, defaultValue: Option[Int], description: String, required: Boolean, minVersion: Int) extends OptionField[Int]
  with BuiltinOptionField {

  val m: Manifest[Option[Int]] = Manifest.singleType[Option[Int]](Some(0))

  override val baseSerializer: Serializer[Int] = IntSerializer

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[_]] = None

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, Option[Int]] = {
    val r = record.get(name)
    if(r == null)
      None.successNel
    else
      Some(r.asInstanceOf[Int]).successNel
  }

  override def toAvroValue(value: Any) = {
    value.asInstanceOf[Option[Int]].orNull
  }

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().optional().intType()
}

case class IntSeqSerializationField(name: String, index: Int, defaultValue: Seq[Int], description: String, required : Boolean, minVersion : Int)
  extends SeqField[Int] with BuiltinSeqField {

  override val baseSerializer: Serializer[Int] = IntSerializer

  override implicit val m: Manifest[Seq[Int]] = Manifest.singleType[Seq[Int]](Seq.empty[Int])

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().array().items().intType()
}

object LongSerializer extends Serializer[Long] {
  def doAppendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String) : StringBuilder = sb.append(value.toString)

  def doParseString(reg: FieldValueRegistry, str: String): ValidationNel[FailureResult, Long] = str.toLong.successNel

  def doReadBytesFrom(input: PrimitiveInputStream): ValidationNel[FailureResult, Long] = input.readLong().successNel

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, Long] = record.get(name).asInstanceOf[Long].successNel

  //this will either be a SchemaBuilder.FieldDefault[_, _] or a SchemaBuilder.GenericDefault[Schema] which very irritatingly do not share any inheritance.
  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().longType()

  //bytes
  override def doWriteBytesTo(data: Any, output: PrimitiveOutputStream): Unit =  output.writeLong(data.asInstanceOf[Long])

  def hiveTypeName: String = "BIGINT"
}

/*
case class OptionSerializationField(name: String, index: Int, defaultValue: Option[], description: String, required: Boolean, minVersion: Int) extends OptionField[]
  with BuiltinOptionField {

  val m: Manifest[Option[]] = Manifest.singleType[Option[]](Some())

  override val baseSerializer: Serializer[] =

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[_]] = None

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, Option[]] = {
    Option(record.get(name).asInstanceOf[]).successNel
  }

  override def toAvroValue(value: Any) = {
    value.asInstanceOf[Option[]].orNull
  }

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().optional().
}
*/
case class LongSerializationField(name: String, index: Int, defaultValue: Long, description: String, required : Boolean, minVersion : Int) extends SerializationField[Long] with BuiltinField {
  val m: AnyValManifest[Long] = Manifest.Long

  val serializer = LongSerializer

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[Schema]] = schemaBuilder.asInstanceOf[LongDefault[Schema]].longDefault(defaultValue).some

}

case class LongOptionSerializationField(name: String, index: Int, defaultValue: Option[Long], description: String, required: Boolean, minVersion: Int) extends OptionField[Long]
  with BuiltinOptionField {

  val m: Manifest[Option[Long]] = Manifest.singleType[Option[Long]](Some(0l))

  override val baseSerializer: Serializer[Long] = LongSerializer

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[_]] = None

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, Option[Long]] = {
    val r = record.get(name)
    if(r == null)
      None.successNel
    else
      Some(r.asInstanceOf[Long]).successNel
  }

  override def toAvroValue(value: Any) = {
    value.asInstanceOf[Option[Long]].orNull
  }

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().optional().longType()
}

case class LongSeqSerializationField(name: String, index: Int, defaultValue: Seq[Long], description: String, required : Boolean, minVersion : Int) extends SeqField[Long] with BuiltinSeqField {
  val m: Manifest[Seq[Long]] = Manifest.singleType[Seq[Long]](List.empty[Long])

  override val baseSerializer: Serializer[Long] = LongSerializer

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().array().items().longType()
}

object FloatSerializer extends Serializer[Float] {
  def doAppendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String) : StringBuilder = sb.append(value.toString)

  override def doParseString(reg: FieldValueRegistry, str: String): ValidationNel[FailureResult, Float] = str.toFloat.successNel

  override def doReadBytesFrom(input: PrimitiveInputStream): ValidationNel[FailureResult, Float] = input.readFloat().successNel

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, Float] = record.get(name).asInstanceOf[Float].successNel

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().floatType()

  override def doWriteBytesTo(data: Any, output: PrimitiveOutputStream): Unit =  output.writeFloat(data.asInstanceOf[Float])

  def hiveTypeName: String = "FLOAT"
}

case class FloatSerializationField(name: String, index: Int, defaultValue: Float, description: String, required : Boolean, minVersion : Int) extends SerializationField[Float] with BuiltinField {
  val m: AnyValManifest[Float] = Manifest.Float

  val serializer = FloatSerializer

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[Schema]] = schemaBuilder.asInstanceOf[FloatDefault[Schema]].floatDefault(defaultValue).some

}

case class FloatOptionSerializationField(name: String, index: Int, defaultValue: Option[Float], description: String, required: Boolean, minVersion: Int) extends OptionField[Float]
  with BuiltinOptionField {

  val m: Manifest[Option[Float]] = Manifest.singleType[Option[Float]](Some(0.0))

  override val baseSerializer: Serializer[Float] = FloatSerializer

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[_]] = None

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, Option[Float]] = {
    val r = record.get(name)
    if(r == null)
      None.successNel
    else
      Some(r.asInstanceOf[Float]).successNel
  }

  override def toAvroValue(value: Any) = {
    value.asInstanceOf[Option[Float]].orNull
  }

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().optional().floatType()
}

case class FloatSeqSerializationField(name: String, index: Int, defaultValue: Seq[Float], description: String, required : Boolean, minVersion : Int) extends SeqField[Float] with BuiltinSeqField {
  val m: Manifest[Seq[Float]] = Manifest.singleType[Seq[Float]](List.empty[Float])

  override val baseSerializer: Serializer[Float] = FloatSerializer

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().array().items().floatType()
}

object DoubleSerializer extends Serializer[Double] {
  def doAppendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String) : StringBuilder = sb.append(value.toString)

  override def doParseString(reg: FieldValueRegistry, str: String): ValidationNel[FailureResult, Double] = str.toDouble.successNel

  override def doReadBytesFrom(input: PrimitiveInputStream): ValidationNel[FailureResult, Double] = input.readDouble().successNel

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, Double] = record.get(name).asInstanceOf[Double].successNel

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().doubleType()

  override def doWriteBytesTo(data: Any, output: PrimitiveOutputStream): Unit = output.writeDouble(data.asInstanceOf[Double])

  def hiveTypeName: String = "DOUBLE"
}

case class DoubleSerializationField(name: String, index: Int, defaultValue: Double, description: String, required : Boolean, minVersion : Int) extends SerializationField[Double] with BuiltinField {
  val m: AnyValManifest[Double] = Manifest.Double

  override val serializer: Serializer[Double] = DoubleSerializer

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[Schema]] = schemaBuilder.asInstanceOf[DoubleDefault[Schema]].doubleDefault(defaultValue).some
}

case class DoubleOptionSerializationField(name: String, index: Int, defaultValue: Option[Double], description: String, required: Boolean, minVersion: Int) extends OptionField[Double]
  with BuiltinOptionField {

  val m: Manifest[Option[Double]] = Manifest.singleType[Option[Double]](Some(0d))

  override val baseSerializer: Serializer[Double] = DoubleSerializer

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[_]] = None

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, Option[Double]] = {
    val r = record.get(name)
    if(r == null)
      None.successNel
    else
      Some(r.asInstanceOf[Double]).successNel
  }

  override def toAvroValue(value: Any) = {
    value.asInstanceOf[Option[Double]].orNull
  }

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().optional().doubleType()
}

case class DoubleSeqSerializationField(name: String, index: Int, defaultValue: Seq[Double], description: String, required : Boolean, minVersion : Int) extends SeqField[Double] with BuiltinSeqField {
  override val baseSerializer: Serializer[Double] = DoubleSerializer

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().array().items().doubleType()

  val m: Manifest[Seq[Double]] = Manifest.classType[Seq[Double]](Seq.empty[Double].getClass)
}

object BooleanSerializer extends Serializer[Boolean]{
  override def doParseString(reg: FieldValueRegistry, str: String): ValidationNel[FailureResult, Boolean] = (str == "1").successNel

  override def doAppendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String): StringBuilder = if(value.asInstanceOf[Boolean]) sb.append("1") else sb.append("0")

  override def doReadBytesFrom(input: PrimitiveInputStream): ValidationNel[FailureResult, Boolean] = input.readBoolean().successNel

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, Boolean] = record.get(name).asInstanceOf[Boolean].successNel

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().booleanType()

  override def doWriteBytesTo(data: Any, output: PrimitiveOutputStream): Unit =  output.writeBoolean(data.asInstanceOf[Boolean])

  def hiveTypeName: String = "BOOLEAN"
}

case class BooleanSerializationField(name: String, index: Int, defaultValue: Boolean, description: String, required : Boolean, minVersion : Int) extends SerializationField[Boolean] with BuiltinField {
  val m: AnyValManifest[Boolean] = Manifest.Boolean

  override val serializer: Serializer[Boolean] = BooleanSerializer

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[Schema]] = schemaBuilder.asInstanceOf[BooleanDefault[Schema]].booleanDefault(defaultValue).some
}

case class BooleanOptionSerializationField(name: String, index: Int, defaultValue: Option[Boolean], description: String, required: Boolean, minVersion: Int) extends OptionField[Boolean]
  with BuiltinOptionField {

  val m: Manifest[Option[Boolean]] = Manifest.singleType[Option[Boolean]](Some(false))

  override val baseSerializer: Serializer[Boolean] = BooleanSerializer

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[_]] = None

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, Option[Boolean]] = {
    val r = record.get(name)
    if(r == null)
      None.successNel
    else
      Some(r.asInstanceOf[Boolean]).successNel
  }

  override def toAvroValue(value: Any) = {
    value.asInstanceOf[Option[Boolean]].orNull
  }

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().optional().booleanType()
}

case class BooleanSeqSerializationField(name: String, index: Int, defaultValue: Seq[Boolean], description: String, required : Boolean, minVersion : Int) extends SeqField[Boolean] with BuiltinSeqField {
  val m: Manifest[Seq[Boolean]] = Manifest.singleType[Seq[Boolean]](Seq.empty[Boolean])

  override val baseSerializer: Serializer[Boolean] = BooleanSerializer

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().array().items().booleanType()
}

object DateTimeSerializer extends Serializer[DateTime] {

  override def doParseString(reg: FieldValueRegistry, str: String): ValidationNel[FailureResult, DateTime] = new DateTime(str.toLong).successNel

  override def doAppendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String): StringBuilder = sb.append(value.asInstanceOf[DateTime].getMillis.toString)

  override def doReadBytesFrom(input: PrimitiveInputStream): ValidationNel[FailureResult, DateTime] = new DateTime(input.readLong()).successNel

  override def doWriteBytesTo(data: Any, output: PrimitiveOutputStream): Unit =  output.writeLong(data.asInstanceOf[DateTime].getMillis)

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, DateTime] = {
    val dateMillis = record.get(name).asInstanceOf[Long]
    new DateTime(dateMillis).successNel
  }

  override def doReadSparkRow(name: String, row: org.apache.spark.sql.Row): ValidationNel[FailureResult, DateTime] = {
    val dateMillis = row.getAs[Long](name)
    new DateTime(dateMillis).successNel
  }

  override def toAvroValue(value: Any): Long = value.asInstanceOf[DateTime].getMillis

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any =  fieldBuilder.`type`().longType()

  def hiveTypeName: String = "BIGINT"
}

case class DateTimeSerializationField(name: String, index: Int, defaultValue: DateTime, description: String, required : Boolean, minVersion : Int) extends SerializationField[DateTime] with BuiltinField {
  val m: Manifest[DateTime] = Manifest.singleType[DateTime](new DateTime(0))

  override val serializer: Serializer[DateTime] = DateTimeSerializer

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[Schema]] = schemaBuilder.asInstanceOf[LongDefault[Schema]].longDefault(defaultValue.getMillis).some
}

case class DateTimeOptionSerializationField(name: String, index: Int, defaultValue: Option[DateTime], description: String, required: Boolean, minVersion: Int) extends OptionField[DateTime]
  with BuiltinOptionField {

  val m: Manifest[Option[DateTime]] = Manifest.singleType[Option[DateTime]](Some(new DateTime(0)))

  override val baseSerializer: Serializer[DateTime] = DateTimeSerializer

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[_]] = None

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, Option[DateTime]] = {
    val r = record.get(name)
    if(r == null) {
      None.successNel
    }
    else {
      Some(new DateTime(r.asInstanceOf[Long])).successNel
    }
  }

  override def doReadSparkRow(name: String, row: org.apache.spark.sql.Row): ValidationNel[FailureResult, Option[DateTime]] = {
    if (row.isNullAt(row.fieldIndex(name))) {
      None.successNel
    } else {
      val dateMillis = row.getAs[Long](name)
      Some(new DateTime(dateMillis)).successNel
    }
  }

  override def toAvroValue(value: Any) = {
    val r = value.asInstanceOf[Option[DateTime]] match {
      case Some(dt) => Some(dt.getMillis)
      case None => None
    }
    r.orNull
  }

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().optional().longType()
}

case class DateTimeSeqSerializationField(name: String, index: Int, defaultValue: Seq[DateTime], description: String, required : Boolean, minVersion : Int)
  extends SeqField[DateTime] with BuiltinSeqField {
  val m: Manifest[Seq[DateTime]] = Manifest.singleType[Seq[DateTime]](Seq.empty[DateTime])

  override val baseSerializer: Serializer[DateTime] = DateTimeSerializer

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().array().items().longType()

  override def doReadAvroField(name: String, record: GenericData.Record): ValidationNel[FailureResult, Seq[DateTime]] = {
    try {
      val list = record.get(name).asInstanceOf[java.util.List[Long]].toBuffer
      list.map(v => new DateTime(v)).successNel
    }
    catch {
      case e: Exception => FailureResult("Exception reading Seq[DateTime] from Avro field", e).failureNel
    }
  }
}

class StringSerializer(encoded : Boolean) extends Serializer[String] {
  import com.gravity.logging.Logging._

  override def doParseString(reg: FieldValueRegistry, str: String): ValidationNel[FailureResult, String] = {
    if(encoded)
      URLUtils.urlDecode(str).successNel
    else
      str.successNel
  }

  override def doAppendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String): StringBuilder = {
    if(encoded)
      sb.append(URLUtils.urlEncode(value.asInstanceOf[String]))
    else
      sb.append(value.asInstanceOf[String].replace(listDelimiter, "").replace("^", "").replace("\n", "").replace("\r", ""))
  }

  override def doReadBytesFrom(input: PrimitiveInputStream): ValidationNel[FailureResult, String] = input.readUTF().successNel

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, String] = {
    val field = record.get(name)
    field match {
      case utf: Utf8 =>
        utf.toString.successNel
      case _ =>
        field.asInstanceOf[String].successNel
    }
  }

  //this will either be a SchemaBuilder.FieldDefault[_, _] or a SchemaBuilder.GenericDefault[Schema] which very irritatingly do not share any inheritance.
  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().stringType()

  //bytes
  override def doWriteBytesTo(data: Any, output: PrimitiveOutputStream): Unit = {
    val str = data.asInstanceOf[String]
    try {
      output.writeUTF(str.asInstanceOf[String])
    }
    catch {
      case u:UTFDataFormatException if u.getMessage.startsWith("encoded string too long") =>
        warn("String in serialized item was too long. The maximum serialized size for registerStringField is 64k. Use registerBigStringField. '" + str + "'")
        throw u
      case e:Throwable =>
        throw e
    }
  }

  def hiveTypeName: String = "STRING"
}

class BigStringSerializer(encoded : Boolean) extends Serializer[String] {
  override def doParseString(reg: FieldValueRegistry, str: String): ValidationNel[FailureResult, String] = {
    if(encoded)
      URLUtils.urlDecode(str).successNel
    else
      str.successNel
  }

  override def doAppendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String): StringBuilder = {
    if(encoded)
      sb.append(URLUtils.urlEncode(value.asInstanceOf[String]))
    else
      sb.append(value.asInstanceOf[String].replace(listDelimiter, "").replace("^", "").replace("\n", "").replace("\r", ""))
  }

  override def doReadBytesFrom(input: PrimitiveInputStream): ValidationNel[FailureResult, String] = {
    val bytesLen = VariableLengthEncoding.readIntFromStream(input)
    val strBytes = new Array[Byte](bytesLen)
    input.read(strBytes)
    new String(strBytes, "UTF-8").successNel
  }

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, String] = {
    val field = record.get(name)
    field match {
      case utf: Utf8 =>
        utf.toString.successNel
      case _ =>
        field.asInstanceOf[String].successNel
    }
  }

  //this will either be a SchemaBuilder.FieldDefault[_, _] or a SchemaBuilder.GenericDefault[Schema] which very irritatingly do not share any inheritance.
  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().stringType()

  //bytes
  override def doWriteBytesTo(data: Any, output: PrimitiveOutputStream): Unit = {
    //writeUTF has a 64k size limit because it only ever uses 2 bytes for the length prepend. We can do better!
    val str = data.asInstanceOf[String]
    val bytes = str.getBytes("UTF-8")
    VariableLengthEncoding.writeToStream(bytes.length, output)
    output.write(bytes)
  }

  override def hiveTypeName: String = "STRING"
}

case class UnencodedStringSerializationField(name: String, index: Int, defaultValue: String, description: String, required : Boolean, minVersion : Int) extends SerializationField[String] with BuiltinField {
  val m: Manifest[String] = Manifest.singleType[String]("")

  override val serializer: Serializer[String] = new StringSerializer(encoded = false)

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[Schema]] = schemaBuilder.asInstanceOf[StringDefault[Schema]].stringDefault(defaultValue).some
}

case class StringSerializationField(name: String, index: Int, defaultValue: String, description: String, required : Boolean, minVersion : Int) extends SerializationField[String] with BuiltinField {
  val m: Manifest[String] = Manifest.singleType[String]("")

  override val serializer: Serializer[String] = new StringSerializer(encoded = true)

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[Schema]] = schemaBuilder.asInstanceOf[StringDefault[Schema]].stringDefault(defaultValue).some
}

case class BigStringSerializationField(name: String, index: Int, defaultValue: String, description: String, required : Boolean, minVersion : Int) extends SerializationField[String] with BuiltinField {
  val m: Manifest[String] = Manifest.singleType[String]("")

  override val serializer: Serializer[String] = new BigStringSerializer(encoded = true)

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[Schema]] = schemaBuilder.asInstanceOf[StringDefault[Schema]].stringDefault(defaultValue).some
}

case class UnencodedStringSeqSerializationField(name: String, index: Int, defaultValue: Seq[String], description: String, required : Boolean, minVersion : Int) extends SeqField[String] with BuiltinSeqField {
  val m: Manifest[Seq[String]] = Manifest.singleType[Seq[String]](Seq.empty[String])

  override val baseSerializer: Serializer[String] = new StringSerializer(encoded = false)

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().array().items().stringType()
}

case class StringSeqSerializationField(name: String, index: Int, defaultValue: Seq[String], description: String, required : Boolean, minVersion : Int) extends SeqField[String] with BuiltinSeqField {
  val m: Manifest[Seq[String]] = Manifest.singleType[Seq[String]](Seq.empty[String])

  override val baseSerializer: Serializer[String] = new StringSerializer(encoded = true)

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().array().items().stringType()

}

case class StringOptionSerializationField(name: String, index: Int, defaultValue: Option[String], description: String, required: Boolean, minVersion: Int) extends OptionField[String] with BuiltinOptionField {
  val m: Manifest[Option[String]] = Manifest.singleType[Option[String]](Some(""))
  override val baseSerializer: Serializer[String] = new StringSerializer(encoded = true)

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[_]] = None

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, Option[String]] = {
    Option(record.get(name).asInstanceOf[String]).successNel
  }

  override def toAvroValue(value: Any): String = {
    value.asInstanceOf[Option[String]].orNull
  }

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().optional().stringType()

}

class ConvertableSerializer[T](implicit fieldConverter: FieldConverter[T]) extends Serializer[T] {
  override def doParseString(reg: FieldValueRegistry, str: String): ValidationNel[FailureResult, T] =
    fieldConverter.getInstanceFromString(URLUtils.urlDecode(str))

  override def doAppendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String): StringBuilder =
    sb.append(URLUtils.urlEncode(fieldConverter.toDelimitedFieldString(value.asInstanceOf[T])))

  override def doReadBytesFrom(input: PrimitiveInputStream): ValidationNel[FailureResult, T] = FieldValueRegistry.getInstanceFromPrimitiveInputStream(input)


  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, T] = fieldConverter.getInstanceFromAvroRecord(record.get(name).asInstanceOf[GenericData.Record])

  override def toAvroValue(value: Any): Record = fieldConverter.toAvroRecord(value.asInstanceOf[T])

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`(fieldConverter.avroSchema)

  override def doWriteBytesTo(data: Any, output: PrimitiveOutputStream): Unit =  output.write(fieldConverter.toBytes(data.asInstanceOf[T]))

  override def hiveTypeName: String = "STRUCT<" + fieldConverter.hiveColumnSchema(asStruct = true).toString() + ">"
}

case class ConvertableSerializationField[T](name: String, index: Int, defaultValue: T, description: String, required : Boolean, minVersion : Int)(implicit val m:Manifest[T], ev: FieldConverter[T]) extends SerializationField[T] {
  def getConverter: FieldConverter[T] = ev

  override val serializer: Serializer[T] = new ConvertableSerializer

  override def applyAvroDefaultValue(schemaBuilder: GenericDefault[_]): Option[FieldAssembler[Schema]] = {
    val default = ev.toAvroRecord(defaultValue)
    schemaBuilder.asInstanceOf[GenericDefault[Schema]].withDefault(default).some
  }

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[Schema]] = None
}

case class ConvertableSeqSerializationField[T](name: String, index: Int, defaultValue: Seq[T], description: String, required: Boolean, minVersion : Int)
                                              (implicit val m1:Manifest[T], ev: FieldConverter[T]) extends SeqField[T] {
  val m: Manifest[Seq[T]] = Manifest.singleType[Seq[T]](List(m1.newArray(0)))
  def getConverter: FieldConverter[T] = ev

  override val baseSerializer: Serializer[T] = new ConvertableSerializer

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().array().items().`type`(ev.avroSchema)

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[Schema]] = {
    schemaBuilder.asInstanceOf[ArrayDefault[Schema]].arrayDefault(seqAsJavaList(defaultValue)).some
  }

  override def appendAvroFieldValueTo(value: Any, rb: GenericRecordBuilder) : GenericRecordBuilder = {
    rb.set(name, seqAsJavaList(value.asInstanceOf[Seq[T]].map(v => ev.toAvroRecord(v))))
    rb
  }

  override def doReadAvroField(name: String, record: GenericData.Record): ValidationNel[FailureResult, Seq[T]] = {
    listFromAvroRecord(record)(record => FieldValueRegistry.getInstanceFromAvroRecord(record))
  }
}

class ByteArraySerializer(separator: Char) extends Serializer[Array[Byte]] {
  override def doParseString(reg: FieldValueRegistry, str: String): ValidationNel[FailureResult, Array[Byte]] = {
    val lengthAndBytes = str.split(separator)
    if (lengthAndBytes.length == 2) {
      val bytesString = lengthAndBytes(1)
      lengthAndBytes(0).tryToInt match {
        case Some(length) =>
          val bytes = new Array[Byte](length)
          var readIndex = 0
          var writeIndex = 0
          while (writeIndex < length) {
            val curString = bytesString.substring(readIndex, readIndex + 4)
            curString.trim.tryToByte match {
              case Some(byte) =>
                bytes(writeIndex) = byte
              case None => throw new Exception("Could not parse bytes with string " + curString)
            }
            readIndex += 4
            writeIndex += 1
          }
          bytes.successNel
        case None => FailureResult("Could not parse byte array string " + str).failureNel
      }
    }
    else {
      Array.empty[Byte].successNel
    }
  }

  override def doAppendValueStringTo(value: Any, sb: StringBuilder, listDelimiter: String) : StringBuilder = {
    val bytes = value.asInstanceOf[Array[Byte]]
    val byteStrings = bytes.map(_.formatted("%+04d"))
    sb.append(bytes.length.toString)
    sb.append(separator)
    sb.append(byteStrings.mkString(""))
  }

  override def doReadBytesFrom(input: PrimitiveInputStream): ValidationNel[FailureResult, Array[Byte]] = {
    val length = VariableLengthEncoding.readIntFromStream(input)
    val bytes = new Array[Byte](length)
    input.read(bytes)
    bytes.successNel
  }

  override def doReadAvroField(name: String, record: Record): ValidationNel[FailureResult, Array[Byte]] = record.get(name).asInstanceOf[ByteBuffer].array().successNel

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().bytesType()

  override def toAvroValue(value: Any): ByteBuffer = ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])

  override def doWriteBytesTo(data: Any, output: PrimitiveOutputStream): Unit = {
    val dataBytes = data.asInstanceOf[Array[Byte]]
    output.write(VariableLengthEncoding.encode(dataBytes.length))
    output.write(dataBytes)
  }

  override def hiveTypeName: String = "ARRAY<TINYINT>"
}

case class ByteArraySerializationField(name: String, index: Int, defaultValue: Array[Byte] = Array.empty[Byte], description: String = "Byte Array Field", required: Boolean, minVersion : Int) extends SerializationField[Array[Byte]] with BuiltinField {
  override implicit val m: Manifest[Array[Byte]] = Manifest.arrayType(Manifest.Byte)

  override val serializer: Serializer[Array[Byte]] = new ByteArraySerializer('|')

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[Schema]] =
    schemaBuilder.asInstanceOf[BytesDefault[Schema]].bytesDefault(ByteBuffer.wrap(defaultValue)).some
}

case class ByteArraySeqSerializationField(name: String, index: Int, defaultValue: Seq[Array[Byte]], description: String, required : Boolean, minVersion : Int) extends SeqField[Array[Byte]] with BuiltinSeqField {
  val m: Manifest[Seq[Array[Byte]]] = Manifest.singleType[Seq[Array[Byte]]](Seq.empty[Array[Byte]])
  override val baseSerializer: Serializer[Array[Byte]] = new ByteArraySerializer(':')

  override def applyAvroFieldType(fieldBuilder: FieldBuilder[Schema]): Any = fieldBuilder.`type`().array().items().bytesType()

  override def applyAvroDefaultValue(schemaBuilder: FieldDefault[_, _]): Option[FieldAssembler[Schema]] =
    schemaBuilder.asInstanceOf[ArrayDefault[Schema]].arrayDefault(seqAsJavaList(defaultValue.map(item => ByteBuffer.wrap(item)))).some

  override def doReadAvroField(name: String, record: GenericData.Record): ValidationNel[FailureResult, Seq[Array[Byte]]] = {
    try {
      val rec = record.get(name)
      rec.asInstanceOf[java.util.List[ByteBuffer]].map(item => item.array()).successNel
    }
    catch {
      case e: Exception => FailureResult("Exception reading Seq[Array[Byte]] from Avro field", e).failureNel
    }
  }

  override def appendAvroFieldValueTo(value: Any, rb: GenericRecordBuilder) : GenericRecordBuilder = {
    rb.set(name, seqAsJavaList(value.asInstanceOf[Seq[Array[Byte]]].map(item => ByteBuffer.wrap(item))))
    rb
  }
}


case class SerializationFieldValue[T](field: SerializationField[T], value: T)(implicit m: Manifest[T])

object BigStringEncoding extends App {
  val bigString = {for {i <- 0 until 100000 } yield i}.mkString("")

  val output = bigString.getBytes("UTF-8")

  println(output.size)

}