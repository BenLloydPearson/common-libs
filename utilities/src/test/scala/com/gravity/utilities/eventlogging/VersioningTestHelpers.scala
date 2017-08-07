package com.gravity.utilities.eventlogging

import com.gravity.utilities._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvfields._
import com.gravity.utilities.grvz._
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException
import org.joda.time.DateTime
import org.junit.Assert._
import org.junit.Test

import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scalaz.syntax.apply._
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, Validation, ValidationNel}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/21/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

trait FieldConverterTest extends  {
  val convertersToTest : Seq[FieldConverter[_]]
    @Test def ConvertersTest() {
    convertersToTest.foreach(VersioningTestHelpers.testConverter(_))
  }
}

case class OptionStringTest(opt: Option[String])

object VersioningTestHelpers {

  private val devConnHost = "db-development.aws.prod.grv"
  private val devConnStr = s"jdbc:mysql://$devConnHost/SerializationTesting?autoReconnect=true"
  private val devConnUser = "interest"
  private val devConnPass = "typingpasswordsIZPHUN"
  private val random = new Random()

  implicit object OptionTestConverter extends FieldConverter[OptionStringTest] {
    override def toValueRegistry(o: OptionStringTest): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.opt)

    override def fromValueRegistry(reg: FieldValueRegistry): OptionStringTest = new OptionStringTest(reg.getValue[Option[String]](0))

    override val fields: FieldRegistry[OptionStringTest] = new FieldRegistry[OptionStringTest]("ListTest").registerStringOptionField("list", 0)
  }

  def delete(name: String, version: Int): Unit = {
    println("Clearing value for object " + name + " at version " + version)
    try {
      MySqlConnectionProvider.withConnection(devConnStr, "SerializationTesting", devConnUser, devConnPass) {
        conn => {
          val stmt = conn.prepareStatement("DELETE FROM field_serialization_test WHERE name = ? AND version = ?")
          stmt.setString(1, name)
          stmt.setInt(2, version)
          stmt.execute()
        }
      }
    }
    catch {
      case e:Exception => println(e.toString)
    }
  }

  private def store(name: String, version: Int, value: String, bytes: Array[Byte]) {
    println("Storing value for object " + name + " at version " + version)
    try {
      MySqlConnectionProvider.withConnection(devConnStr, "SerializationTesting", devConnUser, devConnPass) {
        conn => {
          val stmt = conn.prepareStatement("INSERT INTO field_serialization_test (name, version, value, bytes) VALUES (?, ?, ?, ?)")
          stmt.setString(1, name)
          stmt.setInt(2, version)
          stmt.setString(3, value)
          stmt.setBytes(4, bytes)
          stmt.execute()
        }
      }
    }
    catch {
      case e:MySQLIntegrityConstraintViolationException => println("Row already existed for " + name + " at version " + version)
    }
  }

  def createTestValueRegistry[T](ev: FieldConverter[T]) : FieldValueRegistry = {
    val fvr = new FieldValueRegistry(ev.fields)
    ev.fields.sortedFields.foreach {
      case field@(f: IntSerializationField) => fvr.registerFieldValue(field.index, 23)
      case field@(f: IntOptionSerializationField) => fvr.registerFieldValue(field.index, Some(23))
      case field@(f: LongSerializationField) => fvr.registerFieldValue(field.index, 23l)
      case field@(f: LongOptionSerializationField) => fvr.registerFieldValue(field.index, Some(23l))
      case field@(f: FloatSerializationField) => fvr.registerFieldValue(field.index, 4.2f)
      case field@(f: FloatOptionSerializationField) => fvr.registerFieldValue(field.index, Some(4.2f))
      case field@(f: DoubleSerializationField) => fvr.registerFieldValue(field.index, 2.3d)
      case field@(f: DoubleOptionSerializationField) => fvr.registerFieldValue(field.index, Some(2.3d))
      case field@(f: BooleanSerializationField) => fvr.registerFieldValue(field.index, value = true)
      case field@(f: BooleanOptionSerializationField) => fvr.registerFieldValue(field.index, value = Some(true))
      case field@(f: DateTimeSerializationField) => fvr.registerFieldValue(field.index, new DateTime(42))
      case field@(f: DateTimeOptionSerializationField) => fvr.registerFieldValue(field.index, Some(new DateTime(42)))
      case field@(f: UnencodedStringSerializationField) =>
        val valueToUse = if (f.defaultValue.isEmpty)
          "i am a string"
        else
          f.defaultValue
        fvr.registerFieldValue(field.index, valueToUse)
      case field@(f: StringSerializationField) => fvr.registerFieldValue(field.index, "I am a string with a \n newline and delimiters | ^")
      case field@(f: IntSeqSerializationField) => fvr.registerFieldValue(field.index, Seq(42, 23))
      case field@(f: LongSeqSerializationField) => fvr.registerFieldValue(field.index, Seq(23l, 42l))
      case field@(f: FloatSeqSerializationField) => fvr.registerFieldValue(field.index, Seq(2.3f, 4.2f))
      case field@(f: DoubleSeqSerializationField) => fvr.registerFieldValue(field.index, Seq(4.2d, 2.3d))
      case field@(f: BooleanSeqSerializationField) => fvr.registerFieldValue(field.index, Seq(true, false, true))
      case field@(f: DateTimeSeqSerializationField) => fvr.registerFieldValue(field.index, Seq(new DateTime(42), new DateTime(23)))
      case field@(f: UnencodedStringSeqSerializationField) => fvr.registerFieldValue(field.index, Seq("i am a string", "which is in a list"))
      case field@(f: StringSeqSerializationField) => fvr.registerFieldValue(field.index, Seq("I am a string with a \n newline and delimiters | ^", "which is in a list and symbols )(*&#$%"))
      case field@(f: ConvertableSerializationField[_]) =>
        val fev = f.getConverter
        val man = f.m
        val value = fev.fromValueRegistry(createTestValueRegistry(fev))
        fvr.registerFieldValueDangerous(field.index, value)(man, fev)
      case field@(f: ConvertableSeqSerializationField[_]) =>
        val fev = f.getConverter
        val man = f.m1
        val value = fev.fromValueRegistry(createTestValueRegistry(fev))
        val list = List(value, value)
        fvr.registerFieldListValueDangerous(field.index, list)(man, fev)
      case field@(f: StringOptionSerializationField) =>
        fvr.registerFieldValue(field.index, Some("i'm something!"))
      case field@(f: ByteArraySerializationField) =>
        val arr = new Array[Byte](random.nextInt(100))
        random.nextBytes(arr)
        fvr.registerFieldValue(field.index, arr)
    }
    fvr
  }

  def retrieve(name: String, version: Int) : Validation[FailureResult, (String, Array[Byte])] = {
    MySqlConnectionProvider.withConnection(devConnStr, "SerializationTesting", devConnUser, devConnPass) {
      conn => {
        val stmt = conn.prepareStatement("SELECT value, bytes FROM field_serialization_test WHERE name=(?) AND version=(?)")
        stmt.setString(1, name)
        stmt.setInt(2, version)
        val rs = stmt.executeQuery()
        if(rs.next())
          (rs.getString("value"), rs.getBytes("bytes")).success
        else
          FailureResult("Could not find " + name + " at version " + version).failure
      }
    }
  }

  def store[T](ev: FieldConverter[T], testObj: Option[_] = None) {
    val (str, bytes) : (String, Array[Byte]) = testObj match {
      case Some(obj) =>
        (obj.asInstanceOf[T].toDelimitedFieldString()(ev), obj.asInstanceOf[T].toBytes()(ev))
      case None =>
        val vr = createTestValueRegistry(ev)
        (vr.toDelimitedFieldString, vr.toBytes(ev.compressBytes))
    }
    store(ev.getCategoryName, ev.serializationVersion, str, bytes)
  }

  def exerciseConverter[T](ev: FieldConverter[T]) : ValidationNel[FailureResult, Map[Int, (T, T)]] = {
    val successes = new mutable.HashMap[Int, (T, T)]()

    val failures = ArrayBuffer[FailureResult]()
    println("testing " + ev.getCategoryName + " which has current version " + ev.serializationVersion)
    for (i <- 0 to ev.serializationVersion) {
      println("testing version " + i)
      retrieve(ev.getCategoryName, i) match {
        case Success((str, bytes)) =>
          println("got string " + str)
          (ev.getInstanceFromString(str) |@| ev.getInstanceFromBytes(bytes)) {
            (fromString: T, fromBytes: T) => (fromString, fromBytes)
          } match {
            case Success((fromString, fromBytes)) => successes.update(i, (fromString, fromBytes))
            case Failure(fails) => fails.map(failures.append(_))
          }
        case Failure(fail) =>
          if(i == ev.fields.getVersion) {
            println(fail)
            failures.append(FailureResult("No data to test " + ev.getCategoryName + " at version " + i))
          }
      }
    }
    if (failures.isEmpty)
      successes.successNel
    else
      failures.toNel.toFailure(Map.empty[Int, (T,T)])
  }

  def testConverter[T](ev:FieldConverter[T], testObjectOption: Option[Any] = None) {
    try {
      val avroSchema = ev.avroSchema
     // println(avroSchema.toString(true))
    }
    catch {
      case e:Exception =>
        val converterClassName = ev.getClass.getCanonicalName
        fail(s"exception building avro schema for converter class '$converterClassName': ${ScalaMagic.formatException(e)}")
    }

    testCurrentRoundTrip(ev, testObjectOption.asInstanceOf[Option[T]])

    exerciseConverter(ev) match {
      case Success(things) => println("Succeeded with objects \n" + things.mkString("\n"))
      case Failure(fails) => org.junit.Assert.fail(fails.toString)
     }
  }

  def testCurrentRoundTrip[T](ev:FieldConverter[T], testObjectOption : Option[T]) {
    println("testing round trip for " + ev.getCategoryName)
    val testObjectRegistry =
      testObjectOption match {
        case Some(obj) => obj.toValueRegistry()(ev)
        case None => createTestValueRegistry[T](ev)
      }
    val bytes = testObjectRegistry.toBytes(ev.compressBytes)
    println("got " + bytes.length + " bytes for test object")
    println(bytes.mkString(" "))
    val string = testObjectRegistry.toDelimitedFieldString
    println(string)
    val record = testObjectRegistry.toAvroRecord
    println(record.toString)

    ev.getInstanceFromBytes(bytes) match {
      case Success(testObjectFromBytes) =>
        println("from bytes: " + testObjectFromBytes.toDelimitedFieldString()(ev))
        val roundTripRegistry = testObjectFromBytes.toValueRegistry()(ev)
//        val numFields = testObjectRegistry.getMaxIndex
//        val fields = testObjectRegistry.getFields
//        for(i <- 0 to numFields) {
//          fields(i)
//          val expectedField = testObjectRegistry.getField(i)
//          val actualField = roundTripRegistry.getField(i)
//          assertEquals("field " + i + " in " + ev.getCategoryName + " did not match", expectedField.value, actualField.value)
//        }

        val roundTripBytes = testObjectFromBytes.toBytes()(ev)
        assertArrayEquals("round trip bytes didn't match for current version of " + ev.getCategoryName, bytes, roundTripBytes)
      case Failure(fails) => fail("could not read " + ev.getCategoryName + " from bytes: "+ fails.toString)
    }

    ev.getInstanceFromString(string) match {
      case Success(testObjectFromString) =>
        val roundTripString = testObjectFromString.toDelimitedFieldString()(ev)
        assertEquals("round trip string didn't match for current version of " + ev.getCategoryName, string, roundTripString)
      case Failure(fails) => fail("could not read " + ev.getCategoryName + " from string: " + fails.toString)
    }

    ev.getInstanceFromAvroRecord(record) match {
      case Success(testObjectFromRecord) =>
        val roundTripRecord = testObjectFromRecord.toAvroRecord()(ev)
        assertEquals("round trip avro didn't match for current version of " + ev.getCategoryName, record, roundTripRecord)
      case Failure(fails) => fail("could not read " + ev.getCategoryName + " from record: " + fails.toString())
    }

  }
}

class FieldTypeTests extends BaseScalaTest {
  import VersioningTestHelpers._
  val emptyInstance: OptionStringTest = new OptionStringTest(None)
  val blankInstance: OptionStringTest = new OptionStringTest(Some(""))
  val populatedInstance: OptionStringTest = new OptionStringTest(Some("a thing"))

  test("test option string via string") {
    val rt1 = grvfields.getInstanceFromString[OptionStringTest](grvfields.toDelimitedFieldString(emptyInstance))
    println("empty: " + rt1)
    val rt2 = grvfields.getInstanceFromString[OptionStringTest](grvfields.toDelimitedFieldString(blankInstance))
    println("blank: " + rt2)
    val rt3 = grvfields.getInstanceFromString[OptionStringTest](grvfields.toDelimitedFieldString(populatedInstance))
    println("populated " + rt3)

    rt1 match {
      case Success(thing) => thing.opt should be (None)
      case Failure(fails) => fail(fails.toString())
    }

    rt2 match {
      case Success(thing) => thing.opt should be (Some(""))
      case Failure(fails) => fail(fails.toString())
    }

    rt3 match {
      case Success(thing) => thing.opt should be(Some("a thing"))
      case Failure(fails) => fail(fails.toString())
    }
  }

  test("test option string via bytes") {
    val rt1 = grvfields.getInstanceFromBytes[OptionStringTest](grvfields.toBytes(emptyInstance))
    println("empty: " + rt1)
    val rt2 = grvfields.getInstanceFromBytes[OptionStringTest](grvfields.toBytes(blankInstance))
    println("blank: " + rt2)
    val rt3 = grvfields.getInstanceFromBytes[OptionStringTest](grvfields.toBytes(populatedInstance))
    println("populated " + rt3)

    rt1 match {
      case Success(thing) => thing.opt should be (None)
      case Failure(fails) => fail(fails.toString())
    }

    rt2 match {
      case Success(thing) => thing.opt should be (Some(""))
      case Failure(fails) => fail(fails.toString())
    }

    rt3 match {
      case Success(thing) => thing.opt should be(Some("a thing"))
      case Failure(fails) => fail(fails.toString())
    }
  }
}