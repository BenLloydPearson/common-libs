package com.gravity.utilities.utilities

import com.gravity.utilities.utilities.TestConverters.FieldThingConverter
import org.junit._
import org.joda.time.DateTime
import com.gravity.utilities._
import com.gravity.utilities.eventlogging._
import grvfields._

import scalaz.{Failure, Success}

object TestConverters {
  implicit object CoreClassConverter extends FieldConverter[CoreClass] {
    val fields: FieldRegistry[CoreClass] = new FieldRegistry[CoreClass]("RecoLine")
      .registerIntField("int", 0, -1, "integer test", required = true)
      .registerDoubleField("double", 1, -1.asInstanceOf[Double], "double test", required = true)
      .registerFloatField("float", 3, -1.asInstanceOf[Float], "float test", required = true)

    def fromValueRegistry(reg: FieldValueRegistry): CoreClass = {
      new CoreClass(reg)
    }

    def toValueRegistry(o: CoreClass): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, int)
        .registerFieldValue(1, double)
        .registerFieldValue(3, float)
    }
  }

  implicit object NestedClassCoverter extends FieldConverter[NestedClass] {
    val fields: FieldRegistry[NestedClass] = new FieldRegistry[NestedClass]("OtherKindOfLine")
      .add(CoreClassConverter.fields)
      .registerUnencodedStringSeqField("strings", 2, Seq.empty[String], "list of strings test")

    def fromValueRegistry(reg: FieldValueRegistry): NestedClass = {
      new NestedClass(reg)
    }

    def toValueRegistry(o: NestedClass): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValues(CoreClassConverter.toValueRegistry(core))
        .registerFieldValue(2, strings)
    }
  }

  implicit object FieldThingConverter extends FieldConverter[LogLineThing] {
    val fields: FieldRegistry[LogLineThing] = new FieldRegistry[LogLineThing]("LogLineThing")
      .registerIntField("intfield", 0, -1, "int test", required = true)
      .registerDoubleField("doublefield", 1, -1.asInstanceOf[Double], "double test", required = true)
      .registerFloatField("floatfield", 2, -1.asInstanceOf[Float], "float test", required = true)
      .registerBooleanField("boolfield", 3, defaultValue = false, "boolean test", required = true)
      .registerUnencodedStringField("stringfield", 4, "", "string test", required = true)
      .registerUnencodedStringSeqField("stringlist", 5, Seq.empty[String], "string list test")
      .registerFloatSeqField("floatlist", 6, Seq.empty[Float], "float list test")
      .registerLongField("longfield", 7, -1.asInstanceOf[Long], "long test", required = true)
      .registerDateTimeField("dtfield", 8, grvtime.epochDateTime, "datetime test", required = true)
      .registerStringField("spaced_field", 15, "", "string field with a space", required = true)
      .registerIntSeqField("int_list_field", 16)
      .registerByteArrayField("byte_array_field", 17)
      .registerByteArraySeqField("byte_array_list_field", 18)
      .registerBigStringField("big_string_field", 19)


    def fromValueRegistry(reg : FieldValueRegistry): LogLineThing = {
      new LogLineThing(reg)
    }


    def toValueRegistry(o: LogLineThing): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
      .registerFieldValue(0, intfield)
      .registerFieldValue(1, doubleField)
      .registerFieldValue(2, floatField)
      .registerFieldValue(3, boolField)
      .registerFieldValue(4, stringField)
      .registerFieldValue(5, stringListField)
      .registerFieldValue(6, floatListField)
      .registerFieldValue(7, longField)
      .registerFieldValue(8, dtField)
      .registerFieldValue(15, spacedField)
      .registerFieldValue(16, intlistField)
      .registerFieldValue(17, byteArrayField)
      .registerFieldValue(18, byteArrayListField)
        .registerFieldValue(19, bigStringField)
    }

  }

  implicit object VersionedThingConverterZero extends FieldConverter[VersionedThing] {
    override def toValueRegistry(o: VersionedThing): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.fieldOne).registerFieldValue(1, o.listFieldOne)

    override def fromValueRegistry(reg: FieldValueRegistry): VersionedThing = VersionedThing(reg.getValue[Int](0), reg.getValue[Seq[Long]](1), -1, Seq.empty[Boolean])

    override val fields: FieldRegistry[VersionedThing] = new FieldRegistry[VersionedThing]("VersionedThing", version = 0).registerIntField("one", 0).registerLongSeqField("two", 1)
  }

  implicit object VersionedThingConverterOne extends FieldConverter[VersionedThing] {
    override def toValueRegistry(o: VersionedThing): FieldValueRegistry = {
      new FieldValueRegistry(fields)
        .registerFieldValue(0, o.fieldOne)
        .registerFieldValue(1, o.listFieldOne)
        .registerFieldValue(2, o.fieldTwo)
        .registerFieldValue(3, o.listFieldTwo)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): VersionedThing =
      VersionedThing(reg.getValue[Int](0), reg.getValue[Seq[Long]](1), reg.getValue[Double](2), reg.getValue[Seq[Boolean]](3))

    override val fields: FieldRegistry[VersionedThing] = {
      new FieldRegistry[VersionedThing]("VersionedThing", version = 1)
        .registerIntField("one", 0)
        .registerLongSeqField("two", 1)
        .registerDoubleField("three", 2, defaultValue = -1, minVersion = 1)
        .registerBooleanSeqField("four", 3, defaultValue = Seq.empty[Boolean], minVersion = 1)
    }
  }

  implicit object ParentVersionedThingConverter extends FieldConverter[ParentVersionedThing] {
    override def toValueRegistry(o: ParentVersionedThing): FieldValueRegistry = {
      //write with old version, read with new version
      new FieldValueRegistry(fields, version = 0)
        .registerFieldValue(0, o.thing)(manifest[VersionedThing], VersionedThingConverterZero)
        .registerFieldListValueDangerous(1, o.listOfThings)(manifest[VersionedThing], VersionedThingConverterZero)
    }
    override def fromValueRegistry(reg: FieldValueRegistry): ParentVersionedThing = ParentVersionedThing(reg.getValue[VersionedThing](0), reg.getValue[Seq[VersionedThing]](1))

    override val fields: FieldRegistry[ParentVersionedThing] = {
      new FieldRegistry[ParentVersionedThing]("ParentVersionedThing", version = 0)
        .registerField[VersionedThing]("thing", 0, VersionedThing.empty, minVersion = 0)(manifest[VersionedThing], VersionedThingConverterOne)
        .registerSeqField[VersionedThing]("thinglist", 1, Seq.empty[VersionedThing], minVersion = 0)(manifest[VersionedThing], VersionedThingConverterOne)
    }
  }

  implicit object OptionThingsConverter extends FieldConverter[OptionsThing] {
    override def toValueRegistry(o: OptionsThing): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, i)
        .registerFieldValue(1, d)
        .registerFieldValue(2, f)
        .registerFieldValue(3, b)
        .registerFieldValue(4, l)
        .registerFieldValue(5, s)
        .registerFieldValue(6, dt)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): OptionsThing =
      OptionsThing(
        reg.getValue[Option[Int]](0),
        reg.getValue[Option[Double]](1),
        reg.getValue[Option[Float]](2),
        reg.getValue[Option[Boolean]](3),
        reg.getValue[Option[Long]](4),
        reg.getValue[Option[String]](5),
        reg.getValue[Option[DateTime]](6))

    override val fields: FieldRegistry[OptionsThing] =
      new FieldRegistry[OptionsThing]("OptionsThing")
        .registerIntOptionField("i", 0)
        .registerDoubleOptionField("d", 1)
        .registerFloatOptionField("f", 2)
        .registerBooleanOptionField("b", 3)
        .registerLongOptionField("l", 4)
        .registerStringOptionField("s", 5)
        .registerDateTimeOptionField("dt", 6)
  }
}

case class CoreClass(int: Int, double: Double, float: Float) {
  def this(vals: FieldValueRegistry) = this(
    vals.getValue[Int](0), vals.getValue[Double](1), vals.getValue[Float](3)
  )
}

case class NestedClass(core: CoreClass, strings: Seq[String]) {
  def this(vals:FieldValueRegistry) = { this(new CoreClass(vals), vals.getValue[Seq[String]](2)) }
}

case class OptionsThing(i: Option[Int], d: Option[Double], f: Option[Float], b: Option[Boolean], l: Option[Long], s: Option[String], dt: Option[DateTime])

case class LogLineThing(intfield: Int, doubleField: Double, floatField: Float, boolField: Boolean, stringField: String,
                        stringListField: Seq[String], floatListField: Seq[Float], longField: Long, dtField: DateTime,
                        spacedField: String, intlistField: Seq[Int], byteArrayField: Array[Byte], byteArrayListField: Seq[Array[Byte]],
                        bigStringField: String
                       )
   {

  def this(vals : FieldValueRegistry) = this(
    vals.getValue[Int](0),
    vals.getValue[Double](1),
    vals.getValue[Float](2),
    vals.getValue[Boolean](3),
    vals.getValue[String](4),
    vals.getValue[Seq[String]](5),
    vals.getValue[Seq[Float]](6),
    vals.getValue[Long](7),
    vals.getValue[DateTime](8),
    vals.getValue[String](15),
    vals.getValue[Seq[Int]](16),
    vals.getValue[Array[Byte]](17),
    vals.getValue[Seq[Array[Byte]]](18),
    vals.getValue[String](19)
  )
}

object VersionedThing {
  val empty = VersionedThing(-1, Seq.empty[Long], -1, Seq.empty[Boolean])
}

case class VersionedThing(fieldOne: Int, listFieldOne: Seq[Long], fieldTwo: Double, listFieldTwo: Seq[Boolean])

case class ParentVersionedThing(thing: VersionedThing, listOfThings: Seq[VersionedThing])

object SortedTimeTest extends App {
  val iterations = 10000
  val watch = new Stopwatch()

  watch.start()
  var dummy = 0
  for(i <- 0 until iterations) {
    FieldThingConverter.fields.sortedFields.foreach(field => dummy += 1)
  }
  watch.stop()

  println(iterations.toString + " iterations of sorted took " + watch.formattedResult())

  val watch2 = new Stopwatch()
  val maxIndex = FieldThingConverter.fields.getMaxIndex
  watch2.start()
  for(i <- 0 until iterations) {
    for (j <- 0 to maxIndex) {
      FieldThingConverter.fields.getFieldOption(i).foreach(field => dummy += 1)
    }
  }
  watch2.stop()
  println(iterations.toString + " iterations of walking took " + watch2.formattedResult())


}

class SerializationTest extends BaseScalaTest {
  import TestConverters._

  val optionsThingSome = OptionsThing(Some(1), Some(2d), Some(3f), Some(true), Some(5l), Some("here's a string"), Some(new DateTime()))
  val optionsThingNone = OptionsThing(None, None, None, None, None, None, None)

  test("option things string test") {
    val s0 = OptionThingsConverter.toDelimitedFieldString(optionsThingSome)
    println(s0)
    OptionThingsConverter.getInstanceFromString(s0) match {
      case Success(thing) => assertResult(s0)(thing.toDelimitedFieldString())
      case Failure(fails) => fail(fails.toString)
    }

    val s1 = OptionThingsConverter.toDelimitedFieldString(optionsThingNone)
    println(s1)
    OptionThingsConverter.getInstanceFromString(s1) match {
      case Success(thing) => assertResult(s1)(thing.toDelimitedFieldString())
      case Failure(fails) => fail(fails.toString)
    }
  }

  test("option things bytes test") {
    val s0 = OptionThingsConverter.toBytes(optionsThingSome)
    println(s0.mkString(" "))
    OptionThingsConverter.getInstanceFromBytes(s0) match {
      case Success(thing) => assertResult(optionsThingSome.toDelimitedFieldString())(thing.toDelimitedFieldString())
      case Failure(fails) => fail(fails.toString)
    }

    val s1 = OptionThingsConverter.toBytes(optionsThingNone)
    println(s1.mkString(" "))
    OptionThingsConverter.getInstanceFromBytes(s1) match {
      case Success(thing) => assertResult(optionsThingNone.toDelimitedFieldString())(thing.toDelimitedFieldString())
      case Failure(fails) => fail(fails.toString)
    }
  }

  test("option things avro test") {
    val s0 = OptionThingsConverter.toAvroRecord(optionsThingSome)
    println(s0)
    OptionThingsConverter.getInstanceFromAvroRecord(s0) match {
      case Success(thing) => assertResult(optionsThingSome.toDelimitedFieldString())(thing.toDelimitedFieldString())
      case Failure(fails) => fail(fails.toString)
    }

    val s1 = OptionThingsConverter.toAvroRecord(optionsThingNone)
    println(s1)
    OptionThingsConverter.getInstanceFromAvroRecord(s1) match {
      case Success(thing) =>
        assertResult(optionsThingNone.toDelimitedFieldString())(thing.toDelimitedFieldString())
      case Failure(fails) => fail(fails.toString)
    }
  }

  test("field version bytes test") {

    //first make sure both converters work as expected
    val testObject = VersionedThing(4, Seq[Long](1l, 3l, 6l), -1, Seq.empty[Boolean])
    val bytes0 = VersionedThingConverterZero.toBytes(testObject)
    VersionedThingConverterZero.getInstanceFromBytes(bytes0) match {
      case Success(readObject) => assertResult(testObject.toDelimitedFieldString()(VersionedThingConverterZero))(readObject.toDelimitedFieldString()(VersionedThingConverterZero))
      case Failure(fails) => fail(fails.toString)
    }

    val testObject2 = VersionedThing(4, Seq[Long](1l, 3l, 6l), 7, Seq(true, false, false, false, true, true))
    val bytes1 = VersionedThingConverterOne.toBytes(testObject2)
    VersionedThingConverterOne.getInstanceFromBytes(bytes1) match {
      case Success(readObject) => assertResult(testObject2.toDelimitedFieldString()(VersionedThingConverterOne))(readObject.toDelimitedFieldString()(VersionedThingConverterOne))
      case Failure(fails) => fail(fails.toString)
    }

    //write bytes with version 0, which is missing a field
    //read bytes with version 1, ensure missing field has default value
    VersionedThingConverterOne.getInstanceFromBytes(bytes0) match {
      case Success(readObject) =>
        assertResult(testObject.toDelimitedFieldString()(VersionedThingConverterOne))(readObject.toDelimitedFieldString()(VersionedThingConverterOne))
      case Failure(fails) => fail(fails.toString)
    }

    //do the same with a parent object that contains a list of children object with that versioning
    val testParentObject = ParentVersionedThing(testObject, Seq(testObject, testObject, testObject))
    val listBytes = ParentVersionedThingConverter.toBytes(testParentObject)
    ParentVersionedThingConverter.getInstanceFromBytes(listBytes) match {
      case Success(readObject) => assertResult(testParentObject.toDelimitedFieldString())(readObject.toDelimitedFieldString())
      case Failure(fails) => fail(fails.toString)
    }
  }

  test("field version avro test") {

    //first make sure both converters work as expected
    val testObject = VersionedThing(4, Seq[Long](1l, 3l, 6l), -1, Seq.empty[Boolean])
    val bytes0 = VersionedThingConverterZero.toAvroRecord(testObject)
    VersionedThingConverterZero.getInstanceFromAvroRecord(bytes0) match {
      case Success(readObject) => assertResult(testObject.toDelimitedFieldString()(VersionedThingConverterZero))(readObject.toDelimitedFieldString()(VersionedThingConverterZero))
      case Failure(fails) => fail(fails.toString)
    }

    val testObject2 = VersionedThing(4, Seq[Long](1l, 3l, 6l), 7, Seq(true, false, false, false, true, true))
    val bytes1 = VersionedThingConverterOne.toAvroRecord(testObject2)
    VersionedThingConverterOne.getInstanceFromAvroRecord(bytes1) match {
      case Success(readObject) => assertResult(testObject2.toDelimitedFieldString()(VersionedThingConverterOne))(readObject.toDelimitedFieldString()(VersionedThingConverterOne))
      case Failure(fails) => fail(fails.toString)
    }

    //write bytes with version 0, which is missing a field
    //read bytes with version 1, ensure missing field has default value
    VersionedThingConverterOne.getInstanceFromAvroRecord(bytes0) match {
      case Success(readObject) =>
        assertResult(testObject.toDelimitedFieldString()(VersionedThingConverterOne))(readObject.toDelimitedFieldString()(VersionedThingConverterOne))
      case Failure(fails) => fail(fails.toString)
    }

    //do the same with a parent object that contains a list of children object with that versioning
    val testParentObject = ParentVersionedThing(testObject, Seq(testObject, testObject, testObject))
    val listBytes = ParentVersionedThingConverter.toAvroRecord(testParentObject)
    ParentVersionedThingConverter.getInstanceFromAvroRecord(listBytes) match {
      case Success(readObject) => assertResult(testParentObject.toDelimitedFieldString())(readObject.toDelimitedFieldString())
      case Failure(fails) => fail(fails.toString)
    }
  }

  test("testTestClass") {
    val newLineString =
      """some
        |
        |stuff
        |and things^
      """
    println(newLineString)

    val byteArrays = Seq(Array[Byte](8,6,7,5,3,0,9), Array[Byte](1,2,3,4,5,6,11),Array[Byte](10,8,4,13,15,1,9,100))
    val bigString = {for {i <- 0 until 100000 } yield i}.mkString("")

    val thing = new LogLineThing(1, 2, 3, true, newLineString, Seq("1", "2", "3"), Seq(1, 2, 3), 1001,
      new DateTime(), "spaced out", Seq(3, 2, 1), Array[Byte](8,6,7,5,3,0,9), byteArrays, bigString)
    thing.toDelimitedFieldString()
    val line = grvfields.toDelimitedFieldString(thing)
    println(line)
    grvfields.getInstanceFromString[LogLineThing](line).fold({fails=>Assert.fail(fails.toString())},{deserialized => {
      println("string round trip")
      println(deserialized)
      println(deserialized.stringField)
      Assert.assertEquals(line, grvfields.toDelimitedFieldString(deserialized))
    }})
    val bytes = grvfields.toBytes(thing)
    grvfields.getInstanceFromBytes[LogLineThing](bytes).fold({fails=>Assert.fail(fails.toString())}, {deserialized => {
      Assert.assertArrayEquals(bytes, grvfields.toBytes(deserialized))
    }})
    val avro = grvfields.toAvroRecord(thing)
    grvfields.getInstanceFromAvroRecord[LogLineThing](avro).fold({fails=>Assert.fail(fails.toString())}, {deserialized => {
      println("avro round trip")
      println(deserialized)
      Assert.assertEquals(line, grvfields.toDelimitedFieldString(deserialized))
    }})

    val thing2 = CoreClass(6, 7, 8)
    val line2 = grvfields.toDelimitedFieldString(thing2)
    println(line2)
    FieldValueRegistry.getInstanceFromString[CoreClass](line2).fold({fails=>Assert.fail(fails.toString())},{deserialized2=> {
      println(deserialized2)
      Assert.assertEquals(line2, grvfields.toDelimitedFieldString(deserialized2))
    }})

    val thing3 = new NestedClass(thing2, Seq("la", "de", "da"))
    val line3 = grvfields.toDelimitedFieldString(thing3)
    println(line3)
    FieldValueRegistry.getInstanceFromString[NestedClass](line3).fold({fails=>Assert.fail(fails.toString)}, {deserialized3 => {
      println(deserialized3)
      Assert.assertEquals(line3, grvfields.toDelimitedFieldString(deserialized3))
    }})

  }
}
