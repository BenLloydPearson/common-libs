package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.{SampleEvent, SampleNestedEvent}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime
import org.joda.time.DateTime

trait SampleEventConverter {
  this: FieldConverters.type =>

  implicit object SampleEventConverter extends FieldConverter[SampleEvent] {
    val fields: FieldRegistry[SampleEvent] = new FieldRegistry[SampleEvent]("SampleEvent")
      .registerStringField("seString", 0, emptyString, "sample string field")
      .registerIntField("seInt", 1, -1, "sample integer field")
      .registerLongField("seLong", 2, -1L, "sample long field")
      .registerFloatField("seFloat", 3, -1.0F, "sample float field")
      .registerDoubleField("seDouble", 4, -1.0, "sample double field")
      .registerBooleanField("seBoolean", 5, false, "sample boolean field")
      .registerStringSeqField("seListString", 6, Seq.empty[String], "sample list string field")
      .registerIntSeqField("seListInt", 7, Seq.empty[Int], "sample list int field")
      .registerLongSeqField("seListLong", 8, Seq.empty[Long], "sample list long field")
      .registerFloatSeqField("seListFloat", 9, Seq.empty[Float], "sample list float field")
      .registerDoubleSeqField("seListDouble", 10, Seq.empty[Double], "sample list double field")
      .registerBooleanSeqField("seListBoolean", 11, Seq.empty[Boolean], "sample list boolean field")
      .registerDateTimeField("seDateTime", 12, grvtime.epochDateTime, "sample date time field")
      .registerDateTimeSeqField("seListDateTime", 13, Seq.empty[DateTime], "sample list date time field")
      .registerField[SampleNestedEvent]("seObject", 14, SampleNestedEvent.empty)
      .registerSeqField[SampleNestedEvent]("seListObject", 15, Seq.empty[SampleNestedEvent], "sample list object field")
      .registerByteArrayField("seArrayByte", 16, Array.empty[Byte], "sample array of bytes field")
      .registerStringOptionField("seOptionString", 17, Option(null), "sample string option field")
      .registerStringOptionField("seOptionStringTwo", 18, Option(null), "sample string option field for nullable")
      .registerUnencodedStringField("seUnencodedString", 19, emptyString, "sample unencoded string field")
      .registerUnencodedStringSeqField("seListUnencodedString", 20, Seq.empty[String], "sample list unencoded string field")
      .registerByteArraySeqField("seListArrayByte", 21, Seq.empty[Array[Byte]], "sample list of byte array field")

    def fromValueRegistry(reg: FieldValueRegistry): SampleEvent = new SampleEvent(
      reg.getValue[String](0),
      reg.getValue[Int](1),
      reg.getValue[Long](2),
      reg.getValue[Float](3),
      reg.getValue[Double](4),
      reg.getValue[Boolean](5),
      reg.getValue[Seq[String]](6),
      reg.getValue[Seq[Int]](7),
      reg.getValue[Seq[Long]](8),
      reg.getValue[Seq[Float]](9),
      reg.getValue[Seq[Double]](10),
      reg.getValue[Seq[Boolean]](11),
      reg.getValue[DateTime](12),
      reg.getValue[Seq[DateTime]](13),
      reg.getValue[SampleNestedEvent](14),
      reg.getValue[Seq[SampleNestedEvent]](15),
      reg.getValue[Array[Byte]](16),
      reg.getValue[Option[String]](17),
      reg.getValue[Option[String]](18),
      reg.getValue[String](19),
      reg.getValue[Seq[String]](20),
      reg.getValue[Seq[Array[Byte]]](21)
    )

    def toValueRegistry(o: SampleEvent): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, seString)
        .registerFieldValue(1, seInt)
        .registerFieldValue(2, seLong)
        .registerFieldValue(3, seFloat)
        .registerFieldValue(4, seDouble)
        .registerFieldValue(5, seBoolean)
        .registerFieldValue(6, seListString)
        .registerFieldValue(7, seListInt)
        .registerFieldValue(8, seListLong)
        .registerFieldValue(9, seListFloat)
        .registerFieldValue(10, seListDouble)
        .registerFieldValue(11, seListBoolean)
        .registerFieldValue(12, seDateTime)
        .registerFieldValue(13, seListDateTime)
        .registerFieldValue(14, seObject)
        .registerFieldValue(15, seListObject)
        .registerFieldValue(16, seArrayByte)
        .registerFieldValue(17, seOptionString)
        .registerFieldValue(18, seOptionStringTwo)
        .registerFieldValue(19, seUnencodedString)
        .registerFieldValue(20, seListUnencodedString)
        .registerFieldValue(21, seListArrayByte)
    }
  }
}