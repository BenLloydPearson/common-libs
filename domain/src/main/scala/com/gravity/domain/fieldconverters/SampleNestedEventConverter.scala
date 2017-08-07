package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.{ArticleRecoData, SampleNestedEvent}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime
import org.joda.time.DateTime

trait SampleNestedEventConverter {
  this: FieldConverters.type =>

  implicit object SampleNestedEventConverter extends FieldConverter[SampleNestedEvent] {
    val fields: FieldRegistry[SampleNestedEvent] = new FieldRegistry[SampleNestedEvent]("SampleNestedEvent")
      .registerStringField("sneString", 0, emptyString, "sample string field")
      .registerIntField("sneInt", 1, -1, "sample integer field")
      .registerLongField("sneLong", 2, -1L, "sample long field")
      .registerFloatField("sneFloat", 3, -1.0F, "sample float field")
      .registerDoubleField("sneDouble", 4, -1.0, "sample double field")
      .registerBooleanField("sneBoolean", 5, false, "sample boolean field")
      .registerStringSeqField("sneListString", 6, Seq.empty[String], "sample list string field")
      .registerIntSeqField("sneListInt", 7, Seq.empty[Int], "sample list int field")
      .registerLongSeqField("sneListLong", 8, Seq.empty[Long], "sample list long field")
      .registerFloatSeqField("sneListFloat", 9, Seq.empty[Float], "sample list float field")
      .registerDoubleSeqField("sneListDouble", 10, Seq.empty[Double], "sample list double field")
      .registerBooleanSeqField("sneListBoolean", 11, Seq.empty[Boolean], "sample list boolean field")
      .registerDateTimeField("sneDateTime", 12, grvtime.epochDateTime, "sample date time field")
      .registerDateTimeSeqField("sneListDateTime", 13, Seq.empty[DateTime], "sample list date time field")
      .registerField[ArticleRecoData]("sneNestEvent", 14, ArticleRecoData.empty, "sample nested event")

    def fromValueRegistry(reg: FieldValueRegistry): SampleNestedEvent = new SampleNestedEvent(
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
      reg.getValue[ArticleRecoData](14)
    )

    def toValueRegistry(o: SampleNestedEvent): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, sneString)
        .registerFieldValue(1, sneInt)
        .registerFieldValue(2, sneLong)
        .registerFieldValue(3, sneFloat)
        .registerFieldValue(4, sneDouble)
        .registerFieldValue(5, sneBoolean)
        .registerFieldValue(6, sneListString)
        .registerFieldValue(7, sneListInt)
        .registerFieldValue(8, sneListLong)
        .registerFieldValue(9, sneListFloat)
        .registerFieldValue(10, sneListDouble)
        .registerFieldValue(11, sneListBoolean)
        .registerFieldValue(12, sneDateTime)
        .registerFieldValue(13, sneListDateTime)
        .registerFieldValue(14, sneNestEvent)
    }
  }
}