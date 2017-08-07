package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.interfaces.userfeedback.{UserFeedbackPresentation, UserFeedbackVariation}
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

trait ImpressionSlugFieldConverter {
  this: FieldConverters.type =>

  implicit object ImpressionSlugFieldConverter extends FieldConverter[ImpressionSlug] {
    val fields: FieldRegistry[ImpressionSlug] = new FieldRegistry[ImpressionSlug]("ImpressionSlug", version = 0)
      .registerIntField("impressionPurposeId", 0, ImpressionPurpose.defaultId)
      .registerIntField("recoBucket", 1, 0)
      .registerStringField("referrerImpressionHash", 2)
      .registerIntField("userFeedbackVariationId", 3, UserFeedbackVariation.none.id)
      .registerIntField("userFeedbackPresentationId", 4, UserFeedbackPresentation.none.id)
      .registerLongField("recoGenerationDateMillis", 5, 0)

    def toValueRegistry(o: ImpressionSlug): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields, version = 0)
        .registerFieldValue(0, impressionPurposeId)
        .registerFieldValue(1, recoBucket)
        .registerFieldValue(2, referrerImpressionHash)
        .registerFieldValue(3, userFeedbackVariationId)
        .registerFieldValue(4, userFeedbackPresentationId)
        .registerFieldValue(5, recoGenerationDateMillis)
    }

    def fromValueRegistry(vals: FieldValueRegistry): ImpressionSlug = {
      val event = new ImpressionSlug(
        vals.getValue[Int](0),
        vals.getValue[Int](1),
        vals.getValue[String](2),
        vals.getValue[Int](3),
        vals.getValue[Int](4),
        vals.getValue[Long](5)
      )
      event
    }
  }
}
