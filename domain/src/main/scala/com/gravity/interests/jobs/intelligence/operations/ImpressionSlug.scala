package com.gravity.interests.jobs.intelligence.operations

import com.gravity.domain.FieldConverters
import com.gravity.interests.interfaces.userfeedback.{UserFeedbackPresentation, UserFeedbackVariation}
import com.gravity.interests.jobs.intelligence.operations.ImpressionSlug.EncodedImpressionSlug
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.{ArchiveUtils, grvfields}
import play.api.libs.json.{JsString, Writes}

import scalaz._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/**
  * Collects a bunch of data for ImpressionEvent that concern Gravity but not Gravity clients; these data can be
  * passed around as a single, portable somehow encoded slug.
  */
case class ImpressionSlug(impressionPurposeId: Int, recoBucket: Int, referrerImpressionHash: String,
                          userFeedbackVariationId: Int, userFeedbackPresentationId: Int,
                          recoGenerationDateMillis: Long) {
  def encoded: EncodedImpressionSlug = {
    val fieldStr = grvfields.toDelimitedFieldString(this)(FieldConverters.ImpressionSlugFieldConverter)
    ArchiveUtils.compressString(fieldStr)
  }
}

object ImpressionSlug {
  type EncodedImpressionSlug = String

  def fromEvent(event: ImpressionEvent): ImpressionSlug = ImpressionSlug(event.more.impressionPurpose,
    event.getBucketId, event.more.referrerImpressionHash, event.more.userFeedbackVariation,
    event.more.userFeedbackPresentation, event.articleHead.recoGenerationDate)

  def fromEncoded(encodedSlug: EncodedImpressionSlug): ValidationNel[FailureResult, ImpressionSlug] = {
    val decompressed = ArchiveUtils.decompressString(encodedSlug)
    val converter = FieldConverters.ImpressionSlugFieldConverter
    grvfields.getInstanceFromString[ImpressionSlug](decompressed)(converter)
  }

  val encodedJsonWriter = Writes[ImpressionSlug](slug => JsString(slug.encoded))

  val example = ImpressionSlug(ImpressionPurpose.recirc.id, 1, "", UserFeedbackVariation.none.id,
    UserFeedbackPresentation.none.id, System.currentTimeMillis())
}