package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.ArticleKey
import com.gravity.interests.jobs.intelligence.operations.{AlgoSettingsData, ArticleRecoData}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime
import org.joda.time.DateTime

trait ArticleRecoDataConverter {
  this: FieldConverters.type =>

  implicit object ArticleRecoDataConverter extends FieldConverter[ArticleRecoData] {
    val fields: FieldRegistry[ArticleRecoData] = new FieldRegistry[ArticleRecoData]("ArticleRecoData", version = 1)
      .registerLongField("key", 0, 0L, "Article Key for this link")
      .registerUnencodedStringField("rawUrl", 1)
      .registerLongField("recoGenerationDate", 2, grvtime.epochDateTime.getMillis)
      .registerIntField("placementId", 3, -1)
      .registerLongField("sitePlacementId", 4, -1L)
      .registerUnencodedStringField("why", 5)

      .registerIntField("recoAlgo", 6, -1)
      .registerIntField("currentAlgo", 7, -1)
      .registerIntField("recoBucket", 8, -1)
      .registerIntField("currentBucket", 9, -1)
      .registerLongField("recommenderId", 10, -1L)

      .registerUnencodedStringField("sponseeGuid", 11)
      .registerUnencodedStringField("sponsorGuid", 12)
      .registerUnencodedStringField("sponsorPoolGuid", 13)
      .registerUnencodedStringField("auctionId", 14)

      .registerUnencodedStringField("campaignKey", 15)
      .registerLongField("costPerClick", 16, 0L)
      .registerIntField("campaignType", 17, 0, "0 if organic, 1 is sponsored, -1 if unknown")
      .registerIntField("displayIndex", 18, -1, "where the article was displayed in the reco")
      .registerLongField("contentGroupId", 19, -1L)
      .registerUnencodedStringField("sourceKey", 20, emptyString, "associated with the contentGroupId")
      .registerSeqField("algoSettingsData", 21, Seq.empty[AlgoSettingsData])
      .registerIntField("articleSlotsIndex", 22, -1)
      .registerUnencodedStringField("exchangeGuid", 23, minVersion = 1)

    def fromValueRegistry(vals: FieldValueRegistry): ArticleRecoData = {
      ArticleRecoData(
        ArticleKey(vals.getValue[Long](0)), vals.getValue[String](1), vals.getValue[Long](2), vals.getValue[Int](3), vals.getValue[Long](4), vals.getValue[String](5),
        vals.getValue[Int](6), vals.getValue[Int](7), vals.getValue[Int](8), vals.getValue[Int](9), vals.getValue[Long](10),
        vals.getValue[String](11), vals.getValue[String](12), vals.getValue[String](13), vals.getValue[String](14),
        vals.getValue[String](15), vals.getValue[Long](16), vals.getValue[Int](17), vals.getValue[Int](18), vals.getValue[Long](19), vals.getValue[String](20),
        vals.getValue[Seq[AlgoSettingsData]](21), vals.getValue[Int](22), vals.getValue[String](23))
    }


    def toValueRegistry(o: ArticleRecoData): FieldValueRegistry = {
      import o._

      new FieldValueRegistry(fields, version = 1)
        .registerFieldValue(0, key.articleId)
        .registerFieldValue(1, rawUrl)
        .registerFieldValue(2, recoGenerationDate)
        .registerFieldValue(3, placementId)
        .registerFieldValue(4, sitePlacementId)
        .registerFieldValue(5, why)

        .registerFieldValue(6, recoAlgo)
        .registerFieldValue(7, currentAlgo)
        .registerFieldValue(8, recoBucket)
        .registerFieldValue(9, currentBucket)
        .registerFieldValue(10, recommenderId)

        .registerFieldValue(11, sponseeGuid)
        .registerFieldValue(12, sponsorGuid)
        .registerFieldValue(13, sponsorPoolGuid)
        .registerFieldValue(14, auctionId)

        .registerFieldValue(15, campaignKey)
        .registerFieldValue(16, costPerClick)
        .registerFieldValue(17, campaignType)
        .registerFieldValue(18, displayIndex)
        .registerFieldValue(19, contentGroupId)
        .registerFieldValue(20, sourceKey)
        .registerFieldValue(21, algoSettingsData)
        .registerFieldValue(22, articleSlotsIndex)
        .registerFieldValue(23, exchangeGuid)
    }
  }
}