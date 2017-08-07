package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.ArticleKey
import com.gravity.interests.jobs.intelligence.operations.ArticleAggData
import com.gravity.utilities.eventlogging.{FieldValueRegistry, FieldRegistry}
import com.gravity.utilities.grvfields.FieldConverter

/**
  * Created by jengelman14 on 7/14/16.
  */
trait ArticleAggDataConverter {
  this: FieldConverters.type =>

  implicit object ArticleAggDataConverter extends FieldConverter[ArticleAggData] {
    val fields: FieldRegistry[ArticleAggData] = new FieldRegistry[ArticleAggData]("ArticleAggData")
      .registerLongField("key", 0, 0L, "Article Key for this data")
      .registerLongField("impressions", 1, 0L, "Number of article unit impressions")
      .registerLongField("impressionViews", 2, 0L, "Number of article unit impression views")
      .registerLongField("clicks", 3, 0L, "Number of clicks")
      .registerLongField("impressionDiscards", 4, 0L, "Number of discarded article unit impressions")
      .registerLongField("impressionViewDiscards", 5, 0L, "Number of discarded unit impression views")
      .registerLongField("clickDiscards", 6, 0L, "Number of discarded clicks")

    def fromValueRegistry(vals: FieldValueRegistry): ArticleAggData = {
      ArticleAggData(
        ArticleKey(vals.getValue[Long](0)),
        vals.getValue[Long](1),
        vals.getValue[Long](2),
        vals.getValue[Long](3),
        vals.getValue[Long](4),
        vals.getValue[Long](5),
        vals.getValue[Long](6)
      )
    }

    def toValueRegistry(o: ArticleAggData): FieldValueRegistry = {
      import o._

      new FieldValueRegistry(fields)
        .registerFieldValue(0, key.articleId)
        .registerFieldValue(1, impressions)
        .registerFieldValue(2, impressionViews)
        .registerFieldValue(3, clicks)
        .registerFieldValue(4, impressionDiscards)
        .registerFieldValue(5, impressionViewDiscards)
        .registerFieldValue(6, clickDiscards)
    }
  }
}
