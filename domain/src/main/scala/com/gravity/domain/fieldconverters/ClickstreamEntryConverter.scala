package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.user.ClickstreamEntry
import com.gravity.interests.jobs.intelligence.{ArticleKey, ClickStreamKey, ClickType}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.time.DateHour

trait ClickstreamEntryConverter {
  this: FieldConverters.type =>

  implicit object ClickstreamEntryConverter extends FieldConverter[ClickstreamEntry] {
    val fields: FieldRegistry[ClickstreamEntry] = new FieldRegistry[ClickstreamEntry]("ClickStreamKey")
      .registerLongField("hour", 0, DateHour.epoch.getMillis)
      .registerIntField("clickType", 1, ClickType.empty.id)
      .registerLongField("articleKey", 2, 0L, "Article Key for this link")
      .registerLongField("count", 3, 0L, "Count of event type")


    def fromValueRegistry(vals: FieldValueRegistry): ClickstreamEntry = {
      ClickstreamEntry(
        ClickStreamKey(
          DateHour(vals.getValue[Long](0)),
          ClickType.getOrDefault(vals.getValue[Int](1)),
          ArticleKey(vals.getValue[Long](2))
        ),
        vals.getValue[Long](3)
      )
    }


    def toValueRegistry(o: ClickstreamEntry): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, key.hour.getMillis)
        .registerFieldValue(1, key.clickType.id)
        .registerFieldValue(2, key.articleKey.articleId)
        .registerFieldValue(3, count)
    }
  }
}