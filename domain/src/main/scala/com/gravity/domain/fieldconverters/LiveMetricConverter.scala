package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.ScopedMetrics
import com.gravity.interests.jobs.intelligence.hbase.{ScopedFromToKey, ScopedKey}
import com.gravity.interests.jobs.intelligence.operations.metric.LiveMetric
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import scalaz.{Failure, Success}

/**
  * Created by agrealish14 on 10/10/16.
  */
trait LiveMetricConverter {
  this: FieldConverters.type =>

  implicit object LiveMetricConverter extends FieldConverter[LiveMetric] {

    val fields: FieldRegistry[LiveMetric] = new FieldRegistry[LiveMetric]("LiveMetric", version = 1)
      .registerStringField("from", 0)
      .registerStringField("to", 1)
      .registerLongField("impressions", 2)
      .registerLongField("clicks", 3)
      .registerLongField("views", 4)
      .registerLongField("spend", 5)
      .registerLongField("failedImpressions", 6)

    def toValueRegistry(o: LiveMetric): FieldValueRegistry = {
      new FieldValueRegistry(fields, version = 1)
        .registerFieldValue(0, o.key.from.keyString)
        .registerFieldValue(1, o.key.to.keyString)
        .registerFieldValue(2, o.metrics.impressions)
        .registerFieldValue(3, o.metrics.clicks)
        .registerFieldValue(4, o.metrics.views)
        .registerFieldValue(5, o.metrics.spend)
        .registerFieldValue(6, o.metrics.failedImpressions)
    }

    def fromValueRegistry(vals: FieldValueRegistry): LiveMetric = {

      LiveMetric(
        ScopedFromToKey(
          ScopedKey.validateKeyString(vals.getValue[String](0)) match {
            case Success(key) => key
            case Failure(fails) => throw new Exception(fails.toString)
          },
          ScopedKey.validateKeyString(vals.getValue[String](1)) match {
            case Success(key) => key
            case Failure(fails) => throw new Exception(fails.toString)
          }

        ),
        ScopedMetrics(
          vals.getValue[Long](2),
          vals.getValue[Long](3),
          vals.getValue[Long](4),
          vals.getValue[Long](5),
          vals.getValue[Long](6)
        )
      )
    }
  }
}

