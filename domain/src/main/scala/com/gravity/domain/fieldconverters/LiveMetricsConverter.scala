package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.hbase.ScopedMetricsKey
import com.gravity.interests.jobs.intelligence.operations.metric.LiveMetrics
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

/**
 * Created by agrealish14 on 10/13/16.
 */
trait LiveMetricsConverter {
  this: FieldConverters.type =>

  implicit object LiveMetricsConverter extends FieldConverter[LiveMetrics] {

    val fields: FieldRegistry[LiveMetrics] = new FieldRegistry[LiveMetrics]("LiveMetrics", version = 1)
      .registerLongField("dateTimeMs", 0)
      .registerIntField("countBy", 1)
      .registerLongField("count", 2)

    def toValueRegistry(o: LiveMetrics): FieldValueRegistry = {
      new FieldValueRegistry(fields, version = 1)
        .registerFieldValue(0, o.metricKey.dateTimeMs)
        .registerFieldValue(1, o.metricKey.countBy)
        .registerFieldValue(2, o.count)
    }

    def fromValueRegistry(vals: FieldValueRegistry): LiveMetrics = {
      LiveMetrics(
        ScopedMetricsKey(
          vals.getValue[Long](0),
          vals.getValue[Int](1).toByte
        ),
        vals.getValue[Long](2)
      )
    }
  }
}
