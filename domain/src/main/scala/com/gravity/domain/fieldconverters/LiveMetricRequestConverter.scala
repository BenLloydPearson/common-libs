package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.hbase.ScopedFromToKey
import com.gravity.interests.jobs.intelligence.operations.metric.LiveMetricRequest
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

/**
 * Created by agrealish14 on 10/10/16.
 */
trait LiveMetricRequestConverter {
  this: FieldConverters.type =>

  implicit object LiveMetricRequestConverter extends FieldConverter[LiveMetricRequest] {

    val fields: FieldRegistry[LiveMetricRequest] = new FieldRegistry[LiveMetricRequest]("LiveMetricRequest", version = 1)
      .registerSeqField[ScopedFromToKey]("keys", 0, Seq.empty[ScopedFromToKey])
      .registerIntField("bucket", 1)
      .registerIntField("impressionsThreshold", 2)
      .registerIntField("hoursThreshold", 3)

    def toValueRegistry(o: LiveMetricRequest): FieldValueRegistry = {
      new FieldValueRegistry(fields, version = 1)
        .registerFieldValue(0, o.keys)
        .registerFieldValue(1, o.bucket)
        .registerFieldValue(2, o.impressionsThreshold)
        .registerFieldValue(3, o.hoursThreshold)
    }

    def fromValueRegistry(vals: FieldValueRegistry): LiveMetricRequest = {

      LiveMetricRequest(
        vals.getValue[Seq[ScopedFromToKey]](0),
        vals.getValue[Int](1),
        vals.getValue[Int](2),
        vals.getValue[Int](3)
      )
    }
  }
}

