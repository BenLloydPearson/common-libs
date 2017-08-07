package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.metric.{LiveMetricUpdate, LiveMetricUpdateBundle}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

/**
 * Created by agrealish14 on 10/12/16.
 */
trait LiveMetricUpdateConverter {
  this: FieldConverters.type =>

  implicit object LiveMetricUpdateConverter extends FieldConverter[LiveMetricUpdate] {

    val fields: FieldRegistry[LiveMetricUpdate] = new FieldRegistry[LiveMetricUpdate]("LiveMetricUpdate", version = 1)
      .registerSeqField[LiveMetricUpdateBundle]("updates", 0, Seq.empty[LiveMetricUpdateBundle])

    def toValueRegistry(o: LiveMetricUpdate): FieldValueRegistry = {
      new FieldValueRegistry(fields, version = 1)
        .registerFieldValue(0, o.updates)
    }

    def fromValueRegistry(vals: FieldValueRegistry): LiveMetricUpdate = {
      LiveMetricUpdate(
        vals.getValue[Seq[LiveMetricUpdateBundle]](0)
      )
    }
  }
}
