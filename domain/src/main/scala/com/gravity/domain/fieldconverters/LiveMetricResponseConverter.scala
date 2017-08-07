package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.metric.{LiveMetric, LiveMetricResponse}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

/**
  * Created by agrealish14 on 10/10/16.
  */
trait LiveMetricResponseConverter {
  this: FieldConverters.type =>

  implicit object LiveMetricResponseConverter extends FieldConverter[LiveMetricResponse] {

   val fields: FieldRegistry[LiveMetricResponse] = new FieldRegistry[LiveMetricResponse]("LiveMetricResponse", version = 1)
     .registerSeqField[LiveMetric]("metrics", 0, Seq.empty[LiveMetric])

   def toValueRegistry(o: LiveMetricResponse): FieldValueRegistry = {
     new FieldValueRegistry(fields, version = 1)
       .registerFieldValue(0, o.metrics)
   }

   def fromValueRegistry(vals: FieldValueRegistry): LiveMetricResponse = {

     LiveMetricResponse(
       vals.getValue[Seq[LiveMetric]](0)
     )
   }
  }
}

