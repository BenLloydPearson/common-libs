package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.{ProbabilityDistribution, ValueProbability}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

trait ProbabilityDistributionConverter {
  this: FieldConverters.type =>

  implicit object ProbabilityDistributionConverter extends FieldConverter[ProbabilityDistribution] {
    override def toValueRegistry(o: ProbabilityDistribution): FieldValueRegistry = new FieldValueRegistry(fields)
      .registerFieldValue(0, o.valueProbabilityList.map(_.value))
      .registerFieldValue(1, o.valueProbabilityList.map(_.cumulativeProbability))

    override def fromValueRegistry(reg: FieldValueRegistry): ProbabilityDistribution = {
      val values = reg.getValue[Seq[Double]](0)
      val probabilities = reg.getValue[Seq[Int]](1)
      ProbabilityDistribution(values.zip(probabilities).map{case (v, p) => ValueProbability(v, p)})
    }

    override val fields: FieldRegistry[ProbabilityDistribution] = new FieldRegistry[ProbabilityDistribution]("ProbabilityDistribution", version = 0)
      .registerDoubleSeqField("valueProbabilityValues", 0).registerIntSeqField("cumulativeProbabilities", 1)
  }
}