package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.interests.jobs.intelligence.{RecommendationMetrics, RecommendationMetricCountBy}
import com.gravity.utilities.time.DateHour
import com.gravity.utilities.grvcoll

/**
 * A metrics key that represents metrics aggregated against something else.  For example, Articles by Campaign, Articles by Section, etc.
 * The first component of the key is therefore the target of the metrics.  So if the first component is a SectionKey, then it answers the question of:
 * What Sections had what metrics?
 */
case class ScopedToMetricsKey(
                             to: ScopedKey,
                             dateHourMs: Long,
                             bucketId: Int,
                             placementId: Int,
                             algoId: Int,
                             countBy: Byte
                             ) extends MetricsKey {

  def scopedMetrics(value: Long): RecommendationMetrics = {
    recoMetrics(value)
  }

  def dateHour: DateHour = DateHour(dateHourMs)

  def recoMetrics(value: Long): RecommendationMetrics = countBy match {
    case RecommendationMetricCountBy.click => RecommendationMetrics(0l, value, 0l)
    case RecommendationMetricCountBy.articleImpression => RecommendationMetrics(value, 0l, 0l)
    case RecommendationMetricCountBy.unitImpression => RecommendationMetrics(0l, 0l, value)
    case _ => RecommendationMetrics.empty
  }



}


object ScopedToMetricsKey {
  def partialByStartDate(key: ScopedKey, date: DateHour): ScopedToMetricsKey = ScopedToMetricsKey(key, date.getMillis, 0, 0, 0, RecommendationMetricCountBy.articleImpression)

  def groupMapgregate[G](map: Map[ScopedToMetricsKey, Long], grouper: ((ScopedToMetricsKey, Long)) => G): Map[G, RecommendationMetrics] = groupMapgregate(map.toSeq, grouper)

  def groupMapgregate[G](seq: Seq[(ScopedToMetricsKey, Long)], grouper: ((ScopedToMetricsKey, Long)) => G): Map[G, RecommendationMetrics] = {
    grvcoll.groupAndFold(RecommendationMetrics.empty)(seq)(grouper)({
      case (rm: RecommendationMetrics, kv: (ScopedToMetricsKey, Long)) =>
        rm + kv._1.scopedMetrics(kv._2)
    }).toMap
  }
}
