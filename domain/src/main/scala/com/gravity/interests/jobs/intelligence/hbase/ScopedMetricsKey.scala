package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.interests.jobs.intelligence.operations.recommendations.model.ImpressionFailedEvent
import com.gravity.interests.jobs.intelligence.{RecommendationMetrics, RecommendationMetricCountBy}
import com.gravity.utilities.time.{DateMinute, DateHour}
import com.gravity.interests.jobs.intelligence.operations.{ImpressionViewedEvent, ImpressionEvent, ClickEvent}
import com.gravity.utilities.grvcoll

/**
 * A metrics key that represents metrics aggregated against something else.  For example, Articles by Campaign, Articles by Section, etc.
 * The first component of the key is therefore the target of the metrics.  So if the first component is a SectionKey, then it answers the question of:
 * What Sections had what metrics?
 * @param dateTimeMs
 * @param countBy
 */
case class ScopedMetricsKey(   dateTimeMs: Long,
                               countBy: Byte
                               )  {

  def scopedMetrics(value: Long): RecommendationMetrics = {
    recoMetrics(value)
  }

  def dateHour: DateHour = DateHour(dateTimeMs)

  def dateMinute: DateMinute = DateMinute(dateTimeMs)
  
  def recoMetrics(value: Long): RecommendationMetrics = countBy match {
    case RecommendationMetricCountBy.click => RecommendationMetrics(0l, value, 0l)
    case RecommendationMetricCountBy.articleImpression => RecommendationMetrics(value, 0l, 0l)
    case RecommendationMetricCountBy.unitImpression => RecommendationMetrics(0l, 0l, value)
    case RecommendationMetricCountBy.unitImpressionViewed => RecommendationMetrics(0l, 0l, unitImpressionsViewed = value)
    case RecommendationMetricCountBy.conversion => RecommendationMetrics.empty
    case _ => RecommendationMetrics.empty
  }

  def rollUp(bucket: ScopedMetricsBucket.Value): ScopedMetricsKey = {

    bucket match {
      case ScopedMetricsBucket.minutely => rollUp(ScopedMetricsKey.toMinutely)
      case ScopedMetricsBucket.hourly => rollUp(ScopedMetricsKey.toHourly)
      case ScopedMetricsBucket.daily => rollUp(ScopedMetricsKey.toDaily)
      case ScopedMetricsBucket.monthly => rollUp(ScopedMetricsKey.toMonthly)
    }
  }

  def rollUp(rollUpMethod: ScopedMetricsKey => ScopedMetricsKey): ScopedMetricsKey = {

    rollUpMethod(this)
  }
}


object ScopedMetricsKey {
  def partialByStartDate(date: DateHour): ScopedMetricsKey = ScopedMetricsKey(date.getMillis, RecommendationMetricCountBy.articleImpression)

  def partialByStartDate(date: DateMinute): ScopedMetricsKey = ScopedMetricsKey(date.getMillis, RecommendationMetricCountBy.articleImpression)

  def cpcKeyFromClickEvent(event: ClickEvent): ScopedMetricsKey = {
    ScopedMetricsKey(DateMinute(event.getClickDate).getMillis, RecommendationMetricCountBy.costPerClick)
  }

  def fromClickEvent(event: ClickEvent): ScopedMetricsKey = {
    ScopedMetricsKey(DateMinute(event.getClickDate).getMillis, RecommendationMetricCountBy.click)
  }

  def fromImpressionViewed(event:ImpressionViewedEvent): ScopedMetricsKey = {
    ScopedMetricsKey(DateMinute(event.date).getMillis, RecommendationMetricCountBy.unitImpressionViewed)
  }

  def fromImpressionEvent(event:ImpressionEvent): ScopedMetricsKey = {
    ScopedMetricsKey(DateMinute(event.getDate).getMillis, RecommendationMetricCountBy.unitImpression)
  }

  def fromImpressionFailedEvent(event:ImpressionFailedEvent): ScopedMetricsKey = {
    ScopedMetricsKey(DateMinute(event.date).getMillis, RecommendationMetricCountBy.unitImpressionFailed)
  }

  val toMinutely: (ScopedMetricsKey) => ScopedMetricsKey = (sm: ScopedMetricsKey) => ScopedMetricsKey(sm.dateMinute.getMillis, sm.countBy)
  val toHourly: (ScopedMetricsKey) => ScopedMetricsKey = (sm: ScopedMetricsKey) => ScopedMetricsKey(DateHour(sm.dateTimeMs).getMillis, sm.countBy)
  val toDaily: (ScopedMetricsKey) => ScopedMetricsKey = (sm: ScopedMetricsKey) => ScopedMetricsKey(DateHour(sm.dateTimeMs).asMidnightHour.getMillis, sm.countBy)
  val toMonthly: (ScopedMetricsKey) => ScopedMetricsKey = (sm: ScopedMetricsKey) => ScopedMetricsKey(DateHour(sm.dateTimeMs).asMonthHour.getMillis, sm.countBy)

  /**
   * Preserves type inferencing for the grouper, thus allowing a terser syntax when calling, e.g. blah.groupMapgregate2(data)(_.dateHour)
   * @param map
   * @param grouper
   * @tparam G
   * @return
   */
  def groupMapgregate2[G](map: scala.collection.Map[ScopedMetricsKey,Long])(grouper:((ScopedMetricsKey,Long))=>G) : Map[G, RecommendationMetrics] = groupMapgregate(map.toSeq,grouper)

  def groupSequence[G](seq: scala.collection.Map[ScopedMetricsKey,Long])(grouper:((ScopedMetricsKey,Long))=>G) : Seq[(G, RecommendationMetrics)] = {
    grvcoll.groupAndFold(RecommendationMetrics.empty)(seq)(grouper)({
      case (rm: RecommendationMetrics, kv: (ScopedMetricsKey, Long)) =>
        rm + kv._1.scopedMetrics(kv._2)
    }).toSeq
  }

  def groupMapgregate[G](map: Map[ScopedMetricsKey, Long], grouper: ((ScopedMetricsKey, Long)) => G): Map[G, RecommendationMetrics] = groupMapgregate(map.toSeq, grouper)

  def groupMapgregate[G](seq: Seq[(ScopedMetricsKey, Long)], grouper: ((ScopedMetricsKey, Long)) => G): Map[G, RecommendationMetrics] = {
    grvcoll.groupAndFold(RecommendationMetrics.empty)(seq)(grouper)({
      case (rm: RecommendationMetrics, kv: (ScopedMetricsKey, Long)) =>
        rm + kv._1.scopedMetrics(kv._2)
    }).toMap
  }
}
