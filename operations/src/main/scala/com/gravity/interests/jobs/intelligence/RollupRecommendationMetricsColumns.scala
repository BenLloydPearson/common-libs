package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.utilities.grvtime
import com.gravity.utilities.time.GrvDateMidnight

import scala.collection.Map


trait RollupRecommendationMetricsColumns[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  val rollupRecommendationMetrics: this.Fam[RollupRecommendationMetricKey, Long] = family[RollupRecommendationMetricKey, Long]("rrm", rowTtlInSeconds = 2592000, compressed = true)
  val rollupRecommendationDailyMetrics: this.Fam[RollupRecommendationMetricKey, Long] = family[RollupRecommendationMetricKey, Long]("drrm", rowTtlInSeconds = 2592000 * 6, compressed = true)
  val rollupRecommendationMonthlyMetrics: this.Fam[RollupRecommendationMetricKey, Long] = family[RollupRecommendationMetricKey, Long]("mrrm", rowTtlInSeconds = Int.MaxValue, compressed = true)
}

trait RollupRecommendationMetricsRow[T <: HbaseTable[T, R, RR] with RollupRecommendationMetricsColumns[T, R], R, RR <: HRow[T, R]] {
  this: HRow[T, R] =>

  type RollupRecommendationMetricsFilter = (RollupRecommendationMetricKey) => Boolean

  /**
   * Given a way to filter metrics keys, will aggregate the resulting RecommendationMetrics.
   * @param filter If true, keep the metrics.
   * @return The aggregated metrics.
   */
  def rollupRecommendationMetricsBy(filter: RollupRecommendationMetricsFilter): RecommendationMetrics = {
    rollupRecommendationMetrics.aggregate(RecommendationMetrics.empty)((total, kv) => {
      if (filter(kv._1))
        total + kv._1.recoMetrics(kv._2)
      else
        total
    }, _ + _)
  }


  lazy val rollupRecommendationMetrics: Map[RollupRecommendationMetricKey, Long] = family(_.rollupRecommendationMetrics)
  lazy val rollupRecommendationDailyMetrics: Map[RollupRecommendationMetricKey, Long] = family(_.rollupRecommendationDailyMetrics)
  lazy val rollupRecommendationMonthlyMetrics: Map[RollupRecommendationMetricKey, Long] = family(_.rollupRecommendationMonthlyMetrics)

  lazy val rollupRecommendationMetricsByDay: Map[GrvDateMidnight, RecommendationMetrics] = RollupRecommendationMetricKey.groupMapgregate(rollupRecommendationMetrics, (kv: (RollupRecommendationMetricKey, Long)) => kv._1.dateHour.toGrvDateMidnight)

  lazy val rollupRecommendationMetricsByMonth: Map[GrvDateMidnight, RecommendationMetrics] = RollupRecommendationMetricKey.groupMapgregate(rollupRecommendationMetrics, (kv: (RollupRecommendationMetricKey, Long)) => grvtime.dateToMonth(kv._1.dateHour))

  lazy val rollupMetricsNoSiteGroupedMap: Map[RollupRecommendationMetricsNoSiteGroupByHourKey, RecommendationMetrics] = RollupRecommendationMetricKey.groupMapgregate(rollupRecommendationMetrics, (kv: (RollupRecommendationMetricKey, Long)) => RollupRecommendationMetricsNoSiteGroupByHourKey(kv._1.dateHour, kv._1.placementId, kv._1.bucketId))
  lazy val rollupMetricsNoSiteGroupedAndSorted: Seq[(RollupRecommendationMetricsNoSiteGroupByHourKey, RecommendationMetrics)] = rollupMetricsNoSiteGroupedMap.toSeq.sorted(RollupRecommendationMetricsRow.rollupMetricsNoSiteGroupedOrdering)

  lazy val rollupMetricsNoSiteGroupedByDayMap: Map[RollupRecommendationMetricsNoSiteGroupByDayKey, RecommendationMetrics] = RollupRecommendationMetricKey.groupMapgregate(rollupRecommendationMetrics, (kv: (RollupRecommendationMetricKey, Long)) => RollupRecommendationMetricsNoSiteGroupByDayKey(kv._1.dateHour.toGrvDateMidnight, kv._1.placementId, kv._1.bucketId))
  lazy val rollupMetricsNoSiteGroupedByDayAndSorted: Seq[(RollupRecommendationMetricsNoSiteGroupByDayKey, RecommendationMetrics)] = rollupMetricsNoSiteGroupedByDayMap.toSeq.sorted(RollupRecommendationMetricsRow.rollupMetricsNoSiteGroupedByDayOrdering)
}

object RollupRecommendationMetricsRow {
  val rollupMetricsNoSiteGroupedOrdering:Ordering[(RollupRecommendationMetricsNoSiteGroupByHourKey,_)] = new Ordering[(RollupRecommendationMetricsNoSiteGroupByHourKey, _)] {
    def compare(x: (RollupRecommendationMetricsNoSiteGroupByHourKey, _), y: (RollupRecommendationMetricsNoSiteGroupByHourKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.hour.isAfter(k2.hour)) return -1
      if (k2.hour.isAfter(k1.hour)) return 1

      if (k1.placementId < k2.placementId) return -1
      if (k2.placementId > k2.placementId) return 1

      if (k1.bucket < k2.bucket) return -1
      if (k2.bucket < k1.bucket) return 1

      0
    }
  }

  val rollupMetricsNoSiteGroupedByDayOrdering:Ordering[(RollupRecommendationMetricsNoSiteGroupByDayKey, _)] = new Ordering[(RollupRecommendationMetricsNoSiteGroupByDayKey, _)] {
    def compare(x: (RollupRecommendationMetricsNoSiteGroupByDayKey, _), y: (RollupRecommendationMetricsNoSiteGroupByDayKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.dateMidnight.isAfter(k2.dateMidnight)) return -1
      if (k2.dateMidnight.isAfter(k1.dateMidnight)) return 1

      if (k1.placementId < k2.placementId) return -1
      if (k2.placementId > k2.placementId) return 1

      if (k1.bucket < k2.bucket) return -1
      if (k2.bucket < k1.bucket) return 1

      0
    }
  }
}



