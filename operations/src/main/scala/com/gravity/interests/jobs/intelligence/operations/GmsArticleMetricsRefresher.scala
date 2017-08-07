package com.gravity.interests.jobs.intelligence.operations

import com.gravity.data.reporting.GmsElasticSearchService
import com.gravity.domain.gms.{GmsArticleMetrics, GmsArticleMetricsKey, GmsMetricsForIndex}
import com.gravity.interests.jobs.intelligence.RecommendationMetricCountBy
import com.gravity.interests.jobs.intelligence.hbase.{ScopedFromToKey, ScopedMetricsBuckets, ScopedMetricsKey}
import com.gravity.utilities.grvtime
import com.gravity.utilities.cache.SingletonCache
import com.gravity.utilities.cache.metrics.impl.ScopedMetricsSource
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._

import scalaz.{Failure, NonEmptyList, Show, Success, Validation, ValidationNel}
import scalaz.syntax.validation._
import com.gravity.utilities.grvz._

import scalaz.syntax.std.option._
import scalaz.syntax.apply._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.semigroup._
import scalaz.std.iterable._
import scalaz.syntax.foldable._
import scalaz.syntax.show._
import scalaz.syntax.std.boolean._

/**
  * Created by robbie on 10/28/2016.
  *            _ 
  *          /  \
  *         / ..|\
  *        (_\  |_)
  *        /  \@'
  *       /     \
  *   _  /  \   |
  *  \\/  \  | _\
  *   \   /_ || \\_
  *    \____)|_) \_)
  *
  */
object GmsArticleMetricsRefresher extends ScopedMetricsSource {
  import com.gravity.logging.Logging._

  private val recentKeys = GmsArticleMetricsRecentKeysCacher
  private lazy val esqs = GmsElasticSearchService()
  val cacheTTLseconds: Int = 120

  def touch(key: GmsArticleMetricsKey): Unit = {
    recentKeys.cache.putItem(key.keyString, key, cacheTTLseconds)
  }

  def refresh(): Unit = {
    val recentKeyMap = recentKeys.getAllValues.map(k => k.scopedFromToKey -> k).toMap
    val scopedToFromKeys = recentKeyMap.keySet

    if (scopedToFromKeys.isEmpty) return

    getFromHBase(scopedToFromKeys)(GmsService.scopedMetricsCacheQuery) match {
      case Success(metricsMap) =>
        if (metricsMap.isEmpty) return

        val minutelyToBeGreaterThan = grvtime.currentMinute.minusMinutes(31)

        def buildMetrics(fromToKey: ScopedFromToKey, buckets: ScopedMetricsBuckets): GmsMetricsForIndex = {
          var (totalClicks, totalImps, thirtyMinuteClicks, thirtyMinuteImps) = (0L, 0L, 0L, 0L)

          buckets.minutely.map.filterKeys { k =>
              k.dateMinute.isAfter(minutelyToBeGreaterThan) && isClickOrImp(k)
          } foreach {
            case (key: ScopedMetricsKey, clicks: Long) if isClick(key) =>
              thirtyMinuteClicks += clicks

            case (key: ScopedMetricsKey, imps: Long) if isImp(key) =>
              thirtyMinuteImps += imps
          }

          buckets.monthly.map.filterKeys(isClickOrImp).foreach {
            case (key: ScopedMetricsKey, clicks: Long) if isClick(key) =>
              totalClicks += clicks

            case (key: ScopedMetricsKey, imps: Long) if isImp(key) =>
              totalImps += imps
          }

          GmsMetricsForIndex(totalClicks, totalImps, thirtyMinuteClicks, thirtyMinuteImps)
        }

        val itemsToIndex = for {
          (fromToKey, buckets) <- metricsMap
          gmsMetricsKey <- recentKeyMap.get(fromToKey)
        } yield {
          GmsArticleMetrics(gmsMetricsKey, buildMetrics(fromToKey, buckets))
        }

        esqs.updateGmsArticleMetrics(itemsToIndex)

      case Failure(fails) =>
        warn(fails, "Failed to refresh GMS Article Metrics!")
    }
  }

  private def isClick(key: ScopedMetricsKey): Boolean = key.countBy match {
    case RecommendationMetricCountBy.click => true
    case _ => false
  }

  private def isImp(key: ScopedMetricsKey): Boolean = key.countBy match {
    case RecommendationMetricCountBy.unitImpressionViewed => true
    case _ => false
  }

  private def isClickOrImp(key: ScopedMetricsKey): Boolean = key.countBy match {
    case RecommendationMetricCountBy.click => true
    case RecommendationMetricCountBy.unitImpressionViewed => true
    case _ => false
  }
}

object GmsArticleMetricsRecentKeysCacher extends SingletonCache[GmsArticleMetricsKey] {
  def cacheName: String = "gms-article-metrics-recent-keys"

  def getAllValues: Seq[GmsArticleMetricsKey] = {
    getKeysNoDuplicateCheck.flatMap(cache.getItem)
  }
}
