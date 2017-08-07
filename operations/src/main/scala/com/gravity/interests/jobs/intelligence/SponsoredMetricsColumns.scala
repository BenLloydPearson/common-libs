package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.operations.CampaignService
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.grvtime
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}

import scala.collection._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 10/9/12
 * Time: 11:20 AM
 */
trait SponsoredMetricsColumns[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>
  val sponsoredMetrics : this.Fam[SponsoredMetricsKey,Long] = family[SponsoredMetricsKey, Long]("spm", rowTtlInSeconds = 2592000, compressed = true)
  val sponsoredDailyMetrics : this.Fam[SponsoredMetricsKey,Long] = family[SponsoredMetricsKey, Long]("sdm", rowTtlInSeconds = 2592000 * 6, compressed = true)
  val sponsoredMonthlyMetrics : this.Fam[SponsoredMetricsKey,Long]= family[SponsoredMetricsKey, Long]("smm", rowTtlInSeconds = Int.MaxValue, compressed = true)
}


trait ArticleSponsoredMetricsColumns[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  val articleSponsoredMetrics: this.Fam[ArticleSponsoredMetricsKey, Long] = family[ArticleSponsoredMetricsKey, Long]("am", rowTtlInSeconds = 604800, compressed = true)
}

trait SponsoredMetricsRow[T <: HbaseTable[T, R, RR] with SponsoredMetricsColumns[T, R], R, RR <: HRow[T, R]] {
  this: HRow[T, R] =>

  implicit val checker: (CampaignKey) => Boolean = CampaignService.isOrganic

  lazy val sponsoredMetrics: Map[SponsoredMetricsKey, Long] = family(_.sponsoredMetrics)
  lazy val sponsoredDailyMetrics: Map[SponsoredMetricsKey, Long] = family(_.sponsoredDailyMetrics)
  lazy val sponsoredMonthlyMetrics: Map[SponsoredMetricsKey, Long] = family(_.sponsoredMonthlyMetrics)


  type SponsoredMetricsFilter = (SponsoredMetricsKey) => Boolean

  /**
   * Given a way to filter metrics keys, will aggregate the resulting RecommendationMetrics.
   * @param filter If true, keep the metrics.
   * @return The aggregated metrics.
   */
  def sponsoredMetricsBy(filter: SponsoredMetricsFilter): SponsoredRecommendationMetrics = {
    sponsoredMetrics.aggregate(SponsoredRecommendationMetrics.empty)((total, kv) => {
      if (filter(kv._1))
        total + kv._1.sponsoredMetrics(kv._2)(CampaignService.isOrganic)
      else
        total
    }, _ + _)
  }

  def sponsoredDailyMetricsBy(filter: SponsoredMetricsFilter): SponsoredRecommendationMetrics = {
    sponsoredDailyMetrics.aggregate(SponsoredRecommendationMetrics.empty)((total, kv) => {
      if (filter(kv._1))
        total + kv._1.sponsoredMetrics(kv._2)(CampaignService.isOrganic)
      else
        total
    }, _ + _)
  }


  def metricsForDay(day:GrvDateMidnight): Map[SponsoredMetricsKey, Long] = {
    sponsoredMetrics.filterKeys(k => day == k.dateHour.toGrvDateMidnight)
  }

  def metricsForBucket(bucket:Int): Map[SponsoredMetricsKey, Long] = {
    sponsoredMetrics.filterKeys(k => k.bucketId == bucket)
  }

  def clicksForBucket(bucket:Int): Long = {
    sponsoredMetrics.filterKeys(key => key.countBy == RecommendationMetricCountBy.click && key.bucketId == bucket).values.sum
  }

  def impressionsForBucket(bucket:Int): Long = {
    sponsoredMetrics.filterKeys(key => key.countBy == RecommendationMetricCountBy.unitImpression && key.bucketId == bucket).values.sum
  }


  def todaysMetrics: Map[SponsoredMetricsKey, Long] = metricsForDay(grvtime.currentDay)

  def thisMonthsSponsoredMetrics: SponsoredRecommendationMetrics = sponsoredMetricsByMonth.getOrElse(grvtime.currentMonth, SponsoredRecommendationMetrics.empty)
  def lastMonthsSponsoredMetrics: SponsoredRecommendationMetrics = sponsoredMetricsByMonth.getOrElse(grvtime.monthsAgo(1), SponsoredRecommendationMetrics.empty)
  def todaysSponsoredMetrics: SponsoredRecommendationMetrics = sponsoredMetricsByDay(checker).getOrElse(grvtime.currentDay, SponsoredRecommendationMetrics.empty)
  def yesterdaysSponsoredMetrics: SponsoredRecommendationMetrics = sponsoredMetricsByDay(checker).getOrElse(grvtime.currentDay.minusDays(1), SponsoredRecommendationMetrics.empty)
  def thisHoursMetrics: SponsoredRecommendationMetrics = sponsoredMetricsByHourWithMoney.getOrElse(grvtime.currentHour, SponsoredRecommendationMetrics.empty)
  def previousHoursMetrics: SponsoredRecommendationMetrics = sponsoredMetricsByHourWithMoney.getOrElse(grvtime.currentHour.minusHours(1), SponsoredRecommendationMetrics.empty)

  lazy val sponsoredMetricsByDay: ((CampaignKey) => Boolean) => Map[GrvDateMidnight, SponsoredRecommendationMetrics] = (isOrganicCampaignChecker: (CampaignKey) => Boolean) =>
    SponsoredMetricsKey.groupMapgregate(sponsoredMetrics, (kv: (SponsoredMetricsKey, Long)) => kv._1.dateHour.toGrvDateMidnight)(isOrganicCampaignChecker)
  lazy val sponsoredDailyMetricsByDay: ((CampaignKey) => Boolean) => Map[GrvDateMidnight, SponsoredRecommendationMetrics] = (isOrganicCampaignChecker: (CampaignKey) => Boolean) =>
    SponsoredMetricsKey.groupMapgregate(sponsoredDailyMetrics, (kv: (SponsoredMetricsKey, Long)) => kv._1.dateHour.toGrvDateMidnight)(isOrganicCampaignChecker)
  lazy val sponsoredMetricsBySitePlacementId: Map[Int, SponsoredRecommendationMetrics] = SponsoredMetricsKey.groupMapgregate(sponsoredMetrics, (kv: (SponsoredMetricsKey, Long)) => kv._1.sitePlacementId)
  lazy val sponsoredMetricsByMonth: Map[GrvDateMidnight, SponsoredRecommendationMetrics] = SponsoredMetricsKey.groupMapgregate(sponsoredMetrics, (kv: (SponsoredMetricsKey, Long)) => grvtime.dateToMonth(kv._1.dateHour))
  lazy val sponsoredMetricsByHourWithMoney: Map[DateHour, SponsoredRecommendationMetrics] = SponsoredMetricsKey.groupMapgregate(sponsoredMetrics, (kv: (SponsoredMetricsKey, Long)) => kv._1.dateHour)

  lazy val sponsoredDailyMetricsByDailyCampaign: Map[(GrvDateMidnight, CampaignKey), SponsoredRecommendationMetrics] = SponsoredMetricsKey.groupMapgregate(sponsoredDailyMetrics, (kv: (SponsoredMetricsKey, Long)) => (kv._1.dateHour.toGrvDateMidnight, kv._1.campaignKey))

  lazy val sponsoredMetricsGroupedByAlgosWithMoneyMap: Map[SponsoredMetricsGroupByHourKey, SponsoredRecommendationMetrics] = SponsoredMetricsKey.groupMapgregate(sponsoredMetrics, (kv: (SponsoredMetricsKey, Long)) => SponsoredMetricsGroupByHourKey(kv._1.dateHour, kv._1.campaignKey, kv._1.cpc, kv._1.siteKey, kv._1.placementId, kv._1.bucketId, kv._1.algoId))


  lazy val sponsoredMetricsGroupedMap: Map[SponsoredMetricsGroupByHourKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(sponsoredMetrics, (kv: (SponsoredMetricsKey, Long)) => SponsoredMetricsGroupByHourKey(kv._1.dateHour, kv._1.campaignKey, kv._1.cpc, kv._1.siteKey, kv._1.placementId, kv._1.bucketId, kv._1.algoId))
  lazy val sponsoredMetricsGroupedAndSorted: scala.Seq[(SponsoredMetricsGroupByHourKey, RecommendationMetrics)] = sponsoredMetricsGroupedMap.toSeq.sorted(SponsoredMetricsRow.sponsoredMetricsGroupedOrdering)

  lazy val sponsoredMetricsGroupedByCampaignMap: Map[SponsoredMetricsGroupByCampaignByHourKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(sponsoredMetrics, (kv: (SponsoredMetricsKey, Long)) => SponsoredMetricsGroupByCampaignByHourKey(kv._1.dateHour, kv._1.campaignKey, kv._1.cpc, kv._1.siteKey, kv._1.placementId))
  lazy val sponsoredMetricsGroupedByCampaignAndSorted: scala.Seq[(SponsoredMetricsGroupByCampaignByHourKey, RecommendationMetrics)] = sponsoredMetricsGroupedByCampaignMap.toSeq.sorted(SponsoredMetricsRow.sponsoredMetricsGroupedByCampaignOrdering)

  lazy val sponsoredMetricsNoSiteGroupedMap: Map[SponsoredMetricsNoSiteGroupByHourKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(sponsoredMetrics, (kv: (SponsoredMetricsKey, Long)) => SponsoredMetricsNoSiteGroupByHourKey(kv._1.dateHour, kv._1.campaignKey, kv._1.cpc, kv._1.placementId, kv._1.bucketId, kv._1.algoId))
  lazy val sponsoredMetricsNoSiteGroupedAndSorted: scala.Seq[(SponsoredMetricsNoSiteGroupByHourKey, RecommendationMetrics)] = sponsoredMetricsNoSiteGroupedMap.toSeq.sorted(SponsoredMetricsRow.sponsoredMetricsNoSiteGroupedOrdering)

  lazy val sponsoredMetricsGroupedByDayMap: Map[SponsoredMetricsGroupByDayKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(sponsoredDailyMetrics, (kv: (SponsoredMetricsKey, Long)) => SponsoredMetricsGroupByDayKey(kv._1.dateHour.toGrvDateMidnight, kv._1.campaignKey, kv._1.cpc, kv._1.siteKey, kv._1.placementId, kv._1.bucketId, kv._1.algoId))
  lazy val sponsoredMetricsGroupedByDayAndSorted: scala.Seq[(SponsoredMetricsGroupByDayKey, RecommendationMetrics)] = sponsoredMetricsGroupedByDayMap.toSeq.sorted(SponsoredMetricsRow.sponsoredMetricsGroupedByDayOrdering)

  lazy val sponsoredMetricsGroupedByCampaignByDayMap: Map[SponsoredMetricsGroupByCampaignByDayKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(sponsoredDailyMetrics, (kv: (SponsoredMetricsKey, Long)) => SponsoredMetricsGroupByCampaignByDayKey(kv._1.dateHour.toGrvDateMidnight, kv._1.campaignKey, kv._1.cpc, kv._1.siteKey, kv._1.placementId))
  lazy val sponsoredMetricsGroupedByCampaignByDayAndSorted: scala.Seq[(SponsoredMetricsGroupByCampaignByDayKey, RecommendationMetrics)] = sponsoredMetricsGroupedByCampaignByDayMap.toSeq.sorted(SponsoredMetricsRow.sponsoredMetricsGroupByCampaignByDayOrdering)

  lazy val sponsoredMetricsGroupedByDayAndSiteMap: Map[SponsoredMetricsGroupByDayKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(sponsoredMetrics, (kv: (SponsoredMetricsKey, Long)) => SponsoredMetricsGroupByDayKey(kv._1.dateHour.toGrvDateMidnight, CampaignKey.empty, DollarValue(0L), kv._1.siteKey, kv._1.placementId, -1, -1))
  lazy val sponsoredMetricsGroupedByDayAndSiteMapSorted: scala.Seq[(SponsoredMetricsGroupByDayKey, RecommendationMetrics)] = sponsoredMetricsGroupedByDayAndSiteMap.toSeq.sorted(SponsoredMetricsRow.sponsoredMetricsGroupedByDayOrdering)

  lazy val sponsoredMetricsNoSiteGroupedByDayMap: Map[SponsoredMetricsNoSiteGroupByDayKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(sponsoredMetrics, (kv: (SponsoredMetricsKey, Long)) => SponsoredMetricsNoSiteGroupByDayKey(kv._1.dateHour.toGrvDateMidnight, kv._1.campaignKey, kv._1.cpc, kv._1.placementId, kv._1.bucketId, kv._1.algoId))
  lazy val sponsoredMetricsNoSiteGroupedByDayAndSorted: scala.Seq[(SponsoredMetricsNoSiteGroupByDayKey, RecommendationMetrics)] = sponsoredMetricsNoSiteGroupedByDayMap.toSeq.sorted(SponsoredMetricsRow.sponsoredMetricsNoSiteGroupedByDayOrdering)

  lazy val sponsoredMetricsByHourMap: Map[DateHour, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(sponsoredMetrics, (kv: (SponsoredMetricsKey, Long)) => kv._1.dateHour)

  lazy val sponsoredMetricsGroupedAndSortedByHour: scala.Seq[(DateHour, RecommendationMetrics)] = sponsoredMetricsByHourMap.toSeq.sortBy(-_._1.getMillis)

  lazy val sponsoredMetricsByHostSiteMap: Predef.Map[SiteKey, SiteSponsoredMetricsSummary] = sponsoredMetrics.toSeq.groupBy(_._1.siteKey).map((kvs: (SiteKey, Seq[(SponsoredMetricsKey, Long)])) => {
    val key = kvs._1
    val values = RecommendationMetricsKey.groupMapgregate(kvs._2, (kv: (SponsoredMetricsKey, Long)) => SponsoredMetricsNoSiteGroupByHourKey(kv._1.dateHour, kv._1.campaignKey, kv._1.cpc, kv._1.placementId, kv._1.bucketId, kv._1.algoId)).toSeq.sortBy(-_._1.hour.getMillis)
    val total = values.foldLeft(RecommendationMetrics.empty) {case (tot: RecommendationMetrics, kv: (SponsoredMetricsNoSiteGroupByHourKey, RecommendationMetrics)) => tot + kv._2}

    key -> SiteSponsoredMetricsSummary(values, total)
  })

  lazy val sponsoredMetricsByHostSiteAndSortedByCTR: scala.Seq[(SiteKey, SiteSponsoredMetricsSummary)] = sponsoredMetricsByHostSiteMap.toSeq.sortBy(-_._2.total.articleClickThroughRate)
}

trait ArticleSponsoredMetricsRow[T <: HbaseTable[T, R, RR] with ArticleSponsoredMetricsColumns[T, R], R, RR <: HRow[T, R]] {
  this: HRow[T, R] =>

  lazy val articleSponsoredMetrics: Map[ArticleSponsoredMetricsKey, Long] = family(_.articleSponsoredMetrics)

  lazy val articleSponsoredMetricsGroupedMap: Map[ArticleSponsoredMetricsGroupByHourKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(articleSponsoredMetrics, (kv: (ArticleSponsoredMetricsKey, Long)) => ArticleSponsoredMetricsGroupByHourKey(kv._1.dateHour, kv._1.campaignKey, kv._1.cpc, kv._1.siteKey, kv._1.placementId, kv._1.bucketId, kv._1.algoId, kv._1.articleKeyOption))
  lazy val articleSponsoredMetricsGroupedAndSorted: scala.Seq[(ArticleSponsoredMetricsGroupByHourKey, RecommendationMetrics)] = articleSponsoredMetricsGroupedMap.toSeq.sorted(ArticleSponsoredMetricsRow.articleSponsoredMetricsGroupedOrdering)

  lazy val articleSponsoredMetricsGroupedByDayMap: Map[ArticleSponsoredMetricsGroupByDayKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(articleSponsoredMetrics, (kv: (ArticleSponsoredMetricsKey, Long)) => ArticleSponsoredMetricsGroupByDayKey(kv._1.dateHour.toGrvDateMidnight, kv._1.campaignKey, kv._1.cpc, kv._1.siteKey, kv._1.placementId, kv._1.bucketId, kv._1.algoId, kv._1.articleKeyOption))
  lazy val articleSponsoredMetricsGroupedByDayAndSorted: scala.Seq[(ArticleSponsoredMetricsGroupByDayKey, RecommendationMetrics)] = articleSponsoredMetricsGroupedByDayMap.toSeq.sorted(ArticleSponsoredMetricsRow.articleSponsoredMetricsGroupedByDayOrdering)
}

object ArticleSponsoredMetricsRow {
  val articleSponsoredMetricsGroupedByDayOrdering: Ordering[(ArticleSponsoredMetricsGroupByDayKey, _)] = new Ordering[(ArticleSponsoredMetricsGroupByDayKey, _)] {
    def compare(x: (ArticleSponsoredMetricsGroupByDayKey, _), y: (ArticleSponsoredMetricsGroupByDayKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.dateMidnight.isAfter(k2.dateMidnight)) return -1
      if (k2.dateMidnight.isAfter(k1.dateMidnight)) return 1

      if (k1.campaignKey.campaignId < k2.campaignKey.campaignId) return -1
      if (k2.campaignKey.campaignId < k1.campaignKey.campaignId) return 1

      if (k1.cpc.pennies < k2.cpc.pennies) return -1
      if (k2.cpc.pennies < k1.cpc.pennies) return 1

      if (k1.hostSite.siteId < k2.hostSite.siteId) return -1
      if (k2.hostSite.siteId < k1.hostSite.siteId) return 1

      if (k1.placementId < k2.placementId) return -1
      if (k2.placementId < k1.placementId) return 1

      if (k1.bucket < k2.bucket) return -1
      if (k2.bucket < k1.bucket) return 1

      if (k1.algoId < k2.algoId) return -1
      if (k2.algoId < k1.algoId) return 1

      k1.articleKeyOption match {
        case Some(articleKey1) =>
          k2.articleKeyOption match {
            case Some(articleKey2) =>
              if(articleKey1 < articleKey2) return -1
              if(articleKey2 < articleKey1) return 1
            case None =>
              //order no article key before has article key, so 2 is less than 1 in this instance
              return 1
          }
        case None =>
          k2.articleKeyOption match {
            case Some(articleKey2) =>
              //no key for 1, key for 2, 1 is less than 2
              return -1
            case None =>
              //neither has a key, they match
          }
      }

      0
    }
  }
  val articleSponsoredMetricsGroupedOrdering: Ordering[(ArticleSponsoredMetricsGroupByHourKey, _)]  = new Ordering[(ArticleSponsoredMetricsGroupByHourKey, _)] {
    def compare(x: (ArticleSponsoredMetricsGroupByHourKey, _), y: (ArticleSponsoredMetricsGroupByHourKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.hour.isAfter(k2.hour)) return -1
      if (k2.hour.isAfter(k1.hour)) return 1

      if (k1.campaignKey.campaignId < k2.campaignKey.campaignId) return -1
      if (k2.campaignKey.campaignId < k1.campaignKey.campaignId) return 1

      if (k1.cpc.pennies < k2.cpc.pennies) return -1
      if (k2.cpc.pennies < k1.cpc.pennies) return 1

      if (k1.hostSite.siteId < k2.hostSite.siteId) return -1
      if (k2.hostSite.siteId < k1.hostSite.siteId) return 1

      if (k1.placementId < k2.placementId) return -1
      if (k2.placementId < k1.placementId) return 1

      if (k1.bucket < k2.bucket) return -1
      if (k2.bucket < k1.bucket) return 1

      if (k1.algoId < k2.algoId) return -1
      if (k2.algoId < k1.algoId) return 1

      k1.articleKeyOption match {
        case Some(articleKey1) =>
          k2.articleKeyOption match {
            case Some(articleKey2) =>
              if(articleKey1 < articleKey2) return -1
              if(articleKey2 < articleKey1) return 1
            case None =>
              //order no article key before has article key, so 2 is less than 1 in this instance
              return 1
          }
        case None =>
          k2.articleKeyOption match {
            case Some(articleKey2) =>
              //no key for 1, key for 2, 1 is less than 2
              return -1
            case None =>
              //neither has a key, they match
          }
      }
      0
    }
  }
}

object SponsoredMetricsRow {

  val sponsoredMetricsOrdering: Ordering[(SponsoredMetricsKey, Long)]  = new Ordering[(SponsoredMetricsKey, Long)] {
    def compare(x: (SponsoredMetricsKey, Long), y: (SponsoredMetricsKey, Long)): Int = {
      if (x._1.dateHour.isAfter(y._1.dateHour)) return -1
      if (y._1.dateHour.isAfter(x._1.dateHour)) return 1
      x._2.compare(y._2)
    }
  }

  val sponsoredMetricsGroupedOrdering: Ordering[(SponsoredMetricsGroupByHourKey, _)]  = new Ordering[(SponsoredMetricsGroupByHourKey, _)] {
    def compare(x: (SponsoredMetricsGroupByHourKey, _), y: (SponsoredMetricsGroupByHourKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.hour.isAfter(k2.hour)) return -1
      if (k2.hour.isAfter(k1.hour)) return 1

      if (k1.campaignKey.campaignId < k2.campaignKey.campaignId) return -1
      if (k2.campaignKey.campaignId < k1.campaignKey.campaignId) return 1

      if (k1.cpc.pennies < k2.cpc.pennies) return -1
      if (k2.cpc.pennies < k1.cpc.pennies) return 1

      if (k1.hostSite.siteId < k2.hostSite.siteId) return -1
      if (k2.hostSite.siteId < k1.hostSite.siteId) return 1

      if (k1.placementId < k2.placementId) return -1
      if (k2.placementId < k1.placementId) return 1

      if (k1.bucket < k2.bucket) return -1
      if (k2.bucket < k1.bucket) return 1

      if (k1.algoId < k2.algoId) return -1
      if (k2.algoId < k1.algoId) return 1
      0
    }
  }

  val sponsoredMetricsGroupedByCampaignOrdering: Ordering[(SponsoredMetricsGroupByCampaignByHourKey, _)]  = new Ordering[(SponsoredMetricsGroupByCampaignByHourKey, _)] {
    def compare(x: (SponsoredMetricsGroupByCampaignByHourKey, _), y: (SponsoredMetricsGroupByCampaignByHourKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.hour.isAfter(k2.hour)) return -1
      if (k2.hour.isAfter(k1.hour)) return 1

      if (k1.campaignKey.campaignId < k2.campaignKey.campaignId) return -1
      if (k2.campaignKey.campaignId < k1.campaignKey.campaignId) return 1

      if (k1.cpc.pennies < k2.cpc.pennies) return -1
      if (k2.cpc.pennies < k1.cpc.pennies) return 1

      if (k1.hostSite.siteId < k2.hostSite.siteId) return -1
      if (k2.hostSite.siteId < k1.hostSite.siteId) return 1

      if (k1.placementId < k2.placementId) return -1
      if (k2.placementId < k1.placementId) return 1

      0
    }
  }

  val sponsoredMetricsNoSiteGroupedOrdering: Ordering[(SponsoredMetricsNoSiteGroupByHourKey, _)]= new Ordering[(SponsoredMetricsNoSiteGroupByHourKey, _)] {
    def compare(x: (SponsoredMetricsNoSiteGroupByHourKey, _), y: (SponsoredMetricsNoSiteGroupByHourKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.hour.isAfter(k2.hour)) return -1
      if (k2.hour.isAfter(k1.hour)) return 1

      if (k1.campaignKey.campaignId < k2.campaignKey.campaignId) return -1
      if (k2.campaignKey.campaignId < k1.campaignKey.campaignId) return 1

      if (k1.cpc.pennies < k2.cpc.pennies) return -1
      if (k2.cpc.pennies < k1.cpc.pennies) return 1

      if (k1.placementId < k2.placementId) return -1
      if (k2.placementId < k1.placementId) return 1

      if (k1.bucket < k2.bucket) return -1
      if (k2.bucket < k1.bucket) return 1

      if (k1.algoId < k2.algoId) return -1
      if (k2.algoId < k1.algoId) return 1
      0
    }
  }

  val sponsoredMetricsGroupedByDayOrdering: Ordering[(SponsoredMetricsGroupByDayKey, _)] = new Ordering[(SponsoredMetricsGroupByDayKey, _)] {
    def compare(x: (SponsoredMetricsGroupByDayKey, _), y: (SponsoredMetricsGroupByDayKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.dateMidnight.isAfter(k2.dateMidnight)) return -1
      if (k2.dateMidnight.isAfter(k1.dateMidnight)) return 1

      if (k1.campaignKey.campaignId < k2.campaignKey.campaignId) return -1
      if (k2.campaignKey.campaignId < k1.campaignKey.campaignId) return 1

      if (k1.cpc.pennies < k2.cpc.pennies) return -1
      if (k2.cpc.pennies < k1.cpc.pennies) return 1

      if (k1.hostSite.siteId < k2.hostSite.siteId) return -1
      if (k2.hostSite.siteId < k1.hostSite.siteId) return 1

      if (k1.placementId < k2.placementId) return -1
      if (k2.placementId < k1.placementId) return 1

      if (k1.bucket < k2.bucket) return -1
      if (k2.bucket < k1.bucket) return 1

      if (k1.algoId < k2.algoId) return -1
      if (k2.algoId < k1.algoId) return 1
      0
    }
  }

  val sponsoredMetricsGroupByCampaignByDayOrdering: Ordering[(SponsoredMetricsGroupByCampaignByDayKey, _)] = new Ordering[(SponsoredMetricsGroupByCampaignByDayKey, _)] {
    def compare(x: (SponsoredMetricsGroupByCampaignByDayKey, _), y: (SponsoredMetricsGroupByCampaignByDayKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.dateMidnight.isAfter(k2.dateMidnight)) return -1
      if (k2.dateMidnight.isAfter(k1.dateMidnight)) return 1

      if (k1.campaignKey.campaignId < k2.campaignKey.campaignId) return -1
      if (k2.campaignKey.campaignId < k1.campaignKey.campaignId) return 1

      if (k1.cpc.pennies < k2.cpc.pennies) return -1
      if (k2.cpc.pennies < k1.cpc.pennies) return 1

      if (k1.hostSite.siteId < k2.hostSite.siteId) return -1
      if (k2.hostSite.siteId < k1.hostSite.siteId) return 1

      if (k1.placementId < k2.placementId) return -1
      if (k2.placementId < k1.placementId) return 1

      0
    }
  }

  val sponsoredMetricsNoSiteGroupedByDayOrdering: Ordering[(SponsoredMetricsNoSiteGroupByDayKey, _)]  = new Ordering[(SponsoredMetricsNoSiteGroupByDayKey, _)] {
    def compare(x: (SponsoredMetricsNoSiteGroupByDayKey, _), y: (SponsoredMetricsNoSiteGroupByDayKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.dateMidnight.isAfter(k2.dateMidnight)) return -1
      if (k2.dateMidnight.isAfter(k1.dateMidnight)) return 1

      if (k1.campaignKey.campaignId < k2.campaignKey.campaignId) return -1
      if (k2.campaignKey.campaignId < k1.campaignKey.campaignId) return 1

      if (k1.cpc.pennies < k2.cpc.pennies) return -1
      if (k2.cpc.pennies < k1.cpc.pennies) return 1

      if (k1.placementId < k2.placementId) return -1
      if (k2.placementId < k1.placementId) return 1

      if (k1.bucket < k2.bucket) return -1
      if (k2.bucket < k1.bucket) return 1

      if (k1.algoId < k2.algoId) return -1
      if (k2.algoId < k1.algoId) return 1
      0
    }
  }
}


case class SiteSponsoredMetricsSummary(values: Seq[(SponsoredMetricsNoSiteGroupByHourKey, RecommendationMetrics)], total: RecommendationMetrics)

case class SponsoredMetricsGroupByHourKey(hour: DateHour, campaignKey: CampaignKey, cpc: DollarValue, hostSite: SiteKey, placementId: Int, bucket: Int, algoId: Int)

case class SponsoredMetricsGroupByCampaignByHourKey(hour: DateHour, campaignKey: CampaignKey, cpc: DollarValue, hostSite: SiteKey, placementId: Int)

case class ArticleSponsoredMetricsGroupByHourKey(hour: DateHour, campaignKey: CampaignKey, cpc: DollarValue, hostSite: SiteKey, placementId: Int, bucket: Int, algoId: Int, articleKeyOption: Option[ArticleKey])

case class SponsoredMetricsNoSiteGroupByHourKey(hour: DateHour, campaignKey: CampaignKey, cpc: DollarValue, placementId: Int, bucket: Int, algoId: Int)

case class SponsoredMetricsGroupByDayKey(dateMidnight: GrvDateMidnight, campaignKey: CampaignKey, cpc: DollarValue, hostSite: SiteKey, placementId: Int, bucket: Int, algoId: Int)

case class SponsoredMetricsGroupByCampaignByDayKey(dateMidnight: GrvDateMidnight, campaignKey: CampaignKey, cpc: DollarValue, hostSite: SiteKey, placementId: Int)

case class ArticleSponsoredMetricsGroupByDayKey(dateMidnight: GrvDateMidnight, campaignKey: CampaignKey, cpc: DollarValue, hostSite: SiteKey, placementId: Int, bucket: Int, algoId: Int, articleKeyOption: Option[ArticleKey])

case class SponsoredMetricsNoSiteGroupByDayKey(dateMidnight: GrvDateMidnight, campaignKey: CampaignKey, cpc: DollarValue, placementId: Int, bucket: Int, algoId: Int)

case class SponsoredMetricsGroupByDayAndSiteKey(dateMidnight: GrvDateMidnight, campaignKey: CampaignKey, cpc: DollarValue)
