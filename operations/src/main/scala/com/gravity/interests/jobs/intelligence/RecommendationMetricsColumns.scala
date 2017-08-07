package com.gravity.interests.jobs.intelligence

import com.gravity.domain.grvmetrics
import com.gravity.hbase.schema.{HRow, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.operations.CampaignService
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}
import com.gravity.utilities.{GrvConcurrentMap, grvtime}

import scala.collection._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/21/12
 * Time: 4:24 PM
 */
trait RecommendationMetricsColumns[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  val recommendationMetrics: this.Fam[ArticleRecommendationMetricKey, Long] = family[ArticleRecommendationMetricKey, Long]("rm", rowTtlInSeconds = 2592000, compressed = true)
  val recommendationDailyMetrics: this.Fam[ArticleRecommendationMetricKey, Long] = family[ArticleRecommendationMetricKey, Long]("dyrm", rowTtlInSeconds = 2592000 * 6, compressed = true)
  val recommendationMonthlyMetrics: this.Fam[ArticleRecommendationMetricKey, Long] = family[ArticleRecommendationMetricKey, Long]("mrm", rowTtlInSeconds = Int.MaxValue, compressed = true)
}

trait TopArticleRecommendationMetricsColumns[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  val topArticleRecommendationMetrics: this.Fam[TopArticleRecommendationMetricKey, Long] = family[TopArticleRecommendationMetricKey, Long]("arm", rowTtlInSeconds = 2592000, compressed = true)
}


trait HasRecommendationMetrics {

  // everything is derived off of this map
  // val=>def per Mr. B. recommendation
  def recommendationMetrics : Map[ArticleRecommendationMetricKey, Long]

  lazy val recommendationMetricsByMonth: Map[GrvDateMidnight, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(recommendationMetrics, (kv: (ArticleRecommendationMetricKey, Long)) => grvtime.dateToMonth(kv._1.dateHour))

  lazy val recommendationMetricsBySite: Map[SiteKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(recommendationMetrics, (kv: (ArticleRecommendationMetricKey, Long)) => kv._1.siteKey)

  lazy val recommendationMetricsBySiteAndPlugin: Map[SiteKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(recommendationMetrics, (kv: (ArticleRecommendationMetricKey, Long)) => kv._1.siteKey)

  lazy val recommendationMetricsByDay: Map[GrvDateMidnight, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(recommendationMetrics, (kv: (ArticleRecommendationMetricKey, Long)) => kv._1.dateHour.toGrvDateMidnight)

  lazy val recommendationMetricsGroupedMap: Map[RecommendationMetricsGroupByHourKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(recommendationMetrics, (kv: (ArticleRecommendationMetricKey, Long)) => RecommendationMetricsGroupByHourKey(kv._1.dateHour, kv._1.siteKey, kv._1.placementId, kv._1.bucketId, kv._1.algoId))
  lazy val recommendationMetricsGroupedAndSorted: scala.Seq[(RecommendationMetricsGroupByHourKey, RecommendationMetrics)] = recommendationMetricsGroupedMap.toSeq.sorted(RecommendationMetricsRow.recommendationMetricsGroupedOrdering)

  lazy val recommendationMetricsNoSiteGroupedMap: Map[RecommendationMetricsNoSiteGroupByHourKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(recommendationMetrics, (kv: (ArticleRecommendationMetricKey, Long)) => RecommendationMetricsNoSiteGroupByHourKey(kv._1.dateHour, kv._1.placementId, kv._1.bucketId, kv._1.algoId))
  lazy val recommendationMetricsNoSiteGroupedAndSorted: scala.Seq[(RecommendationMetricsNoSiteGroupByHourKey, RecommendationMetrics)] = recommendationMetricsNoSiteGroupedMap.toSeq.sorted(RecommendationMetricsRow.recommendationMetricsNoSiteGroupedOrdering)

  lazy val recommendationMetricsGroupedByDayMap: Map[RecommendationMetricsGroupByDayKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(recommendationMetrics, (kv: (ArticleRecommendationMetricKey, Long)) => RecommendationMetricsGroupByDayKey(kv._1.dateHour.toGrvDateMidnight, kv._1.siteKey, kv._1.placementId, kv._1.bucketId, kv._1.algoId))
  lazy val recommendationMetricsGroupedByDayAndSorted: scala.Seq[(RecommendationMetricsGroupByDayKey, RecommendationMetrics)] = recommendationMetricsGroupedByDayMap.toSeq.sorted(RecommendationMetricsRow.recommendationMetricsGroupedByDayOrdering)

  lazy val recommendationMetricsNoSiteGroupedByDayMap: Map[RecommendationMetricsNoSiteGroupByDayKey, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(recommendationMetrics, (kv: (ArticleRecommendationMetricKey, Long)) => RecommendationMetricsNoSiteGroupByDayKey(kv._1.dateHour.toGrvDateMidnight, kv._1.placementId, kv._1.bucketId, kv._1.algoId))
  lazy val recommendationMetricsNoSiteGroupedByDayAndSorted: scala.Seq[(RecommendationMetricsNoSiteGroupByDayKey, RecommendationMetrics)] = recommendationMetricsNoSiteGroupedByDayMap.toSeq.sorted(RecommendationMetricsRow.recommendationMetricsNoSiteGroupedByDayOrdering)

  lazy val recommendationMetricsByHourMap: Map[DateHour, RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(recommendationMetrics, (kv: (ArticleRecommendationMetricKey, Long)) => kv._1.dateHour)
  lazy val recommendationMetricsByBucketAndHourMap: Map[(DateHour, Int), RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(recommendationMetrics, (kv: (ArticleRecommendationMetricKey, Long)) => kv._1.dateHour -> kv._1.bucketId)

  lazy val recommendationMetricsByAlgoAndHourMap: Map[(DateHour, Int), RecommendationMetrics] = RecommendationMetricsKey.groupMapgregate(recommendationMetrics, (kv: (ArticleRecommendationMetricKey, Long)) => kv._1.dateHour -> kv._1.algoId)

  lazy val recommendationMetricsGroupedAndSortedByHour: scala.Seq[(DateHour, RecommendationMetrics)] = recommendationMetricsByHourMap.toSeq.sortBy(-_._1.getMillis)

  lazy val recommendationMetricsByHostSiteMap: Predef.Map[SiteKey, SiteRecommendationMetricsSummary] = recommendationMetrics.toSeq.groupBy(_._1.siteKey).map((kvs: (SiteKey, Seq[(ArticleRecommendationMetricKey, Long)])) => {
    val key = kvs._1
    val values = RecommendationMetricsKey.groupMapgregate(kvs._2, (kv: (ArticleRecommendationMetricKey, Long)) => RecommendationMetricsNoSiteGroupByHourKey(kv._1.dateHour, kv._1.placementId, kv._1.bucketId, kv._1.algoId)).toSeq.sortBy(-_._1.hour.getMillis)
    val total = values.foldLeft(RecommendationMetrics.empty) {
      case (tot: RecommendationMetrics, kv: (RecommendationMetricsNoSiteGroupByHourKey, RecommendationMetrics)) => tot + kv._2
    }

    key -> SiteRecommendationMetricsSummary(values, total)
  })

  lazy val recommendationMetricsByHostAndBucketSiteMap: Predef.Map[SiteBucketKey, SiteBucketRecommendationMetricsSummary] = recommendationMetrics.toSeq.groupBy(kv => SiteBucketKey(kv._1.siteKey, kv._1.bucketId)).map((kvs: (SiteBucketKey, Seq[(ArticleRecommendationMetricKey, Long)])) => {
    val key = kvs._1
    val values = RecommendationMetricsKey.groupMapgregate(kvs._2, (kv: (ArticleRecommendationMetricKey, Long)) => RecommendationMetricsNoSiteOrBucketGroupByHourKey(kv._1.dateHour, kv._1.placementId, kv._1.algoId)).toSeq.sortBy(-_._1.hour.getMillis)
    val total = values.foldLeft(RecommendationMetrics.empty) {
      case (tot: RecommendationMetrics, kv: (RecommendationMetricsNoSiteOrBucketGroupByHourKey, RecommendationMetrics)) => tot + kv._2
    }

    key -> SiteBucketRecommendationMetricsSummary(values, total)
  })

  lazy val recommendationMetricsByHostSiteAndSortedByCTR: scala.Seq[(SiteKey, SiteRecommendationMetricsSummary)] = recommendationMetricsByHostSiteMap.toSeq.sortBy(-_._2.total.articleClickThroughRate)


  type RecommendationMetricsFilter = (RecommendationMetricsKey) => Boolean
  /**
   * Given a way to filter metrics keys, will aggregate the resulting RecommendationMetrics.
   * @param filter If true, keep the metrics.
   * @return The aggregated metrics.
   */
  def recommendationMetricsBy(filter: RecommendationMetricsFilter): RecommendationMetrics = {
    recommendationMetrics.aggregate(RecommendationMetrics.empty)((total, kv) => {
      if (filter(kv._1))
        total + kv._1.recoMetrics(kv._2)(CampaignService.isOrganic)
      else
        total
    }, _ + _)
  }


  import grvmetrics._
  lazy val recoMetricsBy_SiteBucketMemo: Predef.Map[(SiteKey, Int), Predef.Map[DateHour, RecommendationMetrics]] = recommendationMetrics.asRecoMetrics.groupedTimeSeriesBy(key=>key._1.siteKey -> key._1.bucketId)
  lazy val recoMetricsBy_SitePlacementMemo: Predef.Map[(SiteKey, Int), Predef.Map[DateHour, RecommendationMetrics]] = recommendationMetrics.asRecoMetrics.groupedTimeSeriesBy(key => key._1.siteKey -> key._1.placementId)
  // reduce methods may run slower than the alternative.  Odd because it would seem they should be faster
  lazy val recoMetricsBy_SiteMemo: Predef.Map[SiteKey, Predef.Map[DateHour, RecommendationMetrics]] = recommendationMetrics.asRecoMetrics.groupedTimeSeriesBy(_._1.siteKey) //reduceSiteBucketMetricsToSite(recoMetricsBy_SiteBucketMemo)
  lazy val recoMetricsBy_CrossSiteMemo: Predef.Map[DateHour, RecommendationMetrics] = recommendationMetrics.asRecoMetrics.timeSeriesMap //reduceSiteMetricsToCrossSite(recoMetricsBy_SiteMemo)

  def reduceSiteBucketMetricsToSite(recoMetricsBy_SiteBucket: Map[(SiteKey, Int), Map[DateHour, RecommendationMetrics]]) : Map[SiteKey, Map[DateHour, RecommendationMetrics]] = {
    val recoMetricsBy_Site = mutable.Map[SiteKey, mutable.Map[DateHour, RecommendationMetrics]]()

    for (kv <- recoMetricsBy_SiteBucket) {
      val (siteKey, bucket) = kv._1
      val siteBucketMetricsMap = kv._2
      val existingSiteMetrics = recoMetricsBy_Site.getOrElse(siteKey, mutable.Map[DateHour, RecommendationMetrics]())

      for ((dtHr, recoMetrics) <- siteBucketMetricsMap) {
        val existingSiteMetricsForDtHr = existingSiteMetrics.getOrElse(dtHr, RecommendationMetrics.empty)
        existingSiteMetrics(dtHr) = existingSiteMetricsForDtHr + recoMetrics
      }

      recoMetricsBy_Site(siteKey) = existingSiteMetrics
    }

    recoMetricsBy_Site
  }

  def reduceSiteMetricsToCrossSite(recoMetricsBy_Site: Map[SiteKey, Map[DateHour, RecommendationMetrics]] ) : Map[DateHour, RecommendationMetrics] = {
    val recoMetricsBy_CrossSite = mutable.Map[DateHour, RecommendationMetrics]()

    for (kv <- recoMetricsBy_Site) {
      val siteKey = kv._1
      val siteMetricsMap = kv._2

      for ((dtHr, recoMetrics) <- siteMetricsMap) {
        val existingSiteMetricsForDtHr = recoMetricsBy_CrossSite.getOrElse(dtHr, RecommendationMetrics.empty)
        recoMetricsBy_CrossSite(dtHr) = existingSiteMetricsForDtHr + recoMetrics
      }
    }

    recoMetricsBy_CrossSite
  }

  def recoMetricsBy(bucketId:Int, hostSiteKey: Option[SiteKey] = None, placementId: Option[Int] = None): Map[DateHour, RecommendationMetrics] = {
    val empty = Map[DateHour, RecommendationMetrics]()
    hostSiteKey match {
      case Some(siteKey) if placementId.isDefined  =>
        recoMetricsBy_SitePlacementMemo.getOrElse((siteKey, placementId.get), empty)
      case Some(siteKey) if bucketId != -1 =>
        recoMetricsBy_SiteBucketMemo.getOrElse((siteKey, bucketId), empty)
      case Some(siteKey) if bucketId == -1 =>
        recoMetricsBy_SiteMemo.getOrElse(siteKey, empty)
      case None => recoMetricsBy_CrossSiteMemo
    }
  }


  /*
the goal of this method is to answer the question : what was the CTR for a particular article for the last X impressions?
we need to have a sliding hourly window because if we hard code the fromHours in aggregateBetween to a set number of hours articles that should
be ejected because of low CTR rate sneak back in
for example -
  yield optimization rule: any article with at least 3000 impressions and a CTR of < .1 is to be marked as "bad"
  situation: if the article receives 5000 impressions 3 hours ago, but only 500 in the last 2 hours, and our fromHours is set to 2
  the RecommendationMetrics object will have only 500 impressions, and the article will be still thought of as a possible
  candidate by the yield optimizer

  setting the fromHours limit to an arbitrarily high number doesn't work either as headline story could have a high CTR 10
  hours ago but a terrible CTR in the last couple hours.  Therefore we need a sliding window.

 This is feels terribly inefficient, but actually runs pretty fast when generating the worksheet. its probably fine for now because its
 only being used for most popular and algos that have pre-generated result sets that are the same for all users.  If this is going to be
 used with a simscore or other algo that is generated in IIO on a per user basis we probably need to find another way

 finally - this should accept placement as well once we pass placement in from default recos
 or we could standardize on everything always having a unique bucketid

*/
  private def getRecoMetricsForLastXImpressions(numberOfImpressions: Int, bucketId: Int, maxRecoMetricsHoursOld: Int = 1, startHour: DateHour = grvtime.currentHour, byHostSiteKey: Option[SiteKey] = None, byPlacementId: Option[Int], useNewVersion:Boolean=true): RecommendationMetrics = {

    def metricsByHostSiteOrNot = if(useNewVersion) recoMetricsBy(bucketId, byHostSiteKey, byPlacementId) else {
      byHostSiteKey match {
        case Some(siteKey) if byPlacementId.isDefined && bucketId == -1 && recommendationMetricsByHostSiteMap.contains(siteKey) =>
          recommendationMetricsByHostSiteMap.get(siteKey).get.values.filter(_._1.placementId == byPlacementId.get).map(kv => kv._1.hour -> kv._2).toMap
        case Some(siteKey) if bucketId != -1 && recommendationMetricsByHostSiteMap.contains(siteKey) =>
          recommendationMetricsByHostSiteMap.get(siteKey).get.values.filter(_._1.bucket == bucketId).map(kv => kv._1.hour -> kv._2).toMap
        case Some(siteKey) if bucketId == -1 && recommendationMetricsByHostSiteMap.contains(siteKey) =>
          recommendationMetricsByHostSiteMap(siteKey).values.map(kv => kv._1.hour -> kv._2).toMap
        case Some(siteKey) if !recommendationMetricsByHostSiteMap.contains(siteKey) =>
          Map[DateHour, RecommendationMetrics]()
        case None if bucketId == -1 => recommendationMetricsByHourMap
        case _ => recommendationMetricsGroupedMap.filter(_._1.bucket == bucketId).map(kv => kv._1.hour -> kv._2)
      }
    }


    val fromHours = grvtime.cachedCurrentHour.minusHours(maxRecoMetricsHoursOld)

    val recoMetrics = metricsByHostSiteOrNot.aggregateBetween(fromHours, startHour)

    //if we have the number of impressions we want or have reached our max recursion return the RecommendationMetrics object
    if ((recoMetrics.articleImpressions >= numberOfImpressions) || (maxRecoMetricsHoursOld >= 96)) {
      //println("found 3000 impressions returning")
      recoMetrics
    }

    //else lets go back 1 more hour and see if we can get to our impression goal
    else {
      val newMaxMetricsHoursOld = maxRecoMetricsHoursOld + 1
      //println("found don't have 3000 recos going recursive with hoursOld of:" + newMaxMetricsHoursOld)

      getRecoMetricsForLastXImpressions(numberOfImpressions, bucketId, newMaxMetricsHoursOld, startHour, byHostSiteKey = byHostSiteKey, byPlacementId = byPlacementId, useNewVersion = useNewVersion)
    }

  }


  private val recoMetricsLastNImpressionsMap = new GrvConcurrentMap[(Int,Int,Option[SiteKey], Option[Int]), RecommendationMetrics]()

  def getRecoMetricsLastXImpressions(numberOfImpressions: Int, bucketId: Int, byHostSiteKey: Option[SiteKey] = None, byPlacementId: Option[Int]): RecommendationMetrics = {
    val key = (numberOfImpressions, bucketId, byHostSiteKey, byPlacementId)
    recoMetricsLastNImpressionsMap.getOrElseUpdate(key, getRecoMetricsForLastXImpressions(numberOfImpressions, bucketId, byHostSiteKey = byHostSiteKey, byPlacementId = byPlacementId))
  }


}

trait RecommendationMetricsRow[T <: HbaseTable[T, R, RR] with RecommendationMetricsColumns[T, R], R, RR <: HRow[T, R]] extends HasRecommendationMetrics {
  this: HRow[T, R] =>

  lazy val recommendationMetrics: Map[ArticleRecommendationMetricKey, Long] = family(_.recommendationMetrics)
  lazy val recommendationDailyMetrics: Map[ArticleRecommendationMetricKey, Long] = family(_.recommendationDailyMetrics)
  lazy val recommendationMonthlyMetrics: Map[ArticleRecommendationMetricKey, Long] = family(_.recommendationMonthlyMetrics)
}

object RecommendationMetricsRow {

  val recommendationMetricsOrdering: Ordering[(ArticleRecommendationMetricKey, Long)] = new Ordering[(ArticleRecommendationMetricKey, Long)] {
    def compare(x: (ArticleRecommendationMetricKey, Long), y: (ArticleRecommendationMetricKey, Long)): Int = {
      if (x._1.dateHour.isAfter(y._1.dateHour)) return -1
      if (y._1.dateHour.isAfter(x._1.dateHour)) return 1
      x._2.compare(y._2)
    }
  }

  val recommendationMetricsGroupedOrdering: Ordering[(RecommendationMetricsGroupByHourKey, _)] = new Ordering[(RecommendationMetricsGroupByHourKey, _)] {
    def compare(x: (RecommendationMetricsGroupByHourKey, _), y: (RecommendationMetricsGroupByHourKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.hour.isAfter(k2.hour)) return -1
      if (k2.hour.isAfter(k1.hour)) return 1
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

  val recommendationMetricsNoSiteGroupedOrdering: Ordering[(RecommendationMetricsNoSiteGroupByHourKey, _)] = new Ordering[(RecommendationMetricsNoSiteGroupByHourKey, _)] {
    def compare(x: (RecommendationMetricsNoSiteGroupByHourKey, _), y: (RecommendationMetricsNoSiteGroupByHourKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.hour.isAfter(k2.hour)) return -1
      if (k2.hour.isAfter(k1.hour)) return 1

      if (k1.placementId < k2.placementId) return -1
      if (k2.placementId < k1.placementId) return 1

      if (k1.bucket < k2.bucket) return -1
      if (k2.bucket < k1.bucket) return 1

      if (k1.algoId < k2.algoId) return -1
      if (k2.algoId < k1.algoId) return 1
      0
    }
  }

  val recommendationMetricsGroupedByDayOrdering: Ordering[(RecommendationMetricsGroupByDayKey, _)] = new Ordering[(RecommendationMetricsGroupByDayKey, _)] {
    def compare(x: (RecommendationMetricsGroupByDayKey, _), y: (RecommendationMetricsGroupByDayKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.dateMidnight.isAfter(k2.dateMidnight)) return -1
      if (k2.dateMidnight.isAfter(k1.dateMidnight)) return 1
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

  val recommendationMetricsNoSiteGroupedByDayOrdering: Ordering[(RecommendationMetricsNoSiteGroupByDayKey, _)] = new Ordering[(RecommendationMetricsNoSiteGroupByDayKey, _)] {
    def compare(x: (RecommendationMetricsNoSiteGroupByDayKey, _), y: (RecommendationMetricsNoSiteGroupByDayKey, _)): Int = {
      val k1 = x._1
      val k2 = y._1

      if (k1.dateMidnight.isAfter(k2.dateMidnight)) return -1
      if (k2.dateMidnight.isAfter(k1.dateMidnight)) return 1

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

trait TopArticleRecommendationMetricsRow[T <: HbaseTable[T, R, RR] with TopArticleRecommendationMetricsColumns[T, R], R, RR <: HRow[T, R]] extends HasRecommendationMetrics {
  this: HRow[T, R] =>

  lazy val topArticleRecommendationMetrics: Map[TopArticleRecommendationMetricKey, Long] = family(_.topArticleRecommendationMetrics)
}

case class SiteBucketKey(siteKey: SiteKey, bucket: Int)

case class SiteRecommendationMetricsSummary(values: Seq[(RecommendationMetricsNoSiteGroupByHourKey, RecommendationMetrics)], total: RecommendationMetrics)

case class SiteBucketRecommendationMetricsSummary(values: Seq[(RecommendationMetricsNoSiteOrBucketGroupByHourKey, RecommendationMetrics)], total: RecommendationMetrics)

case class RecommendationMetricsGroupByHourKey(hour: DateHour, hostSite: SiteKey, placementId: Int, bucket: Int, algoId: Int)

case class RecommendationMetricsNoSiteGroupByHourKey(hour: DateHour, placementId: Int, bucket: Int, algoId: Int)

case class RecommendationMetricsNoSiteOrBucketGroupByHourKey(hour: DateHour, placementId: Int, algoId: Int)

case class RecommendationMetricsGroupByDayKey(dateMidnight: GrvDateMidnight, hostSite: SiteKey, placementId: Int, bucket: Int, algoId: Int)

case class RecommendationMetricsNoSiteGroupByDayKey(dateMidnight: GrvDateMidnight, placementId: Int, bucket: Int, algoId: Int)