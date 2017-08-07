package com.gravity.interests.jobs.intelligence.operations.metric

import scala.collection.Map

import com.cloudera.org.joda.time.DateTime
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.{ScopedFromToKey, ScopedKeyTypes, ScopedMetricsKey}
import com.gravity.service.remoteoperations.SplittablePayload
import com.gravity.utilities.MurmurHash
import com.gravity.valueclasses.ValueClassesForDomain.SitePlacementId

/**
 * Created by agrealish14 on 10/12/16.
 */
case class LiveMetricUpdate(updates:Seq[LiveMetricUpdateBundle]) extends SplittablePayload[LiveMetricUpdate] {
  override def size = updates.size

  override def split(into: Int): Array[Option[LiveMetricUpdate]] = {
    val results = new Array[scala.collection.mutable.Set[LiveMetricUpdateBundle]](into)

    updates.foreach { update => {

      val routeId = MurmurHash.hash64(update.key.from.keyString + "+" + update.key.to.keyString)

      val thisIndex = bucketIndexFor(routeId, into)
      if (results(thisIndex) == null)
        results(thisIndex) = scala.collection.mutable.Set[LiveMetricUpdateBundle](update)
      else
        results(thisIndex).add(update)
    }}

    results.map { result => {
      if (result == null)
        None
      else
        Some(LiveMetricUpdate(result.toList))
    }}
  }
}

object LiveMetricUpdate {

  val testArticleKey = ArticleKey("http://somesite.com/some/article.html").toScopedKey

  val testSiteKey = SiteKey("http://somesite.com/").toScopedKey

  val testSitePlacementKey = SitePlacementIdKey(SitePlacementId(9999l)).toScopedKey

  val testMetrics = List[LiveMetrics](
    LiveMetrics(ScopedMetricsKey(DateTime.now().getMillis, RecommendationMetricCountBy.unitImpression), 1L),
    LiveMetrics(ScopedMetricsKey(DateTime.now().getMillis, RecommendationMetricCountBy.click), 1L)
  )

  val testLiveMetrics = List(
    LiveMetricUpdateBundle(ScopedFromToKey(testArticleKey, testSiteKey), testMetrics),
    LiveMetricUpdateBundle(ScopedFromToKey(testArticleKey, testSitePlacementKey), testMetrics)
  )

  val testObject: LiveMetricUpdate = LiveMetricUpdate(testLiveMetrics)

  def apply(allEventsMap: Map[ScopedFromToKey, Map[ScopedMetricsKey, Long]]): LiveMetricUpdate = {

    val res: LiveMetricUpdate = LiveMetricUpdate(
      filterMap(allEventsMap).map(row => {
        LiveMetricUpdateBundle(
          row._1,
          row._2.map(m => {
            LiveMetrics(m._1, m._2)
          }).toList
        )
      }).toList
    )

    res
  }

  private def filterMap(allEventsMap: Map[ScopedFromToKey, Map[ScopedMetricsKey, Long]]) = {

    allEventsMap.filter(row => {

      validLiveFromToKey(row._1)
    })
  }

  private def validLiveFromToKey(key:ScopedFromToKey) = {

    val res = (key.from.scope == ScopedKeyTypes.ALL_SITES && key.to.scope == ScopedKeyTypes.SITE) ||
      (key.from.scope == ScopedKeyTypes.ALL_SITES && key.to.scope == ScopedKeyTypes.SITE_PLACEMENT_ID) ||
      //(key.from.scope == ScopedKeyTypes.ALL_SITES && key.to.scope == ScopedKeyTypes.SITE_PLACEMENT_WHY) ||
      (key.from.scope == ScopedKeyTypes.ORDINAL_KEY && key.to.scope == ScopedKeyTypes.SITE_PLACEMENT) ||
      (key.from.scope == ScopedKeyTypes.ARTICLE && key.to.scope == ScopedKeyTypes.ALL_SITES) ||
      (key.from.scope == ScopedKeyTypes.ARTICLE && key.to.scope == ScopedKeyTypes.SITE) ||
      (key.from.scope == ScopedKeyTypes.ARTICLE && key.to.scope == ScopedKeyTypes.SITE_PLACEMENT_ID) ||
      (key.from.scope == ScopedKeyTypes.ARTICLE && key.to.scope == ScopedKeyTypes.SITE_PLACEMENT_BUCKET) ||
      (key.from.scope == ScopedKeyTypes.ARTICLE_ORDINAL_KEY && key.to.scope == ScopedKeyTypes.SITE) ||
      (key.from.scope == ScopedKeyTypes.ALL_SITES && key.to.scope == ScopedKeyTypes.ENTITY_SCOPE_KEY) ||
      (key.from.scope == ScopedKeyTypes.ALL_SITES && key.to.scope == ScopedKeyTypes.ENTITY_SCOPE_KEY) ||
      (key.from.scope == ScopedKeyTypes.ALL_SITES && key.to.scope == ScopedKeyTypes.ENTITY_SCOPE_KEY) ||
      (key.from.scope == ScopedKeyTypes.ORDINAL_KEY && key.to.scope == ScopedKeyTypes.ENTITY_SCOPE_KEY) ||
      (key.from.scope == ScopedKeyTypes.ARTICLE && key.to.scope == ScopedKeyTypes.ENTITY_SCOPE_KEY) ||
      (key.from.scope == ScopedKeyTypes.ARTICLE && key.to.scope == ScopedKeyTypes.ENTITY_SCOPE_KEY) ||
      (key.from.scope == ScopedKeyTypes.ARTICLE && key.to.scope == ScopedKeyTypes.ENTITY_SCOPE_KEY) ||
      (key.from.scope == ScopedKeyTypes.ARTICLE && key.to.scope == ScopedKeyTypes.ENTITY_SCOPE_KEY) ||
      (key.from.scope == ScopedKeyTypes.ARTICLE_ORDINAL_KEY && key.to.scope == ScopedKeyTypes.ENTITY_SCOPE_KEY)

    res
  }
}

case class LiveMetricUpdateBundle(key:ScopedFromToKey, metrics: Seq[LiveMetrics]) {

  def toMetricsMap: Map[ScopedMetricsKey, Long] = {

    metrics.map(m=>{

      m.metricKey -> m.count
    }).toMap
  }
}

case class LiveMetrics(metricKey:ScopedMetricsKey, count:Long)