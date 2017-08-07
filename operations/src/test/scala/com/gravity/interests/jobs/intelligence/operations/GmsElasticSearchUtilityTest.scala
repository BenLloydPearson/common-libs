package com.gravity.interests.jobs.intelligence.operations

import com.gravity.test.{ElasticSearchTesting, GmsArticleContext, GmsArticleContexts, operationsTesting}
import com.gravity.utilities.{BaseScalaTest, grvtime}
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.action.admin.indices.get.GetIndexRequest
import org.elasticsearch.client.Client

class GmsElasticSearchUtilityTest extends BaseScalaTest with operationsTesting with ElasticSearchTesting {

  def checkIndicesExist(client: Client) = {
    val req = client.admin().indices().prepareGetIndex()
    req.addFeatures(GetIndexRequest.Feature.ALIASES)
    req.setIndices("_all")
    val getIndexResponse = client.admin().indices().getIndex(req.request()).actionGet()
    println("These are the existing indices:")
    getIndexResponse.indices().foreach(println)
  }

  // This test can only be run locally and not in CI, since the scripting feature doesn't work with an ES client node and
  // it needs ES running in a separate process so inline scripting can be turned on.
  ignore("Can populate data in the index, then query it out in a basic way") {
    val minute1 = grvtime.elasticSearchDataMartFormat.parseDateTime("2016-01-01").withMinuteOfHour(1).getMillis
    val minute2 = grvtime.elasticSearchDataMartFormat.parseDateTime("2016-01-01").withMinuteOfHour(2).getMillis
    val minute3 = grvtime.elasticSearchDataMartFormat.parseDateTime("2016-01-01").withMinuteOfHour(3).getMillis

    val articleId1 = 1l

    val articleId2 = 2l

    val articleContexts = List(
      GmsArticleContext(articleId1, minute1, 15, 3, 3 / 15),
      GmsArticleContext(articleId1, minute2, 40, 12, 12 / 40),
      GmsArticleContext(articleId1, minute3, 25, 8, 8 / 25),
      GmsArticleContext(articleId2, minute1, 12, 2, 2 / 12),
      GmsArticleContext(articleId2, minute2, 38, 18, 18 / 38),
      GmsArticleContext(articleId2, minute3, 24, 10, 10 / 24)
    )

    withPopulatedGmsArticleIndex(GmsArticleContexts(articleContexts)) { etp =>
      val esQueryService = testGmsElasticSearchService(etp)

      val esClient = etp.client

      // Try some more calls to wake up the index...
      val resp = esClient.execute {
        search in s"${GmsElasticSearchUtility.indexBaseName}*" / GmsElasticSearchUtility.typeNameForMeta
        //        search in "_all" / GmsElasticSearchUtility.typeName
      }.await

      val hits = resp.getHits.getHits
      assert(hits.nonEmpty)

      checkIndicesExist(esClient.client)

      val articlesWithMinutelyMetrics = esQueryService.getMinutelyMetrics(List(minute1, minute2, minute3))
      assert(articlesWithMinutelyMetrics.size == 2)
      val article1Result = articlesWithMinutelyMetrics.find(_.articleId == 1L).getOrElse(fail("Unable to get article id 1"))
      assert(article1Result.totalImpressions == 80)
      assert(article1Result.totalClicks == 23)
      val expectedArticle1TotalCtr = 23.0 / 80
      assert(article1Result.totalCtr == expectedArticle1TotalCtr)
      val article1MinuteMetrics = article1Result.minutelyMetrics
      val article1Minute1Metrics = article1MinuteMetrics.find(_.minuteMillis == minute1).getOrElse(fail("unable to get minute 1 metrics"))
      assert(article1Minute1Metrics.impressions == 15)
      assert(article1Minute1Metrics.clicks == 3)
      assert(article1Minute1Metrics.ctr == (3.0 / 15))
      val article1Minute2Metrics = article1MinuteMetrics.find(_.minuteMillis == minute2).getOrElse(fail("unable to get minute 2 metrics"))
      assert(article1Minute2Metrics.impressions == 40)
      assert(article1Minute2Metrics.clicks == 12)
      assert(article1Minute2Metrics.ctr == (12.0 / 40))
      val article1Minute3Metrics = article1MinuteMetrics.find(_.minuteMillis == minute3).getOrElse(fail("unable to get minute 3 metrics"))
      assert(article1Minute3Metrics.impressions == 25)
      assert(article1Minute3Metrics.clicks == 8)
      assert(article1Minute3Metrics.ctr == (8.0 / 25))
    }
  }
}
