package com.gravity.test

import com.gravity.data.reporting.{ElasticSearchClientFactory, GmsElasticSearchService}
import com.gravity.interests.jobs.intelligence.operations.GmsElasticSearchUtility
import com.sksamuel.elastic4s.ElasticDsl._
import org.apache.commons.pool2.PooledObject
import org.elasticsearch.client.Client

trait ElasticSearchTesting {

  this: operationsTesting =>

  def testGmsElasticSearchService(etp: ElasticTestProperties): GmsElasticSearchService = {
    val esClientFactory = new ElasticSearchClientFactory(etp.clusterName, etp.clusterFullName, etp.transportPort) {
      override def create(): Client = etp.client.client

      override def validateObject(p: PooledObject[Client]): Boolean = true
    }

    new GmsElasticSearchService(esClientFactory)
  }

  def withPopulatedGmsArticleIndex(articleContexts: GmsArticleContexts)(work: ElasticTestProperties => Unit): Unit = {
//    withLocallyRunningExternalProcessElasticSearch { etp =>
    withAwsDevElasticSearch { etp =>
      val articleIndexDefinitions = articleContexts.contexts.map { c =>
        GmsElasticSearchUtility.updateMinuteMetricForArticle(c.articleId, c.millis, c.impressions, c.clicks, c.ctr)
      }

      val bulkResult = etp.client.execute {
        bulk(articleIndexDefinitions: _*)
      }.await

      assert(!bulkResult.hasFailures)
      assert(bulkResult.hasSuccesses)

      val wildcardIndex = GmsElasticSearchUtility.indexBaseName + "*"
      val flushResponse = etp.client.execute {
        flush index wildcardIndex
      }.await

      assert(flushResponse.getSuccessfulShards > 0)

      work(etp)
    }
  }

}