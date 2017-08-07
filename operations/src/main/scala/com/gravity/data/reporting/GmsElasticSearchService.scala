package com.gravity.data.reporting

import com.gravity.data.gms.{ArticleWithMinutelyMetrics, MinutelyMetrics}
import com.gravity.domain.aol.{AolDeliveryMethod, AolGmsArticle}
import com.gravity.domain.gms.{GmsArticleMetrics, GmsArticleStatus}
import com.gravity.interests.jobs.intelligence.{ArticleKey, SiteKey}
import com.gravity.interests.jobs.intelligence.operations.{AolGmsKeyQueryResult, GmsArticleStatusCounts, GmsElasticSearchUtility}
import com.gravity.utilities.grvstrings
import com.gravity.utilities.analytics.DateMidnightRange
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvcoll.Pager
import com.gravity.utilities.grvz._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.web.{NegatableEnum, SortSpec}
import org.apache.commons.pool2.BasePooledObjectFactory
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.script.{Script, ScriptService}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum
import org.elasticsearch.search.aggregations.pipeline.{InternalSimpleValue, PipelineAggregatorBuilders}
import org.elasticsearch.search.sort.SortOrder

import scala.collection.JavaConversions._
import scalaz.Scalaz._
import scalaz._

object GmsElasticSearchService {

  var esqsInstance: GmsElasticSearchService = null

  def apply(): GmsElasticSearchService = {
    if (esqsInstance == null) {
      esqsInstance = new GmsElasticSearchService(new ElasticSearchClientFactory())
    }

    esqsInstance
  }

  val maxResultSize: Int = 10000

  val maxTerms: Int = 1024

}

class GmsElasticSearchService(override val esClientFactory: BasePooledObjectFactory[Client]) extends CanQueryElasticSearch {
 import com.gravity.logging.Logging._

  import GmsElasticSearchService._

  def updateGmsArticle(article: AolGmsArticle, siteKey: SiteKey, refresh: Boolean = false): IndexResponse = {
    withClient { client =>
      GmsElasticSearchUtility.updateGmsArticle(article, siteKey, client, refresh)
    }
  }

  def updateGmsArticles(articles: Traversable[AolGmsArticle], siteKey: SiteKey, refresh: Boolean = false): BulkResponse = {
    withClient { client =>
      GmsElasticSearchUtility.updateGmsArticles(articles, siteKey, client, refresh)
    }
  }

  def updateGmsArticleMetrics(articleMetrics: Traversable[GmsArticleMetrics]): BulkResponse = {
    withClient { client =>
      val U = GmsElasticSearchUtility
      val F = U.MetricsFields
      val bulkRequest = client.prepareBulk()
      articleMetrics.foreach(a => {
        val indexReq = client.prepareIndex(U.metricsIndexName, U.typeNameForMetrics, a.key.keyString)
          .setSource {
            val o = XContentFactory.jsonBuilder().startObject()
            o.field(F.articleId, a.key.articleKey.articleId)
              .field(F.contentGroupId, a.key.contentGroupId)
              .field(F.sitePlacementId, a.key.sitePlacementIdKey.id.raw)
              .field(F.gravityCalculatedPlid, a.key.articleKey.intId)
              .field(F.totalClicks, a.metrics.totalClicks)
              .field(F.totalImpressions, a.metrics.totalImpressions)
              .field(F.totalCtr, a.metrics.totalCtr)
              .field(F.thirtyMinuteCtr, a.metrics.thirtyMinuteCtr)

            o.endObject()
        }.request()

        bulkRequest.add(indexReq)
      })

      client.bulk(bulkRequest.request()).get()
    }
  }

  private val defaultGmsPager = Pager(1, maxResultSize, SortSpec(GmsElasticSearchUtility.MetaFields.dlArticleStatus))

  private val counstByStatusStatusNames = Seq(
    GmsArticleStatus.Live.name, GmsArticleStatus.Approved.name
  )

  private def queryGmsCountsByStatus(client: Client, siteGuid: String, contentGroupId: Option[Long] = None,
    submittedUserId: Option[Int] = None, submittedDate: Option[DateMidnightRange] = None,
    deliveryMethod: Option[AolDeliveryMethod.Type] = None, aolCategory: Option[String] = None,
    aolSource: Option[String] = None): GmsArticleStatusCounts = {
    val counstByStatusStatusNames = Seq(
      GmsArticleStatus.Live.name, GmsArticleStatus.Approved.name
    )

    val F = GmsElasticSearchUtility.MetaFields

    val boolQuery = QueryBuilders.boolQuery()
      .must(QueryBuilders.termsQuery(F.siteGuid, siteGuid))
      .must(QueryBuilders.termsQuery(F.dlArticleStatus, counstByStatusStatusNames: _*))

    contentGroupId.foreach(cgid => boolQuery.must(QueryBuilders.termsQuery(F.contentGroupId(cgid), 1)))

    submittedUserId.foreach(uid => boolQuery.must(QueryBuilders.termsQuery(F.dlArticleSubmittedUserId, uid)))

    deliveryMethod.foreach(dm => boolQuery.must(QueryBuilders.termsQuery(F.deliveryMethod, dm.id.toInt)))

    aolCategory.foreach(cat => boolQuery.must(QueryBuilders.termsQuery(F.aolCategory, cat)))

    aolSource.foreach(src => boolQuery.must(QueryBuilders.termsQuery(F.aolSource, src)))

    submittedDate.foreach(dmr => {
      val rangeQuery = QueryBuilders.rangeQuery(F.dlArticleSubmittedTime)
        .from(dmr.fromInclusive.getEpochSeconds)
        .to(dmr.toInclusive.plusDays(1).getEpochSeconds)

      boolQuery.must(rangeQuery)
    })

    val searchRequest = client.prepareSearch(GmsElasticSearchUtility.siteBasedMetaIndexName(SiteKey(siteGuid)))
      .setTypes(GmsElasticSearchUtility.typeNameForMeta)
      .setQuery(boolQuery)
      .setSize(defaultGmsPager.pageSize)
      .setFrom(defaultGmsPager.from)
      .addSort(defaultGmsPager.sort.property, if (defaultGmsPager.sort.asc) SortOrder.ASC else SortOrder.DESC)

    ifTrace({
      println("ElasticSearch Query:")
      println(searchRequest.toString)
    })

    val searchResponse = searchRequest.execute().actionGet()

    val countsByStatus = searchResponse.getHits.getHits.foldLeft(Map[String, Int]()) {
      case (countsByStatusAccum, hit) =>
        Option(hit.getSource().get("dlArticleStatus")).map(_.toString) match {
          case Some(status) => countsByStatusAccum |+| Map(status -> 1)
          case None => countsByStatusAccum
        }
    }

    GmsArticleStatusCounts(countsByStatus)
  }

  def queryGmsArticles(siteGuid: String, pager: Pager, contentGroupId: Option[Long] = None,
    searchTerms: Set[String] = Set.empty,
    status: Set[NegatableEnum[GmsArticleStatus.Type]] = Set.empty,
    submittedUserId: Option[Int] = None, submittedDate: Option[DateMidnightRange] = None,
    deliveryMethod: Option[AolDeliveryMethod.Type] = None, aolCategory: Option[String] = None,
    aolSource: Option[String] = None): ValidationNel[FailureResult, AolGmsKeyQueryResult] = {

    val F = com.gravity.interests.jobs.intelligence.operations.GmsElasticSearchUtility.MetaFields

    val boolQuery = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(F.siteGuid, siteGuid))

    contentGroupId.foreach(cgid => boolQuery.must(QueryBuilders.termsQuery(F.contentGroupId(cgid), 1)))

    searchTerms.foreach(_.toLowerCase match {
      case probablyUrl if probablyUrl.startsWith("http://") || probablyUrl.startsWith("https://") =>
        boolQuery.must(QueryBuilders.termsQuery(F.url, probablyUrl))

      case lowercasedTerm =>
        val searchTermBoolQ = QueryBuilders.boolQuery().minimumNumberShouldMatch(1)

        searchTermBoolQ.should(QueryBuilders.fuzzyQuery(F.title, lowercasedTerm))

        val wildCardTerm = s"*$lowercasedTerm*"
        searchTermBoolQ.should(QueryBuilders.wildcardQuery(F.aolSource, wildCardTerm))
        searchTermBoolQ.should(QueryBuilders.wildcardQuery(F.aolCategory, wildCardTerm))

        boolQuery.must(searchTermBoolQ)
    })

    val (positiveStatuses, negativeStatuses) = status.partition(_.isPositive) match {
      case (pos, neg) => pos.map(_.value.name) -> neg.map(_.value.name)
    }

    if (positiveStatuses.nonEmpty) {
      boolQuery.must(QueryBuilders.termsQuery(F.dlArticleStatus, positiveStatuses.toSeq: _*))
    }

    if (negativeStatuses.nonEmpty) {
      boolQuery.mustNot(QueryBuilders.termsQuery(F.dlArticleStatus, negativeStatuses.toSeq: _*))
    }

    submittedUserId.foreach(uid => boolQuery.must(QueryBuilders.termsQuery(F.dlArticleSubmittedUserId, uid)))

    deliveryMethod.foreach(dm => boolQuery.must(QueryBuilders.termsQuery(F.deliveryMethod, dm.id.toInt)))

    aolCategory.foreach(cat => boolQuery.must(QueryBuilders.termsQuery(F.aolCategory, cat.toLowerCase)))

    aolSource.foreach(src => boolQuery.must(QueryBuilders.termsQuery(F.aolSource, src.toLowerCase)))

    submittedDate.foreach(dmr => {
      val rangeQuery = QueryBuilders.rangeQuery(F.dlArticleSubmittedTime)
        .from(dmr.fromInclusive.getEpochSeconds)
        .to(dmr.toInclusive.plusDays(1).getEpochSeconds)

      boolQuery.must(rangeQuery)
    })

    withClient { client =>
      tryToSuccessNEL(
        {
          val searchRequest = client.prepareSearch(GmsElasticSearchUtility.siteBasedMetaIndexName(SiteKey(siteGuid)))
            .setTypes(GmsElasticSearchUtility.typeNameForMeta)
            .setQuery(boolQuery)
            .setSize(pager.pageSize)
            .setFrom(pager.from)
            .addSort(pager.sort.property, if (pager.sort.asc) SortOrder.ASC else SortOrder.DESC)

          ifTrace({
            println("ElasticSearch Query:")
            println(searchRequest.toString)
          })

          val searchResponse = searchRequest.execute().actionGet()

          ifTrace({
            println()
            println("*" * 50)
            searchResponse.getHits.hits().foreach { hit =>
              val map = hit.sourceAsMap()
              print(f"${map.get("thirtyMinuteCtr1996").asInstanceOf[Double] * 100}%2.4f")
              println(" => " + map.get("title"))
              println("\t" + map.get("url"))
            }
            println("*" * 50)
            println()
          })

          val articleKeys = searchResponse.getHits.hits().flatMap(_.getId.tryToLong).map(id => ArticleKey(id)).toList
          val totalHits = searchResponse.getHits.totalHits().toInt

          val countsByStatus = queryGmsCountsByStatus(client, siteGuid, contentGroupId, submittedUserId, submittedDate, deliveryMethod, aolCategory, aolSource)

          AolGmsKeyQueryResult(pager.numberOfPages(totalHits), articleKeys, totalHits, countsByStatus)
        },
        ex => FailureResult("Failed to execute elastic search query!", ex)
      )
    }
  }

  private val emptySimilarArticleSuccess: ValidationNel[FailureResult, Seq[ArticleKey]] = Seq.empty[ArticleKey].successNel[FailureResult]
  private val similarArticleRequiredStatuses = Seq(GmsArticleStatus.Live.name, GmsArticleStatus.Approved.name, GmsArticleStatus.Pending.name)

  def querySimilarByTitleGmsArticles(similarToArticle: AolGmsArticle, siteGuid: String): ValidationNel[FailureResult, Seq[ArticleKey]] = {
    val words = grvstrings.tokenizeBoringly(similarToArticle.title.toLowerCase, " ").map(_.trim).filter(_.length >= 4).distinct.toSeq

    if (words.isEmpty) return emptySimilarArticleSuccess

    val F = com.gravity.interests.jobs.intelligence.operations.GmsElasticSearchUtility.MetaFields

    val boolQuery = QueryBuilders.boolQuery()
      .must(QueryBuilders.termQuery(F.siteGuid, siteGuid))
      .mustNot(QueryBuilders.termQuery(F.articleId, similarToArticle.articleId))
      .must(QueryBuilders.termsQuery(F.dlArticleStatus, similarArticleRequiredStatuses: _*))

    val wordQuery = QueryBuilders.boolQuery().minimumNumberShouldMatch(2)

    words.foreach { word =>
      wordQuery.should(QueryBuilders.termQuery(F.title, word))
    }

    boolQuery.must(wordQuery)

    withClient { client =>
      tryToSuccessNEL(
        {
          val searchRequest = client.prepareSearch(GmsElasticSearchUtility.siteBasedMetaIndexName(SiteKey(siteGuid)))
            .setTypes(GmsElasticSearchUtility.typeNameForMeta)
            .setQuery(boolQuery)
            .setSize(maxResultSize)

          ifTrace({
            println("ElasticSearch Query:")
            println(searchRequest.toString)
          })

          val searchResponse = searchRequest.execute().actionGet()
          searchResponse.getHits.hits().flatMap(_.getId.tryToLong).map(id => ArticleKey(id)).toSeq
        },
        ex => FailureResult("Failed to execute elastic search query!", ex))
    }

  }

  def getMinutelyMetrics(minutes: Seq[Long]): Seq[ArticleWithMinutelyMetrics] = {

    val indices = minutes.map { minute =>
      GmsElasticSearchUtility.indexBaseName + minute
    }

    val queryMinutes = QueryBuilders.termsQuery("millis", minutes: _*)
    val boolQuery = QueryBuilders.boolQuery().must(queryMinutes)

    val bucketsPathsMap = Map(
      "total_clicks_per_article" -> "total_clicks_per_article",
      "total_impressions_per_article" -> "total_impressions_per_article"
    )

    // This way is working off the config/scripts dir being set, cannot actually set this locally for testing b/c our
    // client doesn't have the groovy module installed.
    // This works when the script exists on the nodes.
    val calculate_ctr_script = new Script("calculate_ctr", ScriptService.ScriptType.FILE, "groovy", bucketsPathsMap)
    val bs = PipelineAggregatorBuilders
      .bucketScript("total_ctr")
      .setBucketsPathsMap(bucketsPathsMap)
      .script(calculate_ctr_script)

    // This works when inline scripting is turned on
    //    val bs = PipelineAggregatorBuilders
    //      .bucketScript("total_ctr")
    //      .setBucketsPathsMap(bucketsPathsMap)
    //      .script(new Script("total_clicks_per_article / total_impressions_per_article"))

    val termsBuilder = AggregationBuilders.terms("by_article").field("id")
      // This works for sorting on a calculated metric, so why doesn't total_ctr work???
      //    val termsBuilder = AggregationBuilders.terms("by_article").field("id").order(Terms.Order.aggregation("total_clicks_per_article", true))
      // This doesn't work :( :( :(
      //    val termsBuilder = AggregationBuilders.terms("by_article").field("id").order(Terms.Order.aggregation("total_ctr", true))
      .subAggregation(AggregationBuilders.sum("total_impressions_per_article").field("impressions"))
      .subAggregation(AggregationBuilders.sum("total_clicks_per_article").field("clicks"))
      .subAggregation(bs)

      // When I tried it this way, it returned nothing, no matter which of the below versions I tried.
      //      .subAggregation(AggregationBuilders
      //        .scriptedMetric("total_ctr")
      //        .mapScript(new Script("profit = 201; return profit"))
      //        .initScript(new Script("_agg['tcpa'] = []; _agg['tipa'] = []"))
      //        .mapScript(new Script("_agg.tcpa.add(doc['total_clicks_per_article'].value); _agg.tipa.add(doc['total_impressions_per_article'].value)"))
      //        .reduceScript(new Script("total_ctr = 0.0; total_ctr = _agg.tcpa / _agg.tipa; return total_ctr"))
      //    )
      //        .mapScript(new Script("total_clicks_per_article / total_impressions_per_article")))

      .subAggregation(AggregationBuilders.terms("by_minute").field("millis")
      .subAggregation(AggregationBuilders.sum("impressions_sum").field("impressions"))
      .subAggregation(AggregationBuilders.sum("clicks_sum").field("clicks"))
    )

    withClient { client =>
      val searchRequestBuilder = client
        .prepareSearch(indices: _*)
        .setTypes(GmsElasticSearchUtility.typeNameForMeta)
        .setQuery(boolQuery)
        .addAggregation(termsBuilder)
        // This doesn't work, says 'Caused by: SearchParseException[No mapping found for [total_ctr] in order to sort on]'
        //.addSort("total_ctr", SortOrder.ASC)
        // This actually doesn't fail, but it doesn't sort either.
        //        .addSort(SortBuilders.scriptSort(calculate_ctr_script, "number").order(SortOrder.DESC))
        // doesn't fail but doesn't sort either
        //.addSort(SortBuilders.scriptSort(new Script("total_clicks_per_article / total_impressions_per_article"), "number").order(SortOrder.DESC))
        // Holy crap this isn't doing anything, it doesn't fail when using a fake field either?!?!?!
        //        .addSort(SortBuilders.scriptSort(new Script("total_clicks_per_article_fake / total_impressions_per_article"), "number").order(SortOrder.DESC))
        .setSize(0) // we don't want the documents to be returned outside of aggregation since they are unused

      println(searchRequestBuilder.toString)

      val searchResponse = searchRequestBuilder
        .execute()
        .actionGet()

      val byArticleAggregations = searchResponse.getAggregations.getAsMap.get("by_article")
      val articleBuckets = byArticleAggregations.asInstanceOf[LongTerms].getBuckets

      val articlesWithMinutelyMetrics = articleBuckets.toList.map { bucket =>
        val articleId = bucket.getKeyAsNumber.longValue()

        val valuesForThisArticle = bucket.getAggregations
        val bucketAggregationsAsMap = valuesForThisArticle.getAsMap

        val totalImpsAggregation = bucketAggregationsAsMap.get("total_impressions_per_article")
        val totalImps = totalImpsAggregation.asInstanceOf[InternalSum].getValue

        val totalClicksAggregation = bucketAggregationsAsMap.get("total_clicks_per_article")
        val totalClicks = totalClicksAggregation.asInstanceOf[InternalSum].getValue

        val totalCtrAggregation = bucketAggregationsAsMap.get("total_ctr")
        val totalCtr = totalCtrAggregation.asInstanceOf[InternalSimpleValue].getValue

        val minutelyMetricsTerms = bucketAggregationsAsMap.get("by_minute")
        val minutelyMetricsBuckets = minutelyMetricsTerms.asInstanceOf[LongTerms].getBuckets
        val minutelyMetrics = minutelyMetricsBuckets.toList.map { b =>
          val bucketMinute = b.getKeyAsNumber
          val minuteBucketValues = b.getAggregations
          val minuteBucketValuesAsMap = minuteBucketValues.getAsMap
          val minuteImpsSum = minuteBucketValuesAsMap.get("impressions_sum")
          val minuteImps = minuteImpsSum.asInstanceOf[InternalSum].getValue
          val minuteClicksSum = minuteBucketValuesAsMap.get("clicks_sum")
          val minuteClicks = minuteClicksSum.asInstanceOf[InternalSum].getValue
          val ctr =
            if (minuteImps > 0) minuteClicks / minuteImps else 0.0d
          MinutelyMetrics(bucketMinute.longValue(), minuteImps.toLong, minuteClicks.toLong, ctr)
        }

        ArticleWithMinutelyMetrics(articleId, totalImps.toLong, totalClicks.toLong, totalCtr.toDouble, minutelyMetrics)
      }

      articlesWithMinutelyMetrics
    }
  }

  def lookupArticleKeysForPlids(plids: Set[Int]): Map[Int, ArticleKey] = {
    if (plids.size > maxTerms) {
      plids.grouped(maxTerms).foldLeft(Map.empty[Int, ArticleKey]) {
        case (aggregator: Map[Int, ArticleKey], batchOfPlids: Set[Int]) =>
          aggregator ++ lookupArticleKeysForPlidsImpl(batchOfPlids)
      }
    }
    else {
      lookupArticleKeysForPlidsImpl(plids)
    }
  }

  private def lookupArticleKeysForPlidsImpl(plids: Set[Int]): Map[Int, ArticleKey] = {
    val F = com.gravity.interests.jobs.intelligence.operations.GmsElasticSearchUtility.MetaFields

    val boolQuery = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(F.gravityCalculatedPlid, plids.toSeq: _*))

    withClient { client =>
      val searchRequest = client.prepareSearch("gms_article*")
        .setTypes(GmsElasticSearchUtility.typeNameForMeta)
        .setQuery(boolQuery)
        .setSize(maxResultSize)

      ifTrace({
        println("ElasticSearch Query:")
        println(searchRequest.toString)
      })

      val searchResponse = searchRequest.execute().actionGet()

      (for {
        hit <- searchResponse.getHits.hits()
        articleId <- hit.getId.tryToLong
        plid <- Option(hit.getSource.get(F.gravityCalculatedPlid)).flatMap(_.toString.tryToInt)
      } yield {
        plid -> ArticleKey(articleId)
      }).toMap
    }
  }

}
