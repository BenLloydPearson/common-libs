package com.gravity.interests.jobs.intelligence.operations

import cascading.tuple.Fields
import com.gravity.domain.aol.AolGmsArticle
import com.gravity.interests.jobs.intelligence.SiteKey
import com.gravity.utilities.grvstrings._
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.{Settings => ESSettings}
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}

/**
  * Created by robbie on 09/06/2016.
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
object GmsElasticSearchUtility {

  val indexBaseName: String = "gms_articles_"

  private val metaIndexName: String = indexBaseName + "meta"
  val metricsIndexName: String = indexBaseName + "metrics"

  def siteBasedMetaIndexName(sk: SiteKey): String = s"${metaIndexName}_${sk.siteId}"

  def minuteBasedIndexName(minute: Long): String = indexBaseName + minute.toString

  val typeNameForMeta: String = "gms_article"
  val typeNameForMetrics: String = "gms_article_metrics"

  object MetaFields {
    val url = "url"
    val title = "title"
    val siteId = "siteId"
    val articleId = "articleId"
    val gravityCalculatedPlid = "gravityCalculatedPlid"
    val siteGuid = "siteGuid"
    val aolCategory = "aolCategory"
    val aolSource = "aolSource"
    val publishOnDate = "publishOnDate"
    val endDate = "endDate"
    val dlArticleStatus = "dlArticleStatus"
    val deliveryMethod = "deliveryMethod"
    val dlArticleLastGoLiveTime = "dlArticleLastGoLiveTime"
    val dlArticleSubmittedUserId = "dlArticleSubmittedUserId"
    val dlArticleApprovedRejectedByUserId = "dlArticleApprovedRejectedByUserId"
    val dlArticleSubmittedTime = "dlArticleSubmittedTime"
    val updatedTime = "updatedTime"
    val clicksTotal = "clicksTotal"
    val impressionsTotal = "impressionsTotal"
    val ctrTotal = "ctrTotal"

    def contentGroupId(cgId: Long): String = contentGroupId(cgId.toString)
    def contentGroupId(cgId: String): String = "contentGroupId_" + cgId

    def thirtyMinuteCtr(cgId: Long): String = thirtyMinuteCtr(cgId.toString)
    def thirtyMinuteCtr(cgId: String): String = "thirtyMinuteCtr" + cgId

    def impressionsTotalForContentGroup(cgId: Long): String = impressionsTotalForContentGroup(cgId.toString)
    def impressionsTotalForContentGroup(cgId: String): String = impressionsTotal + cgId

    def clicksTotalForContentGroup(cgId: Long): String = clicksTotalForContentGroup(cgId.toString)
    def clicksTotalForContentGroup(cgId: String): String = clicksTotal + cgId

    def ctrTotalForContentGroup(cgId: Long): String = ctrTotalForContentGroup(cgId.toString)
    def ctrTotalForContentGroup(cgId: String): String = ctrTotal + cgId
  }

  object MetricsFields {
    val articleId: String = "articleId"
    val contentGroupId: String = "contentGroupId"
    val sitePlacementId: String = "sitePlacementId"
    val gravityCalculatedPlid: String = "gravityCalculatedPlid"
    val totalClicks: String = "totalClicks"
    val totalImpressions: String = "totalImpressions"
    val totalCtr: String = "totalCtr"
    val thirtyMinuteCtr: String = "thirtyMinuteCtr"
  }

  val fieldNames: Seq[String] = Seq(
     MetaFields.url
    ,MetaFields.title
    ,MetaFields.siteId
    ,MetaFields.articleId
    ,MetaFields.gravityCalculatedPlid
    ,MetaFields.siteGuid
    ,MetaFields.aolCategory
    ,MetaFields.aolSource
    ,MetaFields.publishOnDate
    ,MetaFields.endDate
    ,MetaFields.dlArticleStatus
    ,MetaFields.deliveryMethod
    ,MetaFields.dlArticleLastGoLiveTime
    ,MetaFields.dlArticleSubmittedUserId
    ,MetaFields.dlArticleApprovedRejectedByUserId
    ,MetaFields.dlArticleSubmittedTime
    ,MetaFields.updatedTime
    ,MetaFields.clicksTotal
    ,MetaFields.impressionsTotal
    ,MetaFields.ctrTotal
  )

  val fieldTypes: Seq[java.lang.reflect.Type] = Seq(
     classOf[String]
    ,classOf[String]
    ,classOf[Long]
    ,classOf[Long]
    ,classOf[Int]
    ,classOf[String]
    ,classOf[String]
    ,classOf[String]
    ,classOf[Int]
    ,classOf[Long]
    ,classOf[String]
    ,classOf[Int]
    ,classOf[Int]
    ,classOf[Int]
    ,classOf[Int]
    ,classOf[Int]
    ,classOf[Int]
    ,classOf[Long]
    ,classOf[Long]
    ,classOf[Double]
  )

  private val fields: Fields = new Fields(fieldNames:_*).applyTypes(fieldTypes:_*)

  private val esMapping: XContentBuilder = {
    val builder: XContentBuilder = XContentFactory.jsonBuilder()

    // set up the source config and properties config
    builder.startObject().startObject(typeNameForMeta)
      // disable the giant concat field as we aren't using full text
      .startObject("_all").field("enabled", false).endObject()
      .startObject("properties")

    // add each of the fields and their types
    for (i <- 0 until fields.size()) {
      val name = fields.get(i).toString

      val jtype = fields.getType(i).toString match {
        case "class java.lang.String" => "string"
        case "int" => "integer"
        case allOtherTypes => allOtherTypes
      }


      builder.startObject(name).field("type", jtype)
      // Add any field name you wish to be analyzed to the `if` criteria below
      if (name == "title") {
        builder.field("index", "analyzed")
      }
      else {
        // set doc values to true to enable columnar storage for types that it's not on by default and disable tokenization as it's not needed
        builder.field("doc_values", true).field("index", "not_analyzed")
      }

      builder.endObject()
    }

    // finish closing
    builder.endObject().endObject().endObject()
  }

  private val esSettings = ESSettings.settingsBuilder().put("index.requests.cache.enable", true).build()

  def createSiteSpecificMetaIndex(esClient: Client, siteKey: SiteKey): CreateIndexResponse = {
    createMetaIndex(esClient, siteBasedMetaIndexName(siteKey))
  }

  def createMetaIndex(esClient: Client, indexName: String = metaIndexName): CreateIndexResponse = {
    esClient.admin().indices()
      .prepareCreate(indexName)
      .setSettings(esSettings)
      .addMapping(typeNameForMeta, esMapping)
      .get()
  }

  def updateMinuteMetricForArticle(
    articleId: Long,
    minuteMillis: Long,
    impressions: Long,
    clicks: Long,
    ctr: Double): IndexDefinition = {
    val indexNameWithMillis = indexBaseName + minuteMillis

    // Indexing a document in without explicitly having created the index prior to this.
    val indexDefinition = index into indexNameWithMillis / typeNameForMeta id articleId fields(
      "id" -> articleId,
      "millis" -> minuteMillis,
      "impressions" -> impressions,
      "clicks" -> clicks,
      "ctr" -> ctr
      )

    indexDefinition
  }

  def updateGmsArticles(articles: Traversable[AolGmsArticle], siteKey: SiteKey, esClient: Client, refresh: Boolean = false): BulkResponse = {
    val bulkRequest = esClient.prepareBulk()
    articles.foreach(article => {
      bulkRequest.add(createIndexRequest(article, siteKey, esClient, refresh = false)).setRefresh(refresh)
    })
    esClient.bulk(bulkRequest.request()).get()
  }

  def updateGmsArticle(article: AolGmsArticle, siteKey: SiteKey, esClient: Client, refresh: Boolean = false): IndexResponse = {
    esClient.index(createIndexRequest(article, siteKey, esClient, refresh)).get()
  }

  private def createIndexRequest(article: AolGmsArticle, siteKey: SiteKey, esClient: Client, refresh: Boolean = false): IndexRequest = {
    val F = MetaFields
    val lowerCaseTitle = article.title.trim.trimLeft(''').toLowerCase
    val lowerAolCategory = article.aolCategory.trim.toLowerCase
    val lowerAolSource = article.aolSource.trim.toLowerCase
    val lowerCaseUrl = article.url.trim.toLowerCase

    val indexName = siteBasedMetaIndexName(siteKey)

    esClient.prepareIndex(indexName, typeNameForMeta, article.articleId)
      .setSource {
        val o = XContentFactory.jsonBuilder().startObject()
        o.field(F.url, lowerCaseUrl)
          .field(F.title, lowerCaseTitle)
          .field(F.siteId, siteKey.siteId)
          .field(F.articleId, article.articleId)
          .field(F.gravityCalculatedPlid, article.articleKey.intId)
          .field(F.aolCategory, lowerAolCategory)
          .field(F.aolSource, lowerAolSource)
          .field(F.publishOnDate, article.publishOnDate)
          .field(F.dlArticleStatus, article.dlArticleStatus)
          .field(F.deliveryMethod, article.deliveryMethod.id.toInt)
          .field(F.dlArticleLastGoLiveTime, article.dlArticleLastGoLiveTime)
          .field(F.dlArticleSubmittedUserId, article.dlArticleSubmittedUserId)
          .field(F.dlArticleSubmittedTime, article.dlArticleSubmittedTime)
          .field(F.updatedTime, article.updatedTime)

        article.dlArticleApprovedRejectedByUserId.foreach(id => o.field(F.dlArticleApprovedRejectedByUserId, id))

        SiteService.siteGuid(siteKey).foreach(sg => o.field(F.siteGuid, sg))

        article.metrics.get("total").foreach(m => {
          if (m.lifetime.clicks > 0L) o.field(F.clicksTotal, m.lifetime.clicks)
          if (m.lifetime.impressions > 0L) o.field(F.impressionsTotal, m.lifetime.impressions)
          if (m.lifetime.ctr != 0D) o.field(F.ctrTotal, m.lifetime.ctr)
        })

        // content group specific (dynamic) fields
        article.contentGroupIds.map(_.toString).foreach { cgIdStr =>
          o.field(F.contentGroupId(cgIdStr), 1)
          article.metrics.get(cgIdStr).foreach { m =>
            if (m.lifetime.clicks > 0L) o.field(F.clicksTotalForContentGroup(cgIdStr), m.lifetime.clicks)
            if (m.lifetime.impressions > 0L) o.field(F.impressionsTotalForContentGroup(cgIdStr), m.lifetime.impressions)
            if (m.lifetime.ctr != 0D) o.field(F.ctrTotalForContentGroup(cgIdStr), m.lifetime.ctr)
            if (m.thirtyMinute.ctr != 0D) o.field(F.thirtyMinuteCtr(cgIdStr), m.thirtyMinute.ctr)
          }
        }

        o.endObject()
      }.setRefresh(refresh).request()
  }
}
