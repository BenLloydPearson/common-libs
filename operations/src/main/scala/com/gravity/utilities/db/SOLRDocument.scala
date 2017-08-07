package com.gravity.utilities.db

import org.apache.solr.common.SolrInputDocument
import com.gravity.interests.jobs.intelligence.{Schema, NodeType, ArticleRow}
import org.apache.solr.client.solrj.impl.{XMLResponseParser, HttpSolrServer}

/**
 * User: mtrelinski
 * Date: 7/9/13
 */

object ArticleToDocument {

  def baseQuery = Schema.Articles.query2.withFamilies(_.meta, _.campaigns, _.siteSections, _.storedGraphs)

  val urlBase = "http://solrmaster:8080/solr"

  def getSolrServer() = { val s = new HttpSolrServer(urlBase) ; s.setParser(new XMLResponseParser) ; s }

  def create(row: ArticleRow): SolrInputDocument = {
    val thisDoc = new SolrInputDocument()

    thisDoc.addField("articleId", row.articleKey.articleId)
    thisDoc.addField("title", row.title)
    thisDoc.addField("siteGuid", row.siteGuid)
    // this is the date format to use: 1995-12-31T23:59:59Z
    thisDoc.addField("publishTime", row.publishTime.toString("yyyy-MM-dd'T'HH:mm:ss'Z'"))

    row.conceptGraph.nodes.foreach {
      node =>
        if (node.nodeType == NodeType.Topic)
          thisDoc.addField("topicNode", node.name, node.score.toFloat)
        else if (node.nodeType == NodeType.Interest)
          thisDoc.addField("conceptNode", node.name, node.score.toFloat)
    }

    row.siteSectionPaths.foreach {
      siteSectionPath =>
        val (siteKey, sectionPath) = siteSectionPath
        sectionPath.paths.foreach {
          section =>
            thisDoc.addField("section", section)
        }
    }

    row.campaigns.foreach {
      campaign =>
        val (campaignKey, campaignStatus) = campaign
        thisDoc.addField("campaign", campaignKey.campaignId)
    }

    thisDoc
  }

}
