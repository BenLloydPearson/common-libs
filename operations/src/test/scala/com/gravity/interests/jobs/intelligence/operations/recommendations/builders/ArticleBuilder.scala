package com.gravity.interests.jobs.intelligence.operations.recommendations.builders

import com.gravity.hbase.mapreduce._
import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.operations.ImageWritingMonitor
import org.apache.hadoop.io.BytesWritable
import com.gravity.interests.jobs.intelligence.Schema._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import scala.collection.JavaConversions._
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.StoredGraphExampleUris._
import com.gravity.utilities.grvtime._
import com.gravity.utilities.time.{GrvDateMidnight, DateHour}
import org.joda.time.DateTime
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestEnvironment

/**
 * Created by Jim Plush
 * User: jim
 * Date: 4/18/12
 */

class ArticleBuilder extends HBaseTestEnvironment {

  var debug: Boolean = false
  var siteGuid: String = ""
  var articleUrl: String = ""
  var publishTime: DateTime = new DateTime()
  var articleCategory: String = ""
  var articleContent: String = "empty content from articlebuilder"
  var articleGraph: StoredGraph = getDefaultGraph
  var articleImage: String = "http://jim.com/pic1.jpg"
  var articleTitle: String = "The cat in the hat"
  var articleStandardHourlyMetrics: Map[DateHour, StandardMetrics] = Map(new GrvDateMidnight().toDateHour -> StandardMetrics(10, 9, 8, 7, 1))

  def withDebugOn(on:Boolean=true) = {
    debug = on
    this
  }

  def withSiteGuid(guid: String) = {
    siteGuid = guid
    this
  }

  def withPublishDate(dt: DateTime) = {
    publishTime = dt
    this
  }

  def withUrl(url: String) = {
    articleUrl = url
    this
  }

  def withTitle(title: String) = {
    articleTitle = title
    this
  }

  def withContent(content: String) = {
    articleContent = content
    this
  }

  def withImage(image: String) = {
    articleImage = image
    this
  }

  def withCategory(category: String) = {
    articleCategory = category
    this
  }

  def withGraph(graph: StoredGraph) = {
    articleGraph = graph
    this
  }

  def withStandardHourlyMetrics(metrics: Map[DateHour, StandardMetrics]) = {
    articleStandardHourlyMetrics = metrics
    this
  }

  def getDefaultGraph = {
    def defaultGraph = StoredGraph.make
        .addNode(cats, "Cats", NodeType.Topic, count = 2, score = 0.5)
        .addNode(pets, "Pets", NodeType.Interest, level = 1, count = 2, score = 0.4)
        .addNode(lakers, "Lakers", NodeType.Topic, count = 2, score = 0.3)
        .addNode(basketball, "Basketball", NodeType.Interest, level = 2, score = 0.2)
        .addNode(sports, "Sports", NodeType.Interest, level = 1)
        .addNode(home, "Home", NodeType.Interest, level = 1)
        .addNode(fridges, "Fridges", NodeType.Topic)
        .addNode(appliances, "Appliances", NodeType.Interest, level = 2)
        .addNode(engineering, "Engineering", NodeType.Interest, level = 1)
        .relate(cats, pets)
        .relate(pets, home, EdgeType.BroaderThan, count = 2)
        .relate(lakers, basketball, count = 2)
        .relate(basketball, sports, EdgeType.BroaderThan)
        .relate(fridges, appliances)
        .relate(appliances, engineering, EdgeType.BroaderThan)
        .relate(fridges, home)
        .build
    defaultGraph
  }


  def build() = {
    if (debug) println("Building article: " + articleUrl)
    val articleKey = ArticleKey(articleUrl)

    Schema.Articles.delete(articleKey).execute()

    ImageWritingMonitor.noteWritingArticleImage(articleUrl, articleImage)   // In test

    val ops = Schema.Articles.put(articleKey)
        .value(_.url, articleUrl)
        .value(_.title, articleTitle)
        .value(_.tags, CommaSet(Set("Google", "MySpace", "Facebook", "Friendster")))
        .value(_.image, articleImage)
        .value(_.content, siteGuid)
        .value(_.siteGuid, siteGuid)
        .value(_.publishTime, publishTime)
        .value(_.category, articleCategory)
        .value(_.autoStoredGraph, articleGraph)
        .valueMap(_.standardMetricsHourlyOld, articleStandardHourlyMetrics)
        .execute()

    if (ops.numPuts == 1) {
      if (debug) println("Article Successfully created: " + articleUrl)
      articleKey
    } else {
      throw new Exception("User was unable to be created and saved to cluster")
    }


  }


}
