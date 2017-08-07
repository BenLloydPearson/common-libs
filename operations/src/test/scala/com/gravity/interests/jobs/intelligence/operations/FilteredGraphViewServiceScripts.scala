package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.{ArticleKey, Schema, StoredGraph}

import scala.io.Source

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 12/3/13
 * Time: 1:54 PM
 * To change this template use File | Settings | File Templates.
 */

object filteredGraphViewServiceTestIT extends App {
  val path = "/com/gravity/interests/jobs/intelligence/algorithms/graphing/topic_uri_blacklist.txt"
  val topics = FilteredGraphViewServiceIT.resourceToSet(path)
  println("num topics: " + topics.size)
}

object testFilteredGraph extends App {
  val urls = Seq(
    "http://ca.news.yahoo.com/hamilton-woman-loses-lotto-ticket-still-set-win-152743993.html",
    "http://uk.news.yahoo.com/china-39-39-uk-no-big-power-39-103710090.html",
    "http://ca.finance.yahoo.com/news/wireless-code-gives-customers-rights-starting-today-105627053.html"
  )

  for (url <- urls;
       cg <- FilteredGraphViewServiceIT.getGraph(url);
       fg = FilteredGraphViewService.toFilteredGraph(cg)) {

    println("orig graph")
    cg.prettyPrintNodes(true)
    println("filtered graph")
    fg.prettyPrintNodes(true)

  }
}

object FilteredGraphViewServiceIT {
  implicit val conf = HBaseConfProvider.getConf.defaultConf

  def resourceToSet(fullPath: String) = Source.fromInputStream(getClass.getResourceAsStream(fullPath)).getLines().toSet

  def getGraph(url: String): Option[StoredGraph] = {
    val ak = ArticleKey(url)
    Schema.Articles.query2.withKey(ak).withFamilies(_.meta, _.storedGraphs).singleOption() match {
      case Some(article) => {
        article.column(_.conceptStoredGraph) match {
          case Some(graph) => {
            graph.populateUriAndName()
            Some(GraphAnalysisService.copyGraphToTfIdfGraph(graph))
          }
          case None => {
            println("Article has no graph")
            None
          }
        }
      }
      case None =>
        println("Article key not found!")
        None
    }
  }
}
