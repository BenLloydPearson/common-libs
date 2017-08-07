package com.gravity.interests.jobs.intelligence.operations


import com.gravity.interests.jobs.hbase.HBaseConfProvider
import org.junit.Test
import com.gravity.interests.jobs.intelligence.{ArticleKey, Schema, StoredGraph, StoredGraphDisplayDepth}
import com.gravity.ontology.OntologyGraphName
import com.gravity.utilities.Settings
import org.joda.time.DateTime

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 11/7/13
 * Time: 5:48 PM
 * To change this template use File | Settings | File Templates.
 */

object testNarrowWideGraphing extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  // custom article exists
  //    val article = "http://money.cnn.com/2013/11/14/news/economy/obamacare-insurance"
  //    val article = "http://uk.news.yahoo.com/sirga-lioness-friendship-botswana-valentin-gruener-and-mikkel-legarth-153107523.html"
  //    val article = "https://www.eff.org/deeplinks/2013/11/tpp-leak-confirms-worst-us-negotiators-still-trying-trade-away-internet-freedoms"
  val article = "http://techcrunch.com/2013/11/14/with-1-5m-in-funding-knotch-lets-opinionated-people-show-their-true-colors/"

  val res = NarrowWideGraphService.generateAndSaveGraphs(article)
  println("Narrow: ")
  res(StoredGraphDisplayDepth.NARROW).prettyPrintNodes(false)
  println(res(StoredGraphDisplayDepth.NARROW))

  println("Wide: ")
  res(StoredGraphDisplayDepth.WIDE).prettyPrintNodes(false)

  //
}

object testNarrowWideGraphingMultiArticles extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  val articles = Seq(
    "http://money.cnn.com/2013/11/14/news/economy/obamacare-insurance",
    "https://www.eff.org/deeplinks/2013/11/tpp-leak-confirms-worst-us-negotiators-still-trying-trade-away-internet-freedoms",
    "http://uk.news.yahoo.com/sirga-lioness-friendship-botswana-valentin-gruener-and-mikkel-legarth-153107523.html",
    "http://news.yahoo.com/a-snake-the-size-of-a-school-bus--it-happened-before--and-could-happen-again-153534343.html",
    "http://uk.news.yahoo.com/body-found-search-barrister-180807887.html",
    "http://uk.news.yahoo.com/royal-marine-guilty-murdering-afghan-fighter-140945165.html",
    "http://uk.news.yahoo.com/what-if-the-world-s-icecaps-melted-overnight--120351663.html",
    "http://uk.news.yahoo.com/boy-3-murdered-wet-bed-181413499.html",
    "http://ca.news.yahoo.com/typhoon-historic-power-slams-philippines-brings-fears-catastrophic-052428664.html",
    "http://www.huffingtonpost.com/2013/11/14/obamacare-fix_n_4274051.html"
  )

  for (article <- articles) {
    println(article)

    val res = NarrowWideGraphService.generateAndSaveGraphs(article)
    println("Narrow: ")
    res(StoredGraphDisplayDepth.NARROW).prettyPrintNodes(false)

    println("Wide: ")
    res(StoredGraphDisplayDepth.WIDE).prettyPrintNodes(false)

    println("*************************")

  }

}

object testNarrowGraph extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  implicit val conf = HBaseConfProvider.getConf.defaultConf
  //    val article = "http://uk.news.yahoo.com/sirga-lioness-friendship-botswana-valentin-gruener-and-mikkel-legarth-153107523.html"
  //    val article = "http://money.cnn.com/2013/11/14/news/economy/obamacare-insurance"
  val article = "http://uk.news.yahoo.com/royal-marine-guilty-murdering-afghan-fighter-140945165.html"

  val articleKey = ArticleKey(article)
  println(articleKey)
  val graph = ViewerFriendlyGraphService.getGraph(articleKey)
  graph.prettyPrintNodes()

  //    val saveUrl = "custom:" + article
  //saveArticleGraph(saveUrl, graph)

  //    println("Saved: " + saveUrl)
}

object testTitleGraphingMultiArticles extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  val articles = Array(
    "http://uk.news.yahoo.com/sirga-lioness-friendship-botswana-valentin-gruener-and-mikkel-legarth-153107523.html",
    "http://news.yahoo.com/a-snake-the-size-of-a-school-bus--it-happened-before--and-could-happen-again-153534343.html",
    "http://uk.news.yahoo.com/body-found-search-barrister-180807887.html",
    "http://uk.news.yahoo.com/royal-marine-guilty-murdering-afghan-fighter-140945165.html",
    "http://uk.news.yahoo.com/what-if-the-world-s-icecaps-melted-overnight--120351663.html",
    "http://uk.news.yahoo.com/boy-3-murdered-wet-bed-181413499.html",
    "http://ca.news.yahoo.com/typhoon-historic-power-slams-philippines-brings-fears-catastrophic-052428664.html"
  )

  articles.foreach(url => {
    println("url:" + url)
    val sg = ViewerFriendlyGraphService.getGraph(ArticleKey(url))
    println("url:" + url)
    sg.prettyPrintNodes()

    val saveUrl = "custom:" + url
    ViewerFriendlyGraphIT.saveArticleGraph(saveUrl, sg)
    println("Saved: " + saveUrl)
    println("**************************")
  })

}

object ViewerFriendlyGraphIT {
  implicit val conf = HBaseConfProvider.getConf.defaultConf

  def saveArticleGraph(url: String, sg: StoredGraph, psg: StoredGraph = StoredGraph.makeEmptyGraph) {
    val articleKey = ArticleKey(url)

    Schema.Articles.put(articleKey)
      .value(_.url, url)
      .value(_.title, "custom graph")
      .value(_.publishTime, new DateTime())
      .value(_.siteGuid, "GRVba8190df-03de-443f-887a-83795c1ec462")
      .value(_.conceptStoredGraph, sg)
      .value(_.phraseConceptGraph, psg)
      .execute()
  }
}
