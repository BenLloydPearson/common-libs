package com.gravity.interests.jobs.intelligence.algorithms

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import org.junit.Test
import com.gravity.interests.jobs.intelligence.operations.{GraphAnalysisService, SummaryGraphService}

import scala.collection.JavaConversions._
import scala.collection._
import com.gravity.interests.jobs.intelligence.{ArticleKey, ArticleRow, Schema, StoredGraph}
import com.gravity.interests.jobs.intelligence.algorithms.SiteSpecificGraphingAlgos.{NormalGrapher, PhraseConceptGrapherOnly}
import com.gravity.ontology.OntologyGraphName

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 10/16/13
 * Time: 10:19 PM
 * To change this template use File | Settings | File Templates.
 */

object testGraphSimilarity extends App {
  val url1 = "http://ca.news.yahoo.com/u-senators-hint-possible-fiscal-deal-tuesday-004610122--sector.html"
  val url2 = "http://ca.news.yahoo.com/senate-leaders-lead-hunt-deal-shutdown-debt-limit-084538348--finance.html"
  val url3 = "http://ca.news.yahoo.com/iran-world-powers-seek-end-deadlock-nuclear-talks-231221319.html"

  sim(url1, url2)
  sim(url1, url3)
  sim(url2, url3)



  object calcScore extends GraphCosineSimilarity(weightFuncScore)
  object calcId extends GraphCosineSimilarity(weightFuncId)

  def weightFuncScore(node: com.gravity.interests.jobs.intelligence.Node): Double = {
    node.score
  }

  def weightFuncId(node: com.gravity.interests.jobs.intelligence.Node): Double = {
    node.id.toDouble
  }

  def sim(left: String, right: String) {

    for (a1 <- SimpleGrapherIT.getArticle(left);
         a2 <- SimpleGrapherIT.getArticle(right);
         g1 = a1.liveTfIdfGraph;
         g2 = a2.liveTfIdfGraph) {


      println(left + " vs " + right)
      println(g1.nodes.size + " vs " + g2.nodes.size)

      val s1 = calcScore.score(g1, g2).score
      val s2 = calcId.score(g1, g2).score


      print("  calcScore: " + s1)
      print("  calcId: " + s2)
      println("")


    }

  }
}

object testArticleGraphing extends App {
  implicit val ogName = new OntologyGraphName("graph_concept")

  val urls = Seq(
    "http://techcrunch.com/2013/10/16/apple-reportedly-reduces-iphone-5c-orders-increases-iphone-5s-output-for-q4/",
    "http://ca.news.yahoo.com/iran-world-powers-seek-end-deadlock-nuclear-talks-231221319.html")

  for {url <- urls
       article <- SimpleGrapherIT.getArticle(url)
       sg = PhraseConceptGraphBuilder.getGraphForArticle(article)
       ssg = SummaryGraphService.toSummaryGraph(sg)} {

    println("url: " + url)
    sg.prettyPrintNodes(true)

    println("summary graph")
    ssg.prettyPrintNodes(true)
  }
}

object testCompareGraphs extends App {
  implicit val ogName = new OntologyGraphName("graph_concept")

  val urlSeq = Seq(
    "http://www.huffingtonpost.com/entry/donald-trump-obama-isis-mvp_us_57aca22ae4b0db3be07d670b"
//    ,
//    "http://www.latimes.com/local/la-me-concrete-quake-20131019,0,1097898.story",
//    "http://www.cnn.com/2013/10/16/living/identity-comedy-russell-peters-netflix/index.html",
//    "http://ca.news.yahoo.com/iran-world-powers-seek-end-deadlock-nuclear-talks-231221319.html",
//    "http://uk.news.yahoo.com/81-elephants-die-poisoning-zimbabwe-authorities-192516589.html",
//    //      "http://techcrunch.com/2013/10/20/how-healthcare-gov-doomed-itself-by-screwing-startups/",
//    //      "http://techcrunch.com/2013/10/15/yahoo-reduces-planned-alibaba-share-sale-by-20-will-keep-more-skin-in-the-game-when-it-ipos/"
//
//    "http://uk.news.yahoo.com/chinese-city-blanketed-heavy-pollution-051503205.html"
  )

  urlSeq.foreach(testArticle)

  def testArticle(url: String) {

    println("")
    println(url)

    val article = SimpleGrapherIT.getArticle(url).get
    val graphResult = NormalGrapher.graphArticleRow(article)

    for {cg <- graphResult.conceptGraph
         pg <- graphResult.phraseConceptGraph} {

      val lcg = GraphAnalysisService.copyGraphToTfIdfGraph(cg)
      val lpg = GraphAnalysisService.copyGraphToTfIdfGraph(pg)

      SimpleGrapherIT.diff(lcg, lpg)
    }


    //      println("Orig Grapher")
    //      val sgOrig = showGraph(NormalGrapher, article)

    // new grapher
    //      println("New Grapher")
    //      val sgNew = showGraph(PhraseConceptGrapherOnly, article)

    //      diff(sgOrig, sgNew)

  }
}

object SimpleGrapherIT {
  implicit val ogName = new OntologyGraphName("graph_concept")

  implicit val conf = HBaseConfProvider.getConf.defaultConf

  def getArticle(url: String): Option[ArticleRow] = {

    val ak = ArticleKey(url)
    val art = Schema.Articles.query2.withKey(ak).withFamilies(_.meta, _.storedGraphs, _.text).singleOption()
    art
  }

  def showGraph(grapher: BaseSiteGrapherAlgo, article: ArticleRow): StoredGraph = {
    val result = grapher.graphArticleRow(article)
    val liveTfidfGraph = GraphAnalysisService.copyGraphToTfIdfGraph(result.conceptGraph.getOrElse(StoredGraph.makeEmptyGraph))
    //liveTfidfGraph.prettyPrintNodes(true)
    liveTfidfGraph
  }

  def diff(sg1: StoredGraph, sg2: StoredGraph) {
    val nodes1 = sg1.nodes.map(_.uri).toSet
    val nodes2 = sg2.nodes.map(_.uri).toSet

    val common = nodes1.intersect(nodes2)
    val inSg1Only = nodes1 -- nodes2
    val inSg2Only = nodes2 -- nodes1

    println("Common Nodes: " + common.size)
    //    common.foreach(println)

    println("in Orig Only: " + inSg1Only.size)
    //    inSg1Only.foreach(println)

    println("in New Only: " + inSg2Only.size)
    inSg2Only.foreach(uri => {
      val n = sg2.nodeByUri(uri)
      println(n.uri + " : " + n.score)
    })


  }
}

