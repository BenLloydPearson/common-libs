package com.gravity.interests.jobs.intelligence.operations

import org.junit.Test
import java.io.{BufferedReader, InputStreamReader}

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.{ArticleKey, Schema, StoredGraph}
import com.gravity.interests.jobs.intelligence.algorithms.StoredInterestSimilarityAlgos.{GraphCosineScoreOnlySimilarity, GraphCosineScoreSimilarity}

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 9/26/13
 * Time: 3:50 PM
 * To change this template use File | Settings | File Templates.
 */

object TestSimScores extends App {
  val articles = Seq(
    "http://news.yahoo.com/mass-starvation-feared-syria-no-food-195207806.html",
    "http://ca.news.yahoo.com/u-russia-other-powers-agree-core-u-n-204413969.html",
    "http://uk.news.yahoo.com/u-says-not-done-yet-syria-u-n-214853312.html",
    "http://uk.news.yahoo.com/kenya-white-widow-briton-linked-attack-083532304.html",
    "http://ca.news.yahoo.com/family-vancouver-terror-attack-victim-highlights-charitable-213848224.html",
    "http://news.yahoo.com/terrorists-claim-137-killed-kenya-mall-attack-131630801.html",
    "http://news.yahoo.com/despite-cruz-senate-heads-toward-obamacare-vote-124209679--finance.html",
    "http://news.yahoo.com/senate-moves-toward-test-vote-obamacare-070213163--finance.html",
    "http://finance.yahoo.com/news/analysis-republicans-risky-fight-obama-202311951.html",
    "http://news.yahoo.com/blogs/power-players-abc-news/rick-perry-shutting-down-government-defund-obamacare-not-111624028.html",
    "http://news.yahoo.com/clinton-yavuz-langdon-kenya-mall-shooting-134703828.html",
    "http://news.yahoo.com/obamacares-average-monthly-cost-across-u-328-040209815.html",
    "http://finance.yahoo.com/news/analysis-republicans-risky-fight-obama-202311951.html",
    "http://news.yahoo.com/syria-rebels-reject-opposition-coalition-call-islamic-leadership-092503945.html",

    "http://uk.news.yahoo.com/81-elephants-die-poisoning-zimbabwe-authorities-192516589.html",
    "http://news.yahoo.com/why-catholic-priests-cant-marry-least-now-115824493.html",
    "http://uk.news.yahoo.com/death-toll-pakistan-earthquake-jumps-173-030537112.html",
    "http://uk.news.yahoo.com/peru-drugs-pair-plead-guilty-court-184750542.html",
    "http://news.yahoo.com/oklahoma-teen-found-guilty-school-shooting-plot-111307841.html",
    "http://uk.news.yahoo.com/free-wi-fi-hotspots-allow-adult-content-005923896.html",
    "http://music.yahoo.com/news/mom-selling-kurt-cobains-childhood-home-wash-050404230.html",
    "http://news.yahoo.com/nasas-innovative-ion-space-thruster-sets-endurance-world-110945990.html",
    "http://ca.news.yahoo.com/university-alerts-students-faculty-student-tb-101257949.html"

  )

  val articlesWithGraphs = articles.map(url => {
    val sgOrig = SummaryGraphServiceIT.getGraph(url).get
    (url, sgOrig, SummaryGraphService.toSummaryGraph(sgOrig))
  })

  val res =
    for {(url1, sgOrig1, sg1) <- articlesWithGraphs
         (url2, sgOrig2, sg2) <- articlesWithGraphs if url1 != url2
         simScore = GraphCosineScoreOnlySimilarity.score(sg1, sg2) //if simScore.score > 0.0
         origSimScore = GraphCosineScoreSimilarity.score(sgOrig1, sgOrig2).score} yield {
      (simScore.score, url1, url2, origSimScore)
    }

  res.sortBy(-_._1).foreach(e => println(e._1 + " : " + e._4 + "  :  " + e._2 + " vs " + e._3))
}

object TestSummaryGraph extends App {
  //    object TestSummaryGraph extends App {
  val articles = Seq(
    "http://uk.news.yahoo.com/81-elephants-die-poisoning-zimbabwe-authorities-192516589.html",
    "http://news.yahoo.com/why-catholic-priests-cant-marry-least-now-115824493.html",
    "http://uk.news.yahoo.com/death-toll-pakistan-earthquake-jumps-173-030537112.html",
    "http://uk.news.yahoo.com/peru-drugs-pair-plead-guilty-court-184750542.html",
    "http://news.yahoo.com/oklahoma-teen-found-guilty-school-shooting-plot-111307841.html",
    "http://uk.news.yahoo.com/free-wi-fi-hotspots-allow-adult-content-005923896.html",
    "http://music.yahoo.com/news/mom-selling-kurt-cobains-childhood-home-wash-050404230.html",
    "http://news.yahoo.com/nasas-innovative-ion-space-thruster-sets-endurance-world-110945990.html",
    "http://ca.news.yahoo.com/university-alerts-students-faculty-student-tb-101257949.html"

  )

  for (url <- articles;
       sg <- SummaryGraphServiceIT.getGraph(url)) {
    val summaryGraph = SummaryGraphService.toSummaryGraph(sg)
    println("*** " + url + " ***")
    summaryGraph.prettyPrintNodes(true)
    println("   total topic nodes:" + sg.topics.size)
  }
}

object TestKeyNodesApp extends App {
  //    object TestKeyNodesApp extends App {
  val articles = Seq(
    "http://uk.news.yahoo.com/81-elephants-die-poisoning-zimbabwe-authorities-192516589.html",
    "http://news.yahoo.com/why-catholic-priests-cant-marry-least-now-115824493.html",
    "http://uk.news.yahoo.com/death-toll-pakistan-earthquake-jumps-173-030537112.html",
    "http://uk.news.yahoo.com/peru-drugs-pair-plead-guilty-court-184750542.html",
    "http://news.yahoo.com/oklahoma-teen-found-guilty-school-shooting-plot-111307841.html",
    "http://uk.news.yahoo.com/free-wi-fi-hotspots-allow-adult-content-005923896.html",
    "http://music.yahoo.com/news/mom-selling-kurt-cobains-childhood-home-wash-050404230.html",
    "http://news.yahoo.com/nasas-innovative-ion-space-thruster-sets-endurance-world-110945990.html",
    "http://ca.news.yahoo.com/university-alerts-students-faculty-student-tb-101257949.html"

  )

  articles.foreach(runArticle(_))

  //runLoop(articles.head)

  def runLoop(defaultUrl: String) {
    var continue = true
    val in = new BufferedReader(new InputStreamReader(java.lang.System.in))
    print(">")
    while (continue) {
      val ln = in.readLine()
      ln match {
        case "q" =>
          continue = false
          println("")
          println("exiting...")
        case str =>
          val url = if (ln.startsWith("http")) ln else defaultUrl
          //          println("fetching: " + url + " ...")
          runArticle(url)
          println("")
          print(">")
      }
    }
  }

  def runArticle(url: String) {
    println("")
    println("****** " + url + " ******")
    for (sg <- SummaryGraphServiceIT.getGraph(url);
         (nodeId, score) <- KeyNodes.extract(sg, Int.MinValue).sortBy(-_._2)) {
      println(sg.nodeById(nodeId).uri + " " + score)
    }
  }
}

object SummaryGraphServiceIT {
  implicit val conf = HBaseConfProvider.getConf.defaultConf

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
