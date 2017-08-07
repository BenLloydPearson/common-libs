package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.algorithms.PhraseConceptGraphBuilder
import com.gravity.interests.graphs.graphing.{ContentType, Grapher, Weight}
import com.gravity.ontology.{ConceptGraph, OntologyGraphName}
import org.joda.time.DateTime
import com.gravity.utilities.web.ContentUtils
import com.gravity.interests.graphs.graphing.ContentToGraph
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.MutableGraph
import com.gravity.interests.jobs.intelligence.operations.graphing.{ConceptGraphCreationStrategy, TitleAndMetaGraphCreationStrategy, TitleTopicsRawKeywordsGraphCreationStrategy}

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 11/7/13
 * Time: 3:51 PM
 * To change this template use File | Settings | File Templates.
 */


object NarrowWideGraphService {
  implicit val conf = HBaseConfProvider.getConf.defaultConf
  val siteGuid = "GRV680929e3-8389-40b1-b6d4-3ce2c6e15bf3"

  def generateAndSaveGraphs(url: String)(implicit ogName: OntologyGraphName): Map[StoredGraphDisplayDepth.Type, StoredGraph] = {
    val cUrl = customUrl(url)

    val res = {
      // TBD: currenlty all shortcut paths are disabled so article will be crawled and graphed everytime!!
      getArticleRow(ArticleKey(cUrl + "disable")) match {
        case Some(crow) =>
          println("custom article exist")
          // custom article exist
          Map(StoredGraphDisplayDepth.NARROW -> crow.autoGraph, StoredGraphDisplayDepth.WIDE -> crow.conceptGraph)

        case None => {
          // no - but does real article exist?
          println("custom article does NOT exist")

          getArticleRow(ArticleKey(url + "disable")) match {
            case Some(arow) => {
              println("real article exists")

              // real article exists
              //              val narrowGraph = ViewerFriendlyGraphService.getGraph(arow)
              val narrowGraph = getNarrowGraph(url, arow.content, arow.title, arow.keywords)
              val wideGraph = getWideGraph(url, arow.content, arow.title, arow.keywords)

              // save custom article
              saveArticle(cUrl, narrowGraph, wideGraph, arow.content, arow.title, arow.keywords)
              Map(StoredGraphDisplayDepth.NARROW -> narrowGraph, StoredGraphDisplayDepth.WIDE -> wideGraph)
            }
            case None =>
              println("real article does NOT exist")
              // real article does NOT exist
              val articleContent = ContentUtils.fetchAndExtractMetadataSafe(url)
              val title = articleContent.title
              val keywords = articleContent.keywords
              val content = articleContent.text

              println("save dummy custom article & and refetch")
              // save dummy custom article & and refetch
              saveArticle(cUrl, StoredGraph.makeEmptyGraph, StoredGraph.makeEmptyGraph, content, title, keywords)

              getArticleRow(ArticleKey(cUrl)) match {
                case Some(drow) =>
                  println("custom article exists")
                  //                  val narrowGraph = ViewerFriendlyGraphService.getGraph(drow)
                  val narrowGraph = getNarrowGraph(cUrl, content, title, keywords)
                  val wideGraph = getWideGraph(cUrl, content, title, keywords)

                  saveArticle(cUrl, narrowGraph, wideGraph, content, title, keywords)
                  Map(StoredGraphDisplayDepth.NARROW -> narrowGraph, StoredGraphDisplayDepth.WIDE -> wideGraph)
                case None =>
                  println("custom article does NOT exist")
                  // no dice
                  Map(StoredGraphDisplayDepth.NARROW -> StoredGraph.makeEmptyGraph, StoredGraphDisplayDepth.WIDE -> StoredGraph.makeEmptyGraph)
              }
          }

        }


      }
    }

    res.foreach(_._2.populateUriAndName())
    res
  }

  private def getNarrowGraph(url: String, content: String, title: String, keywords: String)(implicit ogName: OntologyGraphName): StoredGraph = {
    new TitleTopicsRawKeywordsGraphCreationStrategy().getGraph(url, content, title, keywords)
  }

  private def getWideGraph(url: String, content: String, title: String, keywords: String)(implicit ogName: OntologyGraphName): StoredGraph = {
    new ConceptGraphCreationStrategy().getGraph(url, content, title, keywords)
  }

  private def getArticleRow(articleKey: ArticleKey) = {
    Schema.Articles.query2.withKey(articleKey).withFamilies(_.meta, _.storedGraphs).singleOption()
  }

  private def saveArticle(url: String, narrow: StoredGraph, wide: StoredGraph, content: String, title: String, keywords: String) {
    val articleKey = ArticleKey(url)
    Schema.Articles.put(articleKey)
      .value(_.url, url)
      .value(_.title, title)
      .value(_.keywords, keywords)
      .value(_.content, content)
      .value(_.publishTime, new DateTime())
      .value(_.siteGuid, siteGuid)
      .value(_.autoStoredGraph, narrow)
      .value(_.conceptStoredGraph, wide)
      .execute()
  }

  private def customUrl(url: String) = {
    "custom:" + url
  }

}

// this object is no longer used
object ViewerFriendlyGraphService {

  implicit val conf = HBaseConfProvider.getConf.defaultConf
  def getGraph(articleKey: ArticleKey)(implicit ogName: OntologyGraphName): StoredGraph = {
    (for {articleRow <- Schema.Articles.query2.withKey(articleKey).withFamilies(_.meta, _.text).singleOption()}
    yield getGraph(articleRow)).getOrElse(StoredGraph.makeEmptyGraph)
  }

  /*
    Graph = (topics from titles) + raw kewords
   */
  def getGraph(article: ArticleRow)(implicit ogName: OntologyGraphName): StoredGraph = {
    new TitleTopicsRawKeywordsGraphCreationStrategy().getGraph(article.url, "", article.title, article.keywords)
  }

  def getGraph_Old(article: ArticleRow)(implicit ogName: OntologyGraphName): StoredGraph = {

    //    val sg1 = parsedTitleGraph(article)
    //    dumpGraph("parsed title graph", sg1)

    val sg2 = titleAndKeywordGraphArticles(article)
    //    dumpGraph("basic title grapher", sg2)

    val (sg3, sg4) = buildCustomTitleGraphs(article.title)
    //    dumpGraph("custom tweet title graph", sg3)
    //    dumpGraph("custom tweet phrase graph", sg4)

    val newSg = connectEdges(keepTopicsAndTermsOnly(sg2 + sg3))
    //    dumpGraph("Combined Graph", newSg)
    newSg
  }


  private def titleAndKeywordGraphArticles(articleRow: ArticleRow)(implicit ogName: OntologyGraphName): StoredGraph = {
    new TitleAndMetaGraphCreationStrategy().getGraph(articleRow.url, "", articleRow.title, articleRow.keywords)
  }

  private def connectEdges(sg: StoredGraph) = {
    val newSg = new StoredGraph(sg.topicsAndTerms, Seq.empty[Edge], sg.date, sg.algorithm)
    val mg = new MutableGraph(newSg)
    for (n1 <- sg.nodes;
         n2 <- sg.nodes if n1.id != n2.id) {

      mg.addEdge(n1, n2, Edge(n1.id, n2.id, 1, 1.0, EdgeType.BroaderThan))

    }
    mg.toStoredGraph
  }

  private def dumpGraph(label: String, sg: StoredGraph) {
    println(label)
    println("nodes: " + sg.nodes.size + " topics: " + sg.topics.size + " terms: " + sg.terms.size)
    sg.topics.foreach(n => println("   " + n.uri))
    println("edges: " + sg.edges.size)
    println("--------------------------------")
    println("--------------------------------")

  }

  private def buildCustomTitleGraphs(content: String)(implicit ogName: OntologyGraphName) = {
    val uri = ""
    val pubTime = new DateTime()

    val cogList = ContentToGraph(uri, pubTime, content, Weight.High, ContentType.Title) :: Nil
    val grapher = new Grapher(cogList)
    val conceptGraphResult = ConceptGraph.getRelatedConcepts(2, grapher, true)
    val sg = StoredGraphHelper.buildGraph(conceptGraphResult, 30, 45, 100, 750, 2, 1)
    val psg = PhraseConceptGraphBuilder.buildPhraseConceptGraph(cogList.toBuffer).getOrElse(StoredGraph.makeEmptyGraph)

    (sg, psg)
  }

  private def keepTopicsAndTermsOnly(sg: StoredGraph) = {
    new StoredGraph(sg.topicsAndTerms, Seq.empty[Edge], sg.date, sg.algorithm)
  }

}
