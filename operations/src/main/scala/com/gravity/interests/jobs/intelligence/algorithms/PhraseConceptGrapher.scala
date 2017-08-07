package com.gravity.interests.jobs.intelligence.algorithms

import com.gravity.hbase.schema.CommaSet
import com.gravity.interests.graphs.graphing.{ContentToGraph, _}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.algorithms.graphing.StopWordLoader
import com.gravity.interests.jobs.intelligence.operations.ArticleRowDomainDoNotUseAnymorePlease
import com.gravity.ontology.{ConceptGraph, ConceptGraphResult, OntologyGraphName, OntologyNode}
import com.gravity.textutils.analysis.{ExtractionLevel, PhraseExtractorInterop}
import com.gravity.utilities.grvstrings._
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.collection._
import scala.collection.mutable.Buffer


/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 10/16/13
 * Time: 9:38 PM
 * To change this template use File | Settings | File Templates.
 */


class PhraseConceptGrapher(graphTitle: Boolean,
                           graphTags: Boolean,
                           graphMetaKeywords: Boolean,
                           graphSummary: Boolean,
                           graphContent: Boolean = true,
                           topicUriExclusionSet: Set[String] = Set(),
                           conceptUriExclusionSet: Set[String] = Set()
                            ) extends BaseSiteGrapherAlgo(8, 1) {

  // override def graphArticleRow(article: ArticleRow, counter: SchemaTypes.CounterFunc): SiteSpecificGraphingAlgoResult = {
  override def graphArticleRow(article: ArticleLike, counter: SchemaTypes.CounterFunc)(implicit ogName: OntologyGraphName): SiteSpecificGraphingAlgoResult = {
    counter("Simple grapher ops begun", 1l)
    val publishDate = article.publishDateOption.getOrElse(new DateTime())
    val title = article.title
    val url = article.url
    val tags = article.tags
    val contentToGraph = Buffer[ContentToGraph]()
    val siteGuid = article.siteGuid

    val content = article.content


    if (graphTags && tags != CommaSet.empty) {
      val tagStr = tags.mkString(",")
      contentToGraph += ContentToGraph(url, publishDate, tagStr, Weight.High, ContentType.Keywords)
    }

    if (title.length > 0 && graphTitle) {
      contentToGraph += ContentToGraph(url, publishDate, title, Weight.High, ContentType.Title)
    }
    if (content.length > 0 && graphContent && !content.contains("Keep me signed in on this computer")) {
      contentToGraph += ContentToGraph(url, publishDate, content, Weight.Low, ContentType.Article)
    }

    if (article.keywords != null && article.keywords.nonEmpty && graphMetaKeywords) {
      val keywords = article.keywords.splitBetter(",").map(_.trim).mkString(",")
      contentToGraph += ContentToGraph(url, publishDate, keywords, Weight.High, ContentType.Keywords)
    }

    if (article.summary != null && article.summary.nonEmpty) {
      contentToGraph += ContentToGraph(url, publishDate, article.summary, Weight.Medium, ContentType.Title)
    }

    if (article.termVector1 != null && article.termVector1.nonEmpty) {
      article.termVector1.foreach(scoredTerm => {
        contentToGraph += ContentToGraph(url, publishDate, scoredTerm.term, Weight.Medium, ContentType.Title)
      })
    }

    if (article.termVectorG != null && article.termVectorG.nonEmpty) {
      article.termVectorG.foreach(scoredTerm => {
        contentToGraph += ContentToGraph(url, publishDate, scoredTerm.term, TermVectorG.toWeight(scoredTerm), TermVectorG.toContentType(scoredTerm))
      })
    }

    article.phraseVectorKea.foreach(scoredTerm => {
      contentToGraph += ContentToGraph(url, publishDate, scoredTerm.term, Weight.Medium, ContentType.Title)
    })

    //    println("CONTENT TO GRAPH")
    //    contentToGraph.foreach(println)

    //    println("TOPIC EXCLUSION SET")
    //    println(topicUriExclusionSet.toSet)


    SiteSpecificGraphingAlgoResult(
      autoStoredGraph = None,
      annoStoredGraph = None,
      conceptGraph = None,
      phraseConceptGraph = PhraseConceptGraphBuilder.buildPhraseConceptGraph(contentToGraph),
      ig = None
    )
  }

  override def graphArticleRowDomain(article: ArticleRowDomainDoNotUseAnymorePlease, counter: CounterFunc = CounterNoOp)(implicit ogName: OntologyGraphName): SiteSpecificGraphingAlgoResult = {
    counter("Simple grapher ops begun", 1l)
    val publishDate = article.publishDateOption.getOrElse(new DateTime())
    val title = article.title
    val url = article.url
    val tags = article.tags
    val contentToGraph = Buffer[ContentToGraph]()
    val siteGuid = article.siteGuid

    val content = article.content

    if (tags.size > 0 && graphTags) {
      val tagStr = tags.mkString(",")
      contentToGraph += ContentToGraph(url, publishDate, tagStr, Weight.High, ContentType.Keywords)
    }

    if (title.length > 0 && graphTitle) {
      contentToGraph += ContentToGraph(url, publishDate, title, Weight.High, ContentType.Title)
    }
    if (content.length > 0 && graphContent) {
      contentToGraph += ContentToGraph(url, publishDate, content, Weight.Low, ContentType.Article)
    }

    if (article.keywords != null && article.keywords.length() > 0 && graphMetaKeywords) {
      val keywords = article.keywords.splitBetter(",").map(_.trim).mkString(",")
      contentToGraph += ContentToGraph(url, publishDate, keywords, Weight.High, ContentType.Keywords)
    }

    if (article.summary != null && article.summary.length() > 0) {
      contentToGraph += ContentToGraph(url, publishDate, article.summary, Weight.Medium, ContentType.Title)
    }

    if (article.termVector1 != null && article.termVector1.size > 0) {
      article.termVector1.foreach(scoredTerm => {
        contentToGraph += ContentToGraph(url, publishDate, scoredTerm.term, Weight.Medium, ContentType.Title)
      })
    }

    if (article.termVectorG != null && article.termVectorG.nonEmpty) {
      article.termVectorG.foreach(scoredTerm => {
        contentToGraph += ContentToGraph(url, publishDate, scoredTerm.term, TermVectorG.toWeight(scoredTerm), TermVectorG.toContentType(scoredTerm))
      })
    }

    if (article.phraseVectorKea != null && article.phraseVectorKea.size > 0) {
      article.phraseVectorKea.foreach(scoredTerm => {
        contentToGraph += ContentToGraph(url, publishDate, scoredTerm.term, Weight.Medium, ContentType.Title)
      })
    }

    SiteSpecificGraphingAlgoResult(
      autoStoredGraph = None,
      annoStoredGraph = None,
      conceptGraph = None,
      phraseConceptGraph = PhraseConceptGraphBuilder.buildPhraseConceptGraph(contentToGraph),
      ig = None
    )
  }
}

object PhraseConceptGraphBuilder {
  val stopWords = JavaConversions.setAsJavaSet(StopWordLoader.stopWords)
  val phraseTfidfMinScore = 0.15D

  def buildPhraseConceptGraph(contentToGraph: Buffer[ContentToGraph])(implicit ogName: OntologyGraphName): Option[StoredGraph] = {

    val tightGraph = contentToGraph.filter(ctg => ctg.contentType == ContentType.Title && ctg.weight == Weight.High).map(ctg => {
      //      println("tight graphing:" + ctg.text)
      PhraseConceptGraphBuilder.getGraphForContent(Seq(ctg.text), tightGraph = true)
    }
    ).foldLeft(StoredGraph.makeEmptyGraph)(_ + _)

    //    println("tight graph")
    //    tightGraph.prettyPrintNodes(true)

    val rawTextToGraph = contentToGraph.map(ctg => ctg.text)
    val phraseConceptGraph = PhraseConceptGraphBuilder.getGraphForContent(rawTextToGraph) + tightGraph
    Some(phraseConceptGraph)


  }

  def getGraphForArticle(article: ArticleRow)(implicit ogName: OntologyGraphName): StoredGraph = {
    val content = article.column(_.content).getOrElse("")
    val title = article.column(_.title).getOrElse("")
    val summary = article.column(_.summary).getOrElse("")
    getGraphForContent(Seq(content, title, summary))
  }

  def getGraphForContent(contentSeq: Seq[String], nGrams: Int = 2, tightGraph: Boolean = false)(implicit ogName: OntologyGraphName): StoredGraph = {
    // raw ngrams
    val matchBuffer = mutable.Map[String, Int]()
    for {content <- contentSeq
         nGram <- (1 to nGrams)} {
      extract2(content, matchBuffer, nGram)
    }

    // with tfidf
    val phraseCountTfidf = matchBuffer.map(e => (e._1, e._2, PhraseAnalysisService.phraseTfIdf(e._1, e._2, matchBuffer.size))).toSeq.sortBy(-_._3)
    //    println("phrases: " + phraseCountTfidf.size)
    //    phraseCountTfidf.foreach(println)

    // filtered phrases
    //    val minTfidfScore = if (tightGraph) 0.0D else phraseTfidfMinScore
    val filteredPhrases = phraseCountTfidf.filter(p => p._3 >= phraseTfidfMinScore)

    // with topics
    val phraseCntTfidfTopic =
      (for {(p, cnt, tfidf) <- phraseCountTfidf
            t <- ConceptGraph.getNodeRichly(p)} yield (p, cnt, tfidf, t)).toSeq.sortBy(-_._3)

    val topics = phraseCntTfidfTopic.map(_._4)
    val topicUris = phraseCntTfidfTopic.map(_._4.uri)
    //    println("topics: " + topics.size)

    val cleanTopics = topics.filter(!_.uri.contains("Template:"))

    val sg =
      if (tightGraph) {
        makeTopicGraph(cleanTopics)
      }
      else {
        getConceptGraph(topicUris) + makePhraseGraph(filteredPhrases)
      }

    sg
  }

  def makePhraseGraph(phrases: Seq[(String /*uri*/ , Int /*count*/ , Double /*tfidf score*/ )]) = {
    val sgBuilder = StoredGraph.make
    phrases.foreach(p => {
      sgBuilder.addNode(p._1, p._1, NodeType.Term, count = p._2, score = p._3)
    })
    sgBuilder.build
  }

  def makeTopicGraph(topicSeq: Seq[OntologyNode]) = {
    val sgBuilder = StoredGraph.make
    topicSeq.foreach(node => {
      sgBuilder.addNode(node.uri, node.name, NodeType.Topic)
    })
    sgBuilder.build
  }

  def getConceptGraph(topics: Seq[String])(implicit ogName: OntologyGraphName) = {
    val conceptGraphResult = getConceptGraphResult(topics)

    //    if (tightGraph) {
    //        getConceptGraphResultForKeyTopics(topics)
    //    }

    //val sg = StoredGraphHelper.buildGraph(conceptGraphResult, 30, 45, 100, 75, -1, 1)

    val sg = {

      if (topics.size <= 50) {
        StoredGraphHelper.buildGraph(conceptGraphResult, 5, 20, 15, 10, -1, 1)
      }
      else {
        StoredGraphHelper.buildGraph(conceptGraphResult, 10, 40, 15, 20, -1, 3)

      }
    }

    sg
  }

  //  def getConceptGraphResultForKeyTopics(topics: Seq[String]) = {
  //    val grapher = new Grapher(Seq.empty[ContentToGraph])
  //    val depth = 1
  //    val result = new ConceptGraphResult(grapher)
  //    topics foreach (topicUri => {
  //      ConceptGraph.collectConcepts(topicUri, depth, result, true)
  //    })
  //    result.calculateScores()
  //    result
  //
  //  }

  def getConceptGraphResult(topics: Seq[String])(implicit ogName: OntologyGraphName) = {
    val grapher = new Grapher(Seq.empty[ContentToGraph])
    val depth = 4
    val result = new ConceptGraphResult(grapher)
    topics foreach (topicUri => {
      ConceptGraph.collectConcepts(topicUri, depth, result)
    })
    result.calculateScores
    result
  }

  def extract2(text: String, buffer: mutable.Map[String, Int], gramSize: Int = 1) {


    val results = PhraseExtractorInterop.getLegacyExtractor.extract(text, gramSize, gramSize, ExtractionLevel.NGRAMS, stopWords)

    def isStopWordFree(grams: Array[String]): Boolean = {
      for (gram <- grams) {
        if (stopWords.contains(gram)) return false
      }

      true
    }

    results.foreach {
      case s: String =>
        val phrase = s
        val count = results.count(s)

        val grams = phrase.toLowerCase.splitBetter(" ")

        if (isStopWordFree(grams)) {
          val existingCount = buffer.getOrElseUpdate(phrase, 0)
          buffer(phrase) = count + existingCount
        }
    }
  }


}

