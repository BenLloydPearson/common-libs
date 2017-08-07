package com.gravity.interests.jobs.intelligence.algorithms.graphing.datasets

import com.gravity.utilities.grvio
import scala.collection.mutable
import scala.collection.JavaConversions._
import com.gravity.textutils.analysis.{ExtractionLevel, PhraseExtractorInterop}
import com.gravity.interests.jobs.intelligence.{ArticleLike, StoredGraphBuilder, NodeType, StoredGraph, ArticleRow}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
case class WebMDDictionaryTerm(term: String, definition: String, uri: String)


object WebMDDictionary {

  private val terms = mutable.Buffer[WebMDDictionaryTerm]()

  grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/algorithms/graphing/datasets/webmd_medical.txt") {
    line => {
      try {
        line.split('\t') match {
          case Array(term, definition, uri, itemType) => {
            terms += WebMDDictionaryTerm(term, definition, uri)
          }
          case _ =>
        }
      }
      catch {
        case e: Exception => // warn("Unable to parse webmd_medical.txt: " + line)
      }
    }
  }

  val termIndex = terms.map(term => term.term -> term).toMap
  val lowerCaseTermIndex = terms.map(term => term.term.toLowerCase -> term).toMap


  def termsInString(text: String) = {
    val phrases = PhraseExtractorInterop.getInstance.extractLegacy(text, 1, 3, ExtractionLevel.NGRAMS)

    phrases.flatMap(phrase => {
      lowerCaseTermIndex.get(phrase)
    }).toSeq
  }

  def termsInArticleRow(articleRow: ArticleLike) = {
    WebMDDictionary.termsInString(articleRow.title) ++ WebMDDictionary.termsInString(articleRow.keywords) ++ WebMDDictionary.termsInString(articleRow.summary)
  }

  def scrubFromGraph(graph: StoredGraph) = {
    graph.populateUriAndName()
    val b = new StoredGraphBuilder()
    b.nodes.appendAll(graph.nodes.filter(!_.uri.startsWith("http://dictionary.webmd.com")))
    b.edges.appendAll(graph.edges)
    b.build
  }

  def addToArticleGraph(article: ArticleLike, articleGraph: StoredGraph) = {
    val webMdTerms = termsInArticleRow(article)
    val builder = StoredGraph.builderFrom(articleGraph)

    println("FOUND TERMS: " + webMdTerms)

    webMdTerms.groupBy(_.term).foreach {
      case (term, items) =>
        builder.addNode(items.head.uri, items.head.term, NodeType.Topic, 100, 1, 0.5)
    }
    builder.build

  }
}
