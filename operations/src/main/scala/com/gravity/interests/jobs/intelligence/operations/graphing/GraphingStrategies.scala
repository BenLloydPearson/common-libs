package com.gravity.interests.jobs.intelligence.operations.graphing

import com.gravity.interests.jobs.intelligence._

import scala.util.Random
import scala.collection.mutable
import com.gravity.interests.graphs.graphing._
import org.joda.time.DateTime
import com.gravity.interests.jobs.intelligence.algorithms.{PhraseConceptGraphBuilder, SiteSpecificGraphingAlgos}
import com.gravity.ontology.{ConceptGraph, OntologyGraphName}
import com.gravity.interests.jobs.intelligence.operations.users.WebMDStopWordLoader
import com.gravity.interests.graphs.graphing.ContentToGraph
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.MutableGraph

/**
 * Created by apatel on 12/4/13.
 */

trait GraphCreationStrategy {
  implicit val conf = HBaseConfProvider.getConf.defaultConf

  def getGraph(url: String, content: String, title: String, keywords: String)(implicit ogName: OntologyGraphName): StoredGraph

  def connectRandEdges(sg: StoredGraph) = {
    val newSg = new StoredGraph(sg.topicsAndTerms, Seq.empty[Edge], sg.date, sg.algorithm)
    val mg = new MutableGraph(newSg)
    val r = new Random()

    for (n1 <- sg.nodes;
         n2 <- sg.nodes if n1.id != n2.id;
         rand = r.nextBoolean() if (rand)) {

      mg.addEdge(n1, n2, Edge(n1.id, n2.id, 1, 1.0, EdgeType.BroaderThan))

    }
    mg.toStoredGraph
  }
}

class PhraseConceptGraphCreationStrategy extends GraphCreationStrategy {
  def getGraph(url: String, content: String, title: String, keywords: String)(implicit ogName: OntologyGraphName): StoredGraph = {
    val contentToGraph = mutable.Buffer[ContentToGraph]()
    contentToGraph += ContentToGraph(url, new DateTime(), title, Weight.High, ContentType.Title)
    contentToGraph += ContentToGraph(url, new DateTime(), keywords, Weight.High, ContentType.Keywords)
    contentToGraph += ContentToGraph(url, new DateTime(), content, Weight.Low, ContentType.Article)
    val phraseConceptGraph = PhraseConceptGraphBuilder.buildPhraseConceptGraph(contentToGraph)
    phraseConceptGraph.getOrElse(StoredGraph.makeEmptyGraph)
  }
}

class ConceptGraphCreationStrategy extends GraphCreationStrategy {
  def getGraph(url: String, content: String, title: String, keywords: String)(implicit ogName: OntologyGraphName): StoredGraph = {
    val result = ConceptGraph.processArticle(url, content, title + " " + keywords)
    StoredGraphHelper.buildGraph(result)
  }
}

class TitleTopicsRawKeywordsGraphCreationStrategy extends GraphCreationStrategy {
  def getGraph(url: String, content: String, title: String, keywords: String)(implicit ogName: OntologyGraphName): StoredGraph = {

    // Graph = (topics from titles) + raw keywords

    // graph just title topics
    val cogList = ContentToGraph(url, new DateTime(), title, Weight.High, ContentType.Title) :: Nil
    val grapher = new Grapher(cogList)

    val topicUris = grapher.allTopics
    val sgBuilder = StoredGraph.make

    for (topicUri <- topicUris) {
      sgBuilder.addNode(topicUri, "", NodeType.Topic)
    }

    // graph raw keywords as terms
    val kw = keywords.split(",").map(_.trim.toLowerCase()).toSeq
    println(keywords + " : extracted " + kw.size + " keywords")
    for (keyword <- kw if kw.size > 0) {
      val keywordUrl = "http://gravity.com/" + keyword
      sgBuilder.addNode(keywordUrl, keyword, NodeType.Term)
    }

    val sg = sgBuilder.build
    connectRandEdges(sg)
  }
}

class ParsedTitleGraphCreationStrategy extends GraphCreationStrategy {
  def getGraph(url: String, content: String, title: String, keywords: String)(implicit ogName: OntologyGraphName): StoredGraph = {

    val GRAVITY_URI_PREFIX = "http://gravity/"

    val cleanTitle = title.toLowerCase.replace("-", " ").replace(",", " ").replace(":", " ")
    val titleWords = cleanTitle.split(" ").toList
    val filteredTitleWords = titleWords.filterNot(WebMDStopWordLoader.stopWords.toList.contains)

    val builder = StoredGraph.builderFrom(StoredGraph.makeEmptyGraph)
    val titleCountMap = mutable.HashMap[String, Int]()

    filteredTitleWords.foreach(
      title => {
        titleCountMap(title) = titleCountMap.getOrElse(title, 0) + 1
      }
    )

    titleCountMap.foreach(
      title => {
        val titleName = title._1
        val titleCount = title._2

        builder.addNode(
          GRAVITY_URI_PREFIX + titleName,
          titleName,
          NodeType.Term,
          100,
          titleCount,
          0.5
        )
      }
    )

    titleCountMap.foreach(
      title => {
        val titleName1 = title._1
        titleCountMap.foreach(
          title2 => {
            val titleName2 = title2._1
            builder.relate(
              GRAVITY_URI_PREFIX + titleName1,
              GRAVITY_URI_PREFIX + titleName2,
              EdgeType.BroaderThan)
          }
        )
      }
    )

    val sg = builder.build
    sg
  }
}

class TitleAndMetaGraphCreationStrategy extends GraphCreationStrategy {
  def getGraph(url: String, content: String, title: String, keywords: String)(implicit ogName: OntologyGraphName): StoredGraph = {
    var sg = StoredGraph.makeEmptyGraph
    for (
      row <- getArticleRow(url);
      siteGrapher = SiteSpecificGraphingAlgos.getGrapher("TitleAndMetaOnlyGrapher");
      result = siteGrapher.graphArticleRow(row);
      cg <- result.conceptGraph) {
      sg = sg.plusOne(cg)
    }
    sg
  }

  def getArticleRow(url: String) = {
    Schema.Articles.query2.withKey(ArticleKey(url)).withFamilies(_.meta, _.text).singleOption()
  }
}

object AllGraphStrategies {
  lazy val graphStrategies =
    Seq(
      new PhraseConceptGraphCreationStrategy,
      new ConceptGraphCreationStrategy,
      new TitleTopicsRawKeywordsGraphCreationStrategy,
      new ParsedTitleGraphCreationStrategy,
      new TitleAndMetaGraphCreationStrategy
    )
}
