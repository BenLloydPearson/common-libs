package com.gravity.interests.jobs.intelligence.algorithms

import com.gravity.hbase.schema.CommaSet
import com.gravity.interests.graphs.graphing._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.algorithms.graphing.datasets.WebMDDictionary
import com.gravity.interests.jobs.intelligence.algorithms.graphing.{GraphingLists, YahooOlympicsGraphingAlgo}
import com.gravity.interests.jobs.intelligence.operations.{ArticleRowDomainDoNotUseAnymorePlease, ArticleService}
import com.gravity.ontology.{ConceptGraph, OntologyGraphName}
import com.gravity.ontology.annotation.{AnnotatorSuggestion, BadConcepts, BadTopics, WritingAnnotationService}
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.grvstrings._
import org.joda.time.DateTime

import scala.collection._
import scala.collection.mutable.Buffer


/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class SiteSpecificGraphingAlgoResult(phraseConceptGraph: Option[StoredGraph],
                                          conceptGraph: Option[StoredGraph],
                                          autoStoredGraph: Option[StoredGraph],
                                          annoStoredGraph: Option[StoredGraph],
                                          ig: Option[com.gravity.interests.graphs.graphing.InterestGraph] = None)

/**
 * This is a system where you can override how graphers operate against particular sites.
 * This allows you to choose the algorithm, inject or remove topics, etc. etc.
 *
 * NOTE: You should almost always subclass ParameterizedGrapher
 */
object SiteSpecificGraphingAlgos {
  def graphers = Map(
    ArticleWhitelist.siteGuid(_.TECHCRUNCH) -> TechCrunchGrapher,
    ArticleWhitelist.siteGuid(_.BOSTON) -> BostonGrapher,
    ArticleWhitelist.siteGuid(_.YAHOO_OLYMPICS) -> new YahooOlympicsGraphingAlgo(),
    ArticleWhitelist.siteGuid(_.WEBMD) -> WebMDGrapher,
    ArticleWhitelist.siteGuid(_.AUTOBLOG) -> AutoBlogGrapher,

    //this is not a site but this nice framework here lets us build different graphers
    //if this should live somewhere else lemme know
    //also not sure if we should be passing strings around here - jim
    "TitleOnlyGrapher" -> TitleOnlyGrapher,
    "TitleAndMetaOnlyGrapher" -> TitleAndMetaOnlyGrapher
  )


  def getGrapher(guid: String): BaseSiteGrapherAlgo = {
    graphers.getOrElse(guid, NormalGrapher)
  }


  object JaneGrapher extends ParameterizedGrapher(
    graphTitle = true,
    graphTags = true,
    graphMetaKeywords = false,
    graphSummary = false,
    graphContent = false,
    urlsToTopics = SiteSpecificGrapherTopicMaps.xoJaneUrls
  )


  object NormalGrapher extends ParameterizedGrapher(
    graphTitle = true,
    graphTags = true,
    graphMetaKeywords = false,
    graphSummary = true,
    graphContent = true,
    topicUriExclusionSet = GraphingLists.topicUriExclusionSet,
    conceptUriExclusionSet = GraphingLists.conceptUriExclusionSet
  )

  object PhraseConceptGrapherOnly extends PhraseConceptGrapher(
    graphTitle = true,
    graphTags = true,
    graphMetaKeywords = false,
    graphSummary = true,
    graphContent = true,
    topicUriExclusionSet = GraphingLists.topicUriExclusionSet,
    conceptUriExclusionSet = GraphingLists.conceptUriExclusionSet
  )

  object BostonGrapher extends ParameterizedGrapher(
    graphTitle = true,
    graphTags = true,
    graphMetaKeywords = true,
    graphSummary = false, //Summary is just a
    graphContent = true,
    urlsToTopics = SiteSpecificGrapherTopicMaps.bostonUrls
  )

  object TechCrunchGrapher extends ParameterizedGrapher(
    graphTitle = true,
    graphTags = true,
    graphMetaKeywords = true,
    graphSummary = true,
    graphContent = true
  )

  object AutoBlogGrapher extends ParameterizedGrapher(
    graphTitle = true,
    graphTags = true,
    injectTagGraph = true,
    graphMetaKeywords = false,
    graphSummary = true,
    graphContent = true,
    topicUriExclusionSet = GraphingLists.topicUriExclusionSet,
    conceptUriExclusionSet = GraphingLists.conceptUriExclusionSet
  )

  object GraphAllContentGrapher extends ParameterizedGrapher(
    graphTitle = true,
    graphTags = true,
    graphMetaKeywords = true,
    graphSummary = true,
    graphContent = true
  )

  object WebMDGrapher extends ParameterizedGrapher(
    graphTitle = true,
    graphTags = true,
    graphMetaKeywords = true,
    graphSummary = true,
    graphContent = true,
    topicUriExclusionSet = GraphingLists.topicUriExclusionSet
  )


  object MensFitnessGrapher extends ParameterizedGrapher(
    graphTitle = true,
    graphTags = false,
    graphMetaKeywords = true,
    graphSummary = true,
    graphContent = true,
    urlsToTopics = SiteSpecificGrapherTopicMaps.mensFitnessUrls
  )

  object TitleOnlyGrapher extends ParameterizedGrapher(
    graphTitle = true,
    graphTags = false,
    graphMetaKeywords = false,
    graphSummary = false,
    graphContent = false
  )

  object TitleAndMetaOnlyGrapher extends ParameterizedGrapher(
    graphTitle = true,
    graphTags = true,
    graphMetaKeywords = true,
    graphSummary = false,
    graphContent = false
  )

}

object SiteSpecificGrapherTopicMaps {
  val bostonUrls = Map(
    "business" -> "http://dbpedia.org/resource/Business",
    "fashion" -> "http://dbpedia.org/resource/Fashion_design",
    "health" -> "http://dbpedia.org/resource/Health",
    "travel" -> "http://dbpedia.org/resource/Travel",
    "sports" -> "http://dbpedia.org/resource/Sport",
    "lifestyle" -> "http://dbpedia.org/resource/Lifestyle_(sociology)",
    "cars" -> "http://dbpedia.org/resource/Automobile",
    "jobs" -> "http://dbpedia.org/resource/Employment",
    "realestate" -> "http://dbpedia.org/resource/Real_estate",
    "news" -> "http://dbpedia.org/resource/News",
    "bigpicture" -> "http://dbpedia.org/resource/Photography",
    "education" -> "http://dbpedia.org/resource/Education",
    "numbers" -> "http://dbpedia.org/resource/Lottery",
    "traffic" -> "http://dbpedia.org/resource/Commuting",
    "comics" -> "http://dbpedia.org/resource/Comics",
    "games" -> "http://dbpedia.org/resource/Games",
    "celebrity" -> "http://dbpedia.org/resource/Movie_star",
    "tv" -> "http://dbpedia.org/resource/Television",
    "teater_arts" -> "http://dbpedia.org/resource/The_arts",
    "music" -> "http://dbpedia.org/resource/Music",
    "restaurants" -> "http://dbpedia.org/resource/Food",
    "movies" -> "http://dbpedia.org/resource/Film",
    "politics" -> "http://dbpedia.org/resource/Politics",
    "opinion" -> "http://dbpedia.org/resource/Opinion",
    "hockey" -> "http://dbpedia.org/resource/Hockey",
    "basketball" -> "http://dbpedia.org/resource/Basketball",
    "baseball" -> "http://dbpedia.org/resource/Baseball"
  )

  val mensFitnessUrls = Map(
    "training" -> topic("Exercise"),
    "lose-weight" -> topic("Weight_Loss"),
    "nutrition" -> topic("Nutrition"),
    "supplements" -> topic("Supplements"),
    "women" -> topic("Women"),
    "sports" -> topic("Sport"),
    "travel" -> topic("Travel"),
    "outdoor" -> topic("Outdoors"),
    "fashion-and-trends" -> topic("Fashion")
  )

  val xoJaneUrls = Map(
    "beauty" -> "http://dbpedia.org/resource/Beauty",
    "sex" -> "http://dbpedia.org/resource/Sex",
    "fashion" -> "http://dbpedia.org/resource/Fashion",
    "entertainment" -> "http://dbpedia.org/resource/Entertainment",
    "tech" -> "http://dbpedia.org/resource/Technology",
    "jane" -> "http://dbpedia.org/resource/Jane_Pratt",
    "diy" -> "http://dbpedia.org/resource/DIY",
    "family" -> "http://dbpedia.org/resource/Family",
    "healthy" -> "http://dbpedia.org/resource/Health",
    "fun" -> "http://dbpedia.org/resource/Humor",
    "newagey" -> "http://dbpedia.org/resource/New_Agey",
    "relationships" -> "http://dbpedia.org/resource/Relationship",
    "sports" -> "http://dbpedia.org/resource/Sport")

  def topic(name: String) = "http://dbpedia.org/resource/" + name
}


object Annotations {
  def badTopics = PermaCacher.getOrRegister("Annotations_badTopics", {
    try {
      WritingAnnotationService.getBannedTopicsInCurrentOntology()
    } catch {
      case ex: Exception => new BadTopics(scala.collection.immutable.Set[(String, AnnotatorSuggestion)]())
    }
  }, 60 * 5)

  def badLinks = PermaCacher.getOrRegister("Annotations_badLinks", {
    try {
      WritingAnnotationService.getBadConceptLinksInCurrentOntology()
    } catch {
      case ex: Exception => BadConcepts.empty
    }
  }, 60 * 5)

}

class ParameterizedGrapher(graphTitle: Boolean,
                           graphTags: Boolean,
                           injectTagGraph: Boolean = false,
                           graphMetaKeywords: Boolean,
                           graphSummary: Boolean,
                           graphContent: Boolean = true,
                           buildConceptGraph: Boolean = true,
                           urlsToTopics: Map[String, String] = Map(),
                           topicUriExclusionSet: Set[String] = Set(),
                           conceptUriExclusionSet: Set[String] = Set(),
                           graphCategory: Boolean = false
                            ) extends BaseSiteGrapherAlgo(7, 1) {


  private def buildConceptGraph(grapher: Grapher, counter: CounterFunc, buildGraph: Boolean)(implicit ogName: OntologyGraphName): Option[StoredGraph] = {
    if (buildGraph) {
      try {
        val storedCG =
          if (ConceptGraph.isNewOntology(ogName)){
//            println("topics size: " + grapher.topics.size)

            if (grapher.topics.size < 50){
//              println("small article")
              StoredGraphHelper.buildGraphFromAllTopics(grapher, depth=2, minFrequency = 1)
            }
            else{
//              println("large article")
              StoredGraphHelper.buildGraphFromAllTopics(grapher, depth=5, minFrequency = 2)

            }
          }
          else{
            StoredGraphHelper.build(grapher)
          }
        counter("ConceptGraph Created", 1l)
        Some(storedCG)
      }
      catch {
        case ex: Exception =>
          counter("ConceptGraph Error", 1l)
          None
      }
    }
    else {
      None
    }
  }

  // override def graphArticleRow(article: ArticleRow, counter: SchemaTypes.CounterFunc): SiteSpecificGraphingAlgoResult = {
  override def graphArticleRow(article: ArticleLike, counter: SchemaTypes.CounterFunc)(implicit ogName: OntologyGraphName): SiteSpecificGraphingAlgoResult = {
    counter("Default site grapher ops begun", 1l)
    val publishDate = article.publishDateOption.getOrElse(new DateTime())
    val title = article.title
    val url = article.url
    val tags = article.tags
    val contentToGraph = Buffer[ContentToGraph]()
    val siteGuid = article.siteGuid

    val content = article.content

    println("graphing url " + url)  //+" against ontology " + ogName.graphName)

    for ((key, value) <- urlsToTopics) {
      if (url.contains(key)) {
//        ("Adding custom topic: " + value)
        contentToGraph += ContentToGraph(url, publishDate, value, Weight.High, ContentType.Topic)
      }
    }

    if (graphTags && tags != CommaSet.empty) {
      val tagStr = tags.mkString(",")
      contentToGraph += ContentToGraph(url, publishDate, tagStr, Weight.High, ContentType.Keywords)
    }

    val tagGraph =
      if (injectTagGraph){
        ArticleService.tagGraphArticle(article, 0.2)
      }
      else {
        None
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

    if (article.category.nonEmpty && graphCategory){
      val category = article.category.splitBetter(",").map(_.trim).mkString(",")
      contentToGraph += ContentToGraph(url, publishDate, category, Weight.High, ContentType.Keywords)
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
    //
    //    println("TOPIC EXCLUSION SET")
    //    println(topicUriExclusionSet.toSet)


    val grapher = new Grapher(contentToGraph,
      excludeTopicUris = topicUriExclusionSet.toSet,
      termVector1 = article.termVector1,
      termVector2 = article.termVector2,
      termVector3 = article.termVector3,
      termVectorG = article.termVectorG,
      phraseVectorKea = article.phraseVectorKea)

    //LAYERED ALGO
    val ig = grapher.interestGraph(GrapherAlgo.LayeredAlgo)

    val useWebMDSpecialAdditions = false

    val conceptGraphOption = buildConceptGraph(grapher, counter, this.buildConceptGraph) match {
      case Some(conceptGraph) if siteGuid == ArticleWhitelist.siteGuid(_.WEBMD) && useWebMDSpecialAdditions => {
        if (siteGuid == ArticleWhitelist.siteGuid(_.WEBMD)) {
          val finalGraph = WebMDDictionary.addToArticleGraph(article, conceptGraph)
          Some(finalGraph)
        } else {
          Some(conceptGraph)
        }
      }
      case Some(conceptGraph) => Some(conceptGraph)
      case None => None
    }

    val allGraphs = Seq(tagGraph, PhraseConceptGraphBuilder.buildPhraseConceptGraph(contentToGraph))
    val phrasePlusTagGraph = allGraphs.flatten.foldLeft(StoredGraph.makeEmptyGraph)(_ + _)

    SiteSpecificGraphingAlgoResult(
      autoStoredGraph = Some(StoredGraph(ig)),
      annoStoredGraph = None,
      conceptGraph = conceptGraphOption,
      phraseConceptGraph = Some(phrasePlusTagGraph),
      ig = Some(ig)
    )
  }

  override def graphArticleRowDomain(article: ArticleRowDomainDoNotUseAnymorePlease, counter: CounterFunc = CounterNoOp)(implicit ogName: OntologyGraphName): SiteSpecificGraphingAlgoResult = {
    counter("Default site grapher ops begun", 1l)
    val publishDate = article.publishDateOption.getOrElse(new DateTime())
    val title = article.title
    val url = article.url
    val tags = article.tags
    val contentToGraph = Buffer[ContentToGraph]()
    val siteGuid = article.siteGuid

    val content = article.content

    for ((key, value) <- urlsToTopics) {
      if (url.contains(key)) {
//        println("Adding custom topic: " + value)
        contentToGraph += ContentToGraph(url, publishDate, value, Weight.High, ContentType.Topic)
      }
    }


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

    val grapher = new Grapher(contentToGraph, excludeTopicUris = Annotations.badTopics.badTopics, excludeConceptUris = Annotations.badLinks,
      termVector1 = article.termVector1, termVector2 = article.termVector2, termVector3 = article.termVector3, termVectorG = article.termVectorG,
      phraseVectorKea = article.phraseVectorKea)

    //LAYERED ALGO
    val ig = grapher.interestGraph(GrapherAlgo.LayeredAlgo)

    val storedGraph = {
      if (siteGuid == ArticleWhitelist.siteGuid(_.WEBMD)) {
        // TJC-6566-REVIEW: In general, should review all references to ArticleWhitelist.siteGuid(_.WEBMD)

        val storedGraph = StoredGraph(ig)
        val webMdTerms = WebMDDictionary.termsInString(title) ++ WebMDDictionary.termsInString(article.keywords) ++ WebMDDictionary.termsInString(article.summary)
        val builder = StoredGraph.builderFrom(storedGraph)

        webMdTerms.foreach {
          term =>
            builder.addNode(term.uri, term.term, NodeType.Topic, 100, 1, 0.5)
        }
        builder.build
      } else {
        StoredGraph(ig)
      }
    }


    SiteSpecificGraphingAlgoResult(
      autoStoredGraph = Some(storedGraph),
      annoStoredGraph = None,
      conceptGraph = buildConceptGraph(grapher, counter, this.buildConceptGraph),
      phraseConceptGraph = PhraseConceptGraphBuilder.buildPhraseConceptGraph(contentToGraph),
      ig = Some(ig)
    )
  }
}

abstract class BaseSiteGrapherAlgo(val algoId: Int = 1, val algoVersion: Int = 1) {


  def graphArticleRowDomain(article: ArticleRowDomainDoNotUseAnymorePlease, counter: CounterFunc = CounterNoOp)(implicit ogName: OntologyGraphName): SiteSpecificGraphingAlgoResult

  // def graphArticleRow(article: ArticleRow, counter: CounterFunc = CounterNoOp): SiteSpecificGraphingAlgoResult
  def graphArticleRow(article: ArticleLike, counter: CounterFunc = CounterNoOp)(implicit ogName: OntologyGraphName): SiteSpecificGraphingAlgoResult

  protected def grapherFollowedATag(grapher: Grapher) = grapher.scoredTopics.exists(_.topic.followedNodes.exists(_.isTag))
}