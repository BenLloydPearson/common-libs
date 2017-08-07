package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence._
import scalaz.{Failure, Success}
import scalaz.Failure
import scalaz.Success

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 11/4/13
 * Time: 2:56 PM
 * To change this template use File | Settings | File Templates.
 */
object UserGraphGenerationService {

  def dynamicClickstreamQualityGraph(user: UserSiteRow, graphType: GraphType = GraphType.ConceptGraph, maxArticles: Int = 200, nonEmptyGraphsToConsider: Int = 100) = {
    validateGraphType(graphType)

    val articleKeysSorted = user.interactionClickStream.toSeq.sortBy(-_._1.hour.getMillis).take(maxArticles).map(_._1.articleKey)
    ArticleService.fetchMulti(articleKeysSorted.toSet, skipCache = false)(_.withColumns(_.title, _.conceptStoredGraph, _.phraseConceptGraph)) match {
      case Success(articles) => {

        var cnt = 0
        // get graphs based on time sort
        val graphs =
          for {articleKey <- articleKeysSorted if cnt < nonEmptyGraphsToConsider
               articleRow <- articles.get(articleKey)
               g = articleRow.getGraph(graphType)
               if g.nodes.size > 0 &&
                 GraphQuality.isGoodTfIdfGraph(getCorrespondingLiveTfIdfGraph(graphType, articleRow), 4, 0.1, 10) >= 0} yield {
            cnt += 1
            g
          }

        graphs.foldLeft(StoredGraph.EmptyGraph)(_ plusOne _)
      }
      case Failure(fails) => StoredGraph.EmptyGraph
    }
  }

  def dynamicClickstreamGraph(user: UserSiteRow, graphType: GraphType = GraphType.ConceptGraph, maxArticles: Int = 100) = {
    validateGraphType(graphType)

    val articleKeys = user.interactionClickStream.toSeq.sortBy(-_._1.hour.getMillis).take(maxArticles).map(_._1.articleKey).toSet
    ArticleService.fetchMulti(articleKeys, skipCache = false)(_.withColumns(_.title, _.conceptStoredGraph, _.phraseConceptGraph)) match {
      case Success(articles) => {
        articles.map(_._2).foldLeft(StoredGraph.EmptyGraph)(_ plusOne _.getGraph(graphType))
      }
      case Failure(fails) => StoredGraph.EmptyGraph
    }
  }

  private def getCorrespondingLiveTfIdfGraph(graphType: GraphType, articleRow: ArticleRow): StoredGraph = {
    if (graphType == GraphType.PhraseConceptGraph) articleRow.liveTfIdfPhraseGraph
    else articleRow.liveTfIdfGraph
  }

  private def validateGraphType(graphType: GraphType) {
    if (graphType != GraphType.PhraseConceptGraph && graphType != GraphType.ConceptGraph) {
      throw new Exception("GraphType " + graphType.name + " not supported for generating user graph")
    }
  }

}
