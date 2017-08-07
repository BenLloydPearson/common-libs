package com.gravity.interests.jobs.intelligence.operations.recommendations.model

import java.text.SimpleDateFormat

import com.gravity.goose.utils.Logging
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.{ArticleKey, ArticleRecommendation, SitePlacementIdBucketKey}
import com.gravity.valueclasses.ValueClassesForDomain.{BucketId, SitePlacementId}
import org.joda.time.DateTime

import scala.collection.Seq

/**
 * Created by agrealish14 on 7/7/16.
 */
case class RecoGenEvent(id:String,
                        timestamp:DateTime,
                        scope:ScopedKey,
                        candidateSetEvent: CandidateSetEvent,
                        worksheetEvent: WorksheetEvent) {



}

object RecoGenEvent {
 import com.gravity.logging.Logging._

  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  def apply(scope:ScopedKey, candidateSet:CandidateSetEvent, key:RecommendedScopeKey, recos:Seq[ArticleRecommendation]): RecoGenEvent = {

    val now = new DateTime()
    val id = dateFormatter.format(now.toDate)

    val worksheet = WorksheetEvent(key, recos.map(ArticleReco(_)).toList)

    RecoGenEvent(id, now, scope, candidateSet, worksheet)
  }

  val testScope = SitePlacementIdBucketKey(SitePlacementId(5690), BucketId(1)).toScopedKey

  val testCandidateSetEvent = CandidateSetEvent(100, 10)

  val testCandidateSetQualifier = CandidateSetQualifier(Some(5690),
    Some(1),
    Some(0),
    Some(1),
    Some(1))

  val testAlgoStateKey = AlgoStateKey(
    Some(685),
    Some(1234567L),
    Some(1234567L)
  )

  val testRecommendedScopeKey =  RecommendedScopeKey(
    testScope,
    testCandidateSetQualifier,
    testAlgoStateKey
  )

  val testArticleRecommendation = Seq[ArticleRecommendation](
    ArticleRecommendation(
      ArticleKey("http://domain.com/article1"),
      1.0),
    ArticleRecommendation(
      ArticleKey("http://domain.com/article2"),
      2.0),
    ArticleRecommendation(
      ArticleKey("http://domain.com/article3"),
      3.0),
    ArticleRecommendation(
      ArticleKey("http://domain.com/article4"),
      4.0)
  )

  val testRecoGenEvent = RecoGenEvent(testScope,
    testCandidateSetEvent,
    testRecommendedScopeKey,
    testArticleRecommendation)


}

case class CandidateSetEvent(size:Int, zeroImpressionCount:Int)

case class WorksheetEvent(key:RecommendedScopeKey, recos:Seq[ArticleReco])

case class ArticleReco(articleId:ArticleKey, score:Double)

object ArticleReco {
  def apply(articleRecommendation:ArticleRecommendation): ArticleReco = {

    ArticleReco(articleRecommendation.article, articleRecommendation.score)
  }
}

