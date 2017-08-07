package com.gravity.interests.jobs.intelligence.operations

import com.gravity.domain.aol._
import com.gravity.domain.gms.{UniArticleId, GmsArticleStatus}
import com.gravity.interests.jobs.intelligence.ArticleKey
import com.gravity.utilities.analytics.articles.AolMisc
import com.gravity.utilities.grvcoll._
import com.gravity.utilities.grvz._

import play.api.libs.json._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

case class AolGmsKeyQueryResult(pageCount: Int, gmsArticleKeys: List[ArticleKey], totalItems: Int, countsByStatus: GmsArticleStatusCounts)

/**
  * @param similarArticles Keys are article keys as found in [[articles]]; values are articles deemed similar to the
  *                        article specified by the map key. This field is most often *NOT* present due to the extra
  *                        overhead querying similar articles on the fly. [[withSimilarArticles]] may be used to populate
  *                        the field post-instantiation.
  */
case class AolGmsQueryResult(pageCount: Int, articles: List[AolGmsArticle],
                             totalItems: Int, countsByStatus: GmsArticleStatusCounts,
                             totalMetrics: MetricsBase,
                             similarArticles: Map[UniArticleId, List[AolGmsArticle]] = Map.empty) {
  /** @return A copy of this result with [[similarArticles]] populated. */
  def withSimilarArticles: AolGmsQueryResult = {
    if(similarArticles.nonEmpty)
      this
    else
      copy(
        similarArticles = articles.foldLeft(Map.empty[UniArticleId, List[AolGmsArticle]]) { case (similarArticlesAccum, subjectArticle) =>
          val subjectArticleSimilars = SiteService.siteGuid(subjectArticle.siteKey) match {
            case None =>
              Nil

            case Some(siteGuid) =>
              GmsArticleIndexer.similarArticles(subjectArticle.title, excludeArticle = Option(subjectArticle), siteGuid = Option(siteGuid))
                .flatMap(arts => GmsService.getOrderedGmsArticlesFromHbase(arts, withMetrics = false))
                .valueOr(_ => Seq.empty[AolGmsArticle])
          }

          if (subjectArticleSimilars.isEmpty)
            similarArticlesAccum
          else
            similarArticlesAccum + (subjectArticle.uniArticleId -> subjectArticleSimilars.toList)
        }
      )
  }
}

object AolGmsQueryResult {
  private implicit val countsJsonWrites = GmsArticleStatusCounts.jsonWrites
  private implicit val metricsJsonWrites = MetricsBase.jsonWrites
  implicit val mapGmsArtIdToArticlesJsonWrites = // Json.writes[Map[GmsArticleId, List[AolGmsArticle]]]
    Writes[Map[UniArticleId, List[AolGmsArticle]]](akToArticles => {
    JsObject(akToArticles.mapKeys(_.articleKey.articleId.toString).mapValues(Json.toJson(_)).toSeq)
  })
  implicit val jsonWrites = Json.writes[AolGmsQueryResult]
}

case class GmsArticleStatusCounts(pending: Int, rejected: Int, live: Int, expired: Int, deleted: Int, approved: Int)

object GmsArticleStatusCounts {
  implicit val jsonWrites = Writes[GmsArticleStatusCounts](counts => Json.obj(
    "pending" -> counts.pending,
    "rejected" -> counts.rejected,
    "live" -> counts.live,
    "expired" -> counts.expired,
    "deleted" -> counts.deleted,
    "approved" -> counts.approved
  ))

  val empty: GmsArticleStatusCounts = GmsArticleStatusCounts(0, 0, 0, 0, 0, 0)

  def apply(countsByStatusName: Map[String, Int]): GmsArticleStatusCounts = {
    val withDefault = countsByStatusName.withDefaultValue(0)
    GmsArticleStatusCounts(
      withDefault(GmsArticleStatus.Pending.name),
      withDefault(GmsArticleStatus.Rejected.name),
      withDefault(GmsArticleStatus.Live.name),
      withDefault(GmsArticleStatus.Expired.name),
      withDefault(GmsArticleStatus.Deleted.name),
      withDefault(GmsArticleStatus.Approved.name)
    )
  }
}