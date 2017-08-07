package com.gravity.interests.jobs.intelligence

import com.gravity.domain.recommendations.ContentGroup
import com.gravity.interests.jobs.intelligence.operations.AlgoSettingsData
import com.gravity.interests.jobs.intelligence.operations.recommendations.CampaignRecommendationData
import com.gravity.logging.Logstashable
import com.gravity.utilities.grvstrings.emptyString
import com.gravity.utilities.grvjson
import com.gravity.valueclasses.ValueClassesForDomain.ExchangeGuid
import play.api.libs.json._

import scala.collection._
import scalaz.syntax.std.option._


abstract class RecommendationBase() {

  def length: Int

  def isEmpty: Boolean = length == 0

  def algorithm: Int

  def trim(maximum:Int) : RecommendationBase

  def prettyPrint()

}

case class SectionRecommendation(section: SectionKey, score: Double)


// used for storing feed back as to how often and what position an article was generated in
case class RecommendationArticleLog(positionOne: Int = 0, positionTwo: Int = 0, positionThree: Int = 0, positionFour: Int = 0, positionFive: Int = 0, lastGenDate: Long)

case class DatabaseSegmentCompositeKey(sitePlacementId: Long, bucketId: Int, sitePlacementConfigVersion: Long) {
  override def toString: String = "{\"sitePlacementId\": " + sitePlacementId + ", \"bucketId\": " + bucketId + ", \"sitePlacementConfigVersion\": " + sitePlacementConfigVersion + "}"
}

case class SegmentRecoKey(segmentId: DatabaseSegmentCompositeKey, recommenderId: Long, slotIndex: Int)

object SegmentRecoKey {
  val version: Byte = 3
}

@SerialVersionUID(8765370767247169589l)
case class ArticleRecommendation(article: ArticleKey, score: Double, why: String = "", campaign: Option[CampaignRecommendationData] = None,
                                 algoSettings: Seq[AlgoSettingsData] = Seq.empty[AlgoSettingsData], contentGroup: Option[ContentGroup] = None, exchangeGuid: Option[ExchangeGuid] = None) {

  def getCampaignOverrideTitle: Option[String] =  {
    campaign.flatMap(_.settings.flatMap(_.title))
  }
}

object ArticleRecommendation {
  implicit val campaignRecommendationDataJsonFormat: Format[CampaignRecommendationData] = CampaignRecommendationData.jsonFormat

  import play.api.libs.functional.syntax._
  implicit val jsonFormat: Format[ArticleRecommendation] = (
    (__ \ "article").format[ArticleKey] and
    (__ \ "score").format[Double](grvjson.liftwebFriendlyPlayJsonDoubleFormat) and
    (__ \ "why").formatNullable[String] and
    (__ \ "campaign").formatNullable[CampaignRecommendationData] and
    (__ \ "algoSettings").formatNullable[Seq[AlgoSettingsData]] and
    (__ \ "contentGroup").formatNullable[ContentGroup] and
    (__ \ "exchangeGuid").formatNullable[ExchangeGuid]
  )(
    {
      case (article, score, whyOpt, campaign, algoSettingsOpt, contentGroup, exchangeGuid) =>
        ArticleRecommendation(article, score, whyOpt.getOrElse(emptyString), campaign, algoSettingsOpt.getOrElse(Nil), contentGroup, exchangeGuid)
    },
    ar => (ar.article, ar.score, ar.why.some, ar.campaign, ar.algoSettings.some, ar.contentGroup, ar.exchangeGuid)
  )
}

@SerialVersionUID(3361621965681370663l)
case class ArticleRecommendationsSection(key: SectionKey, score: Double, why: String = "", articles: Seq[ArticleRecommendation],
                                         algoSettings: Seq[AlgoSettingsData] = Seq.empty[AlgoSettingsData]) extends Logstashable {
  def getKVs: scala.Seq[(String, String)] = Seq(("arskey", key.toString), ("arswhy", why), ("arsarticlerecos", articles.mkString(",")))
  def isDefaultSection: Boolean = if (key.siteId == -1) true else false
}

object ArticleRecommendationsSection {
  implicit val articleRecommendationJsonFormat: Format[ArticleRecommendation] = ArticleRecommendation.jsonFormat

  import play.api.libs.functional.syntax._
  implicit val jsonFormat: Format[ArticleRecommendationsSection] = (
    (__ \ "key").format[SectionKey] and
    (__ \ "score").format[Double](grvjson.liftwebFriendlyPlayJsonDoubleFormat) and
    (__ \ "why").formatNullable[String] and
    (__ \ "articles").formatNullable[Seq[ArticleRecommendation]] and
    (__ \ "algoSettings").formatNullable[Seq[AlgoSettingsData]]
  )(
    {
      case (key, score, whyOpt, articlesOpt, algoSettingsOpt) =>
        ArticleRecommendationsSection(key, score, whyOpt.getOrElse(emptyString), articlesOpt.getOrElse(Nil), algoSettingsOpt.getOrElse(Nil))
    },
    ars => (ars.key, ars.score, ars.why.some, ars.articles.some, ars.algoSettings.some)
  )
}