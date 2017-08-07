package com.gravity.interests.jobs.intelligence.operations.recommendations

import com.gravity.domain.articles.AuthorData
import com.gravity.interests.jobs.intelligence.ArticleKey
import com.gravity.interests.jobs.intelligence.schemas.GiltDeal
import com.gravity.utilities.grvstrings._
import play.api.libs.json.{Format, Json}

/** stores what article this user should see in the box */
case class ArticleImageList(path: String, height: Int = -1, width: Int = -1, altText: String = emptyString)

case class Article(url: String,
                   title: String,
                   image: String,
                   sort: Int,
                   summary: String = emptyString,
                   paid: Int = 0,
                   metaLink: String = emptyString,
                   imageList: List[ArticleImageList] = null,
                   customObj: CNNMoneyDataObj = null,
                   author: AuthorData = AuthorData.empty,
                   detailsUrl: Option[String] = None,
                   tagLine: Option[String] = None,
                   city: Option[String] = None,
                   deals: Option[List[GiltDeal]] = None,
                   redirectUrl: Option[String] = None,
                   targetUrl: Option[String] = None) {
  def isBehindPaywall: Boolean = paid > 0
}

// stores the box with a list of articles the user should see with it's score
/**
* @size indicates how many UI units this box should take up to indicate how much we think someone likes it
* @sectionType would indicate if this box was personalized, etc.. so they can offer different UI treatments
*
*/
case class BoxResult(boxKey: String = emptyString, displayName: String = emptyString, sectionType: String = emptyString,
                     size: Int = 1, sort: Int = 0, score: Double = 0, articleDS: Int = 3, boxUrl: String = emptyString,
                     articles: List[Article] = Nil)


case class SectionData(id: Long, displayName: String, sectionKey: String, orderedArticleKeys: Seq[ArticleKey])

/**
 * generic wrapper class for a recommendation that all recommenders will use to output data back to
 * the wsj api calls
 * @userGuid  The userGuid that uniquely identifies a user
 * @dateGenerated a UNIX timestamp of when this reco was generated
 * @boxes a List of BoxResult objects
 * @uiv is the UI Version that we're presenting to the user
 * @recType is the reco type - 1 = non persionalized, 2 = personalized, 3 = opted out
 */
case class PersonalResult(userGuid: String, dateGenerated: Long, uiv: String = "1", var recType: Int = 1, boxes: List[BoxResult]) {
  /**
   * Copies this PersonalResult, trimming the TOTAL articles across all sections to maxArticles, in order of the sections.
   *
   * This means earlier sections will have more articles.
   */
  def withTotalArticles(maxArticles: Int): PersonalResult = {
    var totalArticles = 0
    val trimmedBoxes = for {
      box <- boxes
      articles = box.articles takeWhile { _ =>
        totalArticles += 1
        totalArticles <= maxArticles
      }
    } yield box.copy(articles = articles)

    this.copy(boxes = trimmedBoxes)
  }

  def flatten: Stream[Article] = boxes.toStream.flatMap(_.articles.toStream)

  def isPersonalized: Boolean = recType == PersonalResult.personalizedRecType
}
object PersonalResult {
  val personalizedRecType = 2
}

// below are used for custom client implementations
sealed trait CustomPersonalizedDataObject

case class CNNMoneyRelatedArticles(contentType: String, url: String, name: String)
object CNNMoneyRelatedArticles {
  implicit val jsonFormat: Format[CNNMoneyRelatedArticles] = Json.format[CNNMoneyRelatedArticles]
}

case class CNNMoneyRelatedVideos(contentType: String, id: String, url: String, name: String)
object CNNMoneyRelatedVideos {
  implicit val jsonFormat: Format[CNNMoneyRelatedVideos] = Json.format[CNNMoneyRelatedVideos]
}

case class CNNMoneyDataObj(pubDate: String, dateline: String, contentType: String, relatedArticles: Seq[CNNMoneyRelatedArticles],
                           relatedVideos: Seq[CNNMoneyRelatedVideos]) extends CustomPersonalizedDataObject
object CNNMoneyDataObj {
  implicit val jsonFormat: Format[CNNMoneyDataObj] = Json.format[CNNMoneyDataObj]
}