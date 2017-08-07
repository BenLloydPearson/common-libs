package com.gravity.interests.jobs.intelligence.operations

import com.gravity.data.configuration.DlPlacementSetting
import com.gravity.utilities.analytics.articles.{HardcodedAolNarrowBandPlacementId, AolMisc}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvjson._
import com.gravity.valueclasses.ValueClassesForDomain.{BucketId, SitePlacementId}
import play.api.data.validation.ValidationError
import play.api.libs.json._
import com.gravity.utilities.grvz._

import scalaz.Scalaz._
import scalaz._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

sealed class AolDlStaticApiWidget(sitePlacementId: SitePlacementId, contextualRecosUrl: String, forcedBucket: Option[BucketId] = None)
extends StaticApiWidget(sitePlacementId, contextualRecosUrl, forcedBucket = forcedBucket) {
  /** @return The image URL(s) that should be validated for the given article. */
  override def articleImageUrlFromDataFieldGetter(article: JsObject): JsResult[Set[String]] = {
    val imageFieldNames = if(usesNarrowBandImage) Set("image", "narrowBandImage") else Set("image")
    imageFieldNames.map(imageField => {
      val imageJsPath = __ \ "data" \ imageField
      val imageJsValue = imageJsPath.asSingleJson(article)
      imageJsValue.validate[String].leftMap(errors => JsError(errors :+ (imageJsPath, Seq(
        ValidationError(s"Article was: ${Json.prettyPrint(article)}")
      ))))
    }).extrude
  }

  def usesNarrowBandImage: Boolean = sitePlacementId == HardcodedAolNarrowBandPlacementId.aolDlNarrowbandSitePlacementId  // (in AolDlStaticApiWidget, ok for now)
}

/** The "full DL" widgets are widgets requiring a minimum number of recos to be considered viable. */
sealed class AolFullDlStaticApiWidget(sitePlacementId: SitePlacementId, contextualRecosUrl: String, forcedBucket: Option[BucketId] = None)
extends AolDlStaticApiWidget(sitePlacementId, contextualRecosUrl, forcedBucket) {
  override def validateContent(content: String): ValidationNel[FailureResult, JsArray] = {
    val minArticles = DlPlacementSetting.gmsMinArticles(None, sitePlacementId.some)
    super.validateContent(content).flatMap {
      case articles if articles.value.length >= minArticles => articles.successNel
      case articles  => FailureResult(s"Got ${articles .value.length} articles but expected at least $minArticles").failureNel
    }
  }
}

