package com.gravity.interests.jobs.intelligence.operations

import com.gravity.algorithms.model.FeatureSettings
import com.gravity.data.configuration.{ConfigurationQueryService, DlPlacementSetting, SitePlacementRow}
import com.gravity.domain.aol.AolUniFieldNames
import com.gravity.interests.jobs.intelligence.SitePlacementIdKey
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.operations.urlvalidation.UrlValidationService
import com.gravity.utilities.ScalaMagic
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.analytics.articles.AolMisc
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvprimitives._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.valueclasses.ValueClassesForDomain._
import play.api.data.validation.ValidationError
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/**
 * Every so often, /w2 (the widget) is requested and the successful response stored on the CDN. These "static" widgets
 * are served to users on high traffic, strict SLA environments (e.g. AOL.com above the fold unit).
 *
 * @see [[StaticWidgetService]]
 */
sealed trait StaticWidget {
  def sitePlacementId: SitePlacementId

  def sitePlacementIdKey: SitePlacementIdKey = SitePlacementIdKey(sitePlacementId)

  def forcedBucket: Option[BucketId] = None

  /**
   * Should be set to TRUE for widgets that *may* be served (even if not always) over HTTPS. This will force all static
   * asset references rendered into the static widget to be served over HTTPS always to avoid "mixed nonsecure
   * content" errors in browser.
   */
  def forceSecureAssets: Boolean = false

  lazy val usesArticlePinning: Boolean = AolUniService.isManagedSitePlacement(sitePlacementId, skipCache = false)

  def destFile: String
  def devDestFile: String
  def wqaDestFile: String

  final val jsonpCallbackName = "grvStaticWidget" + sitePlacementId.raw

  /** @return URL whose content is stored for the static widget on S3. */
  def contentUrl(implicit ctx: GenerateStaticWidgetCtx = GenerateStaticWidgetCtx.defaultCtx): String

  /** Deep validations for content obtained at [[contentUrl]]. */
  def validateContent(content: String): ValidationNel[FailureResult, _ <: Any]

  def timeoutSecs(implicit ctx: GenerateStaticWidgetCtx): Int

  def createFailure(message: String): FailureResult = createFailure(message, None)

  def createFailure(message: String, exception: Exception): FailureResult = createFailure(message, exception.some)

  def createFailure(message: String, exceptionOption: Option[Exception]): FailureResult = {
    FailureResult(s"StaticWidget Failure on sitePlacementId: ${sitePlacementId.raw}: $message", exceptionOption)
  }

  lazy val sp: SitePlacementRow = ConfigurationQueryService.queryRunner.getSitePlacement(sitePlacementId).getOrElse {
    throw new Exception(s"Site-placement ${sitePlacementId.raw} could not be retrieved")
  }
}

class StaticApiWidget(
  override val sitePlacementId: SitePlacementId,
  contextualRecosUrl: String,
  override val forceSecureAssets: Boolean = false,
  override val forcedBucket: Option[BucketId] = None
) extends StaticWidget {

  val spidKeyAsScopedKey: Seq[ScopedKey] = Seq(sitePlacementIdKey.toScopedKey)

  val isPlidRequired: Boolean = {
    FeatureSettings.getScopedSwitch(FeatureSettings.gmsRequirePlid, spidKeyAsScopedKey).value
  }

  val isImageWithinDataField: Boolean = {
    FeatureSettings.getScopedSwitch(FeatureSettings.gmsImageInDataField, spidKeyAsScopedKey).value
  }

  private val successInt: ValidationNel[FailureResult, Int] = 0.successNel

  override def contentUrl(implicit ctx: GenerateStaticWidgetCtx): String = {
    val baseContentUrl = URLUtils.appendParameters(
      ctx.apiBaseUrl,
      "ipOverride" -> "104.174.123.182",
      "placement" -> sitePlacementId.raw.toString,
      "userguid" -> "", // we can't know the userguid in this context, so pass nothing
      "logResult" -> "0", // this request is not used to serve recos to users, so don't log the impression served
      "url" -> URLUtils.urlEncode(contextualRecosUrl)
    )

    forcedBucket match {
      case Some(bucket) => URLUtils.appendParameter(baseContentUrl, "bucket", bucket.raw.toString)
      case None => baseContentUrl
    }
  }

  /** @return The success type is JsArray of articles from the API payload. */
  override def validateContent(content: String): ValidationNel[FailureResult, JsArray] = {
    Json.parse(content) \ "payload" \ "articles" match {
      case articles: JsArray if articles.value.nonEmpty => articles.value.map(validateArticle).extrude.map(JsArray.apply)
      case _: JsArray => createFailure("Got empty list of articles").failureNel
      case x => createFailure(s"Got unexpected value $x for articles").failureNel
    }
  }

  /** @return The success type is the article JsObject. */
  def validateArticle(article: JsValue): ValidationNel[FailureResult, JsObject] = article match {
    case a: JsObject =>
      var articleUrl = "NO_DISPLAY_URL_FOUND_OR_SET"

      // Must have a valid displayUrl
      val urlV = (a \ "displayUrl").tryToString.flatMap(u => {
        articleUrl = u
        u.tryToURL.toJsResult(s"'displayUrl' must be a valid url. '$articleUrl' is not valid.")
      }).toValidation.leftMap(_.toFailureResult(prependContext = s"SP #${sitePlacementId.raw} :: ArticleURL = '$articleUrl'")).toNel

      // Display URL HTTP HEAD call response status OK
      val urlHeadCallV = (a \ "displayUrl").tryToString.flatMap(u => {
        articleUrl = u
        UrlValidationService.validateUrl(u) match {
          case Success(_) =>
            JsSuccess("ok")
          case Failure(fails) =>
            JsError((__, ValidationError(s"Failed to validate articleUrl: '$u'. Failures: ${fails.mkString(" :: AND :: ")}")))
        }

      }).toValidation.leftMap(_.toFailureResult(prependContext = s"SP #${sitePlacementId.raw} :: ArticleURL = '$articleUrl'")).toNel

      // gravityCalculatedPlid is non-empty
      val plidV = if (isPlidRequired) {
        (a \ "data" \ AolUniFieldNames.GravityCalculatedPlid).tryToInt.flatMap(plid => (plid != 0).asJsResult(plid, "gravityCalculatedPlid must not be zero."))
          .toValidation.leftMap(_.toFailureResult(prependContext = s"SP #${sitePlacementId.raw} :: ArticleURL = '$articleUrl'")).toNel
      }
      else {
        successInt
      }

      // Title >= 3 chars
      val titleV = (a \ "title").tryToString.flatMap(t => (t.trim.length >= 3).asJsResult(t, s"Title '$t' must be >= 3 chars."))
        .toValidation.leftMap(_.toFailureResult(prependContext = s"SP #${sitePlacementId.raw} :: ArticleURL = '$articleUrl'")).toNel

      // Images
      val imageV = validateArticleImages(a, articleUrl)

      Seq(urlV, urlHeadCallV, titleV, plidV, imageV).extrude.map(_ => a)

    case _ => createFailure(s"Expected article as JsObject; got $article").failureNel
  }

  /** @return The image URL(s) that should be validated for the given article. */
  def articleImageUrlFromDataFieldGetter(article: JsObject): JsResult[Set[String]] = (article \ "data" \ "image").tryToString.map(Set(_))

  def articleImageUrlGetter(article: JsObject): JsResult[Set[String]] = (article \ "image").tryToString.map(Set(_))

  /** @return The success type is the article JsObject. */
  def validateArticleImages(article: JsObject, articleUrl: String): ValidationNel[FailureResult, JsObject] = {
    val retrievedImageUrls = if (isImageWithinDataField) {
      articleImageUrlFromDataFieldGetter(article)
    }
    else {
      articleImageUrlGetter(article)
    }

    retrievedImageUrls.toValidation.leftMap(_.toFailureResult(prependContext = s"SP #${sitePlacementId.raw} :: ArticleURL = '$articleUrl'")).toNel
      .flatMap(_.toSeq.map(imgUrl => validateArticleImage(imgUrl, articleUrl)).extrude).map(_ => article)
  }

  def validateArticleImage(imageUrl: String, articleUrl: String): ValidationNel[FailureResult, String] = imageUrl match {
    case mt if ScalaMagic.isNullOrEmpty(mt) => createFailure(s"Empty image URL for article: '$articleUrl'").failureNel[String]

    case _ =>
      UrlValidationService.validateImageUrl(imageUrl) match {
        case Success(_) =>
          imageUrl.successNel

        case Failure(fails) =>
          fails.<::(createFailure(s"URL/Image validation failed for imageUrl: '$imageUrl' belonging to article: '$articleUrl'")).failure
      }
  }

  override def timeoutSecs(implicit ctx: GenerateStaticWidgetCtx): Int = ctx.apiTimeoutSecs

  override val destFile = s"staticApiWidget${sitePlacementId.raw}.json"
  override val devDestFile = s"devStaticApiWidget${sitePlacementId.raw}.json"
  override val wqaDestFile = s"wqaStaticApiWidget${sitePlacementId.raw}.json"
}

object StaticWidget {
  def allStaticWidgets: Set[StaticWidget] = {
    ConfigurationQueryService.queryRunner.getAllSitePlacements(includeInactive = false)
        .filter(_.generateStaticJson).map(StaticWidget.fromSitePlacementRow).toSet
  }

  def allStaticWidgetsUsingPinning: Set[StaticWidget] = allStaticWidgets.filter(_.usesArticlePinning)

  def find(sitePlacementId: SitePlacementId): Option[StaticWidget] = allStaticWidgets.find(_.sitePlacementId == sitePlacementId)

  def fromSitePlacementRow(row: SitePlacementRow): StaticWidget = {
    row.siteGuid match {
      // "Full DL" AOL widget
      case AolMisc.aolSiteGuid if DlPlacementSetting.gmsMinArticles(None, row.sp.some) > 0 =>
        new AolFullDlStaticApiWidget(row.sp, row.contextualRecosUrlOverride, row.forceBucket.map(_.asBucketId))

      // AOL widget
      case AolMisc.aolSiteGuid =>
        new AolDlStaticApiWidget(row.sp, row.contextualRecosUrlOverride, row.forceBucket.map(_.asBucketId))

      // GMS widet or other
      case _ =>
        new StaticApiWidget(row.sp, row.contextualRecosUrlOverride, forcedBucket = row.forceBucket.map(_.asBucketId))
    }
  }

  implicit val jsonWrites: Writes[StaticWidget] = (
    (__ \ "sitePlacementId").write[Long] and
    (__ \ "destFile").write[String] and
    (__ \ "devDestFile").write[String] and
    (__ \ "wqaDestFile").write[String] and
    (__ \ "contentUrl").write[String]
  )(sw => (sw.sitePlacementId.raw, sw.destFile, sw.devDestFile, sw.wqaDestFile, sw.contentUrl))
}

object StaticWidgetAppToPrintStuffForRobbie extends App {
  val all = StaticWidget.allStaticWidgets.toSeq.sortBy(sw => {
    val sp = sw.sp // go ahead and set this lazy val now
    sw.sitePlacementId.raw
  })


  all.foreach(sw => println(s"${sw.sitePlacementId.raw}: '${sw.sp.displayName}'"))
}
