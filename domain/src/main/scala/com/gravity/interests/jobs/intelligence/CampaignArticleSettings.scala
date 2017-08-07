package com.gravity.interests.jobs.intelligence

import java.io.{IOException, ObjectInputStream}

import com.gravity.hbase.schema.ComplexByteConverter
import com.gravity.interests.jobs.intelligence.SchemaTypes.CampaignArticleSettingsConverter
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvenum.GrvEnum._
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.ArticleReviewStatus
import com.gravity.valueclasses.ValueClassesForDomain._
import net.liftweb.json.JsonAST.JValue
import net.liftweb.json.{Formats, Serializer, TypeInfo}
import play.api.libs.json.Json.JsValueWrapper
import play.api.libs.json._

import scala.collection.mutable

@SerialVersionUID(1L)
case class CampaignArticleSettings(status: CampaignArticleStatus.Type = RssFeedSettings.default.initialArticleStatus,
                                   isBlacklisted: Boolean = RssFeedSettings.default.initialArticleBlacklisted,
                                   clickUrl: Option[String] = None,
                                   title: Option[String] = None,
                                   image: Option[String] = None,
                                   displayDomain: Option[String] = None,
                                   isBlocked: Option[Boolean] = None,
                                   blockedReasons: Option[Set[BlockedReason.Type]] = None,
                                   articleImpressionPixel: Option[String] = None,
                                   articleClickPixel: Option[String] = None,
                                   trackingParams: Map[String, String] = Map.empty[String, String]) {

  import com.gravity.utilities.Counters._

  /**
   * Override the default Java readObject behavior to correct the deserialization of newly-added fields.
   *
   * @param in The object stream being read from.
   */
  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    // default deSerialization
    in.defaultReadObject()

    countPerSecond(counterCategory, "CampaignArticleSettings.readObject Called")

    case class NullableValue[T](value: T, default: T) {
      def isNull: Boolean = value == null
      def getOrDefault: T = Option(value).getOrElse(default)
    }


    // New fields may come in as null from an older version of a serialized CampaignArticleSettings.
    // Check for this and replace the evil null with a happy None.
    val newerOptions = Map(
         "isBlocked" -> NullableValue(isBlocked, None)
        ,"blockedReasons" -> NullableValue(blockedReasons, None)
        ,"articleImpressionPixel" -> NullableValue(articleImpressionPixel, None)
        ,"articleClickPixel" -> NullableValue(articleClickPixel, None)
        ,"trackingParams" -> NullableValue(trackingParams, Map.empty[String, String])
    )

    for{
      (casName, casVal) <- newerOptions
      if casVal.isNull
    } {
      val field = this.getClass.getDeclaredField(casName)
      field.setAccessible(true)

      field.set(this, casVal.default)
    }
  }

  // This JSON-like toString is just used for things like trace messages, e.g. as called from CampaignArticleAddResult.toString.
  override def toString: String = {
    countPerSecond(counterCategory, "CampaignArticleSettings.toString Called")
    //CampaignArticleSettingsCounters.info(new Exception, "CampaignArticleSettings.toString Called")

    val kvs = new mutable.HashMap[String, JsValue]()

    // This serialization mimics the previous "hand built" serialization logic exactly
    kvs += "status" -> status.toString
    kvs += "isBlacklisted" -> isBlacklisted
    kvs += "clickUrl" -> clickUrl.flatMap(_.noneForEmpty).fold[JsValue](JsNull)(JsString.apply)
    kvs += "title" -> title.flatMap(_.noneForEmpty).fold[JsValue](JsNull)(JsString.apply)
    kvs += "image" -> image.flatMap(_.noneForEmpty).fold[JsValue](JsNull)(JsString.apply)
    kvs += "domain" -> displayDomain.flatMap(_.noneForEmpty).fold[JsValue](JsNull)(JsString.apply)
    if(isBlocked.exists(_ == true))
      kvs += "isBlocked" -> true

    blockedReasons.flatMap(_.toNel).foreach {
      case reasons =>
        if(CampaignArticleSettingsSerializer.version <= 3)
          kvs += "blockedWhy" -> reasons.list.map(x => BlockedReasonHelper(x.name)).reduce(_ + _).toString
        else
          kvs += "blockedReasons" -> reasons.list.toSet.toString
    }

    articleImpressionPixel.flatMap(_.noneForEmpty).foreach {
      case html =>
        kvs += "articleImpressionPixel" -> html
    }

    articleClickPixel.flatMap(_.noneForEmpty).foreach {
      case html =>
        kvs += "articleClickPixel" -> html
    }

    if (trackingParams.nonEmpty) {
      kvs += "trackingParams" -> Json.toJson(trackingParams)
    }

    Json.stringify(JsObject(kvs.toSeq))
  }

  @transient lazy val isActive: Boolean = status == CampaignArticleStatus.active && !isBlacklisted && !isBlocked.getOrElse(false)
  @transient lazy val hasOverride: Boolean = clickUrl.isDefined || image.isDefined || title.isDefined || displayDomain.isDefined

  def effectiveImage(articleImageUrl: ImageUrl): ImageUrl = {
    val campImgUrl = for {
      campUrl <- image
      nonEmptyUrl <- campUrl.noneForEmpty
    } yield ImageUrl(nonEmptyUrl)

    campImgUrl.getOrElse(articleImageUrl)
  }

  def effectiveTitle(articleTitle: Title): Title = {
    val campTitle = for {
      campTitle <- title
      nonEmptyTitle <- campTitle.noneForEmpty
    } yield Title(nonEmptyTitle)

    campTitle.getOrElse(articleTitle)
  }

  def applyArticleTrackingParams(forUrl: String): String = if (trackingParams.isEmpty) {
    forUrl
  }
  else {
    URLUtils.replaceParameters(forUrl, trackingParams)
  }

  def equateActiveAndBlacklisted(that: CampaignArticleSettings): Boolean = {
    this.isBlacklisted == that.isBlacklisted && this.status == that.status
  }

}

object CampaignArticleSettings {
  val activeDefault: CampaignArticleSettings = CampaignArticleSettings(CampaignArticleStatus.active, isBlacklisted = false)
  val inactiveDefault: CampaignArticleSettings = CampaignArticleSettings(CampaignArticleStatus.inactive, isBlacklisted = true)

  def getDefault(isActive: Boolean = false): Option[CampaignArticleSettings] = if (isActive) Some(activeDefault) else Some(inactiveDefault)

  implicit val setBlockedReasonFormat: Format[Set[BlockedReason.Type]] = Format(Reads[Set[BlockedReason.Type]] {
    case jsArray: JsArray =>
      jsArray.value.map(BlockedReason.jsonFormat.reads).extrude.map(_.toSet)
    case _ =>  JsError()
  }, Writes[Set[BlockedReason.Type]] { reasonSet => JsArray(reasonSet.map(Json.toJson(_)).toSeq) })



  def blockedReasonReads(obj: JsObject): JsResult[Option[Set[BlockedReason.Type]]] = {
    if(CampaignArticleSettingsSerializer.version <= 3)
      (obj \ "blockedWhy").validateOptWithNoneOnAnyFailure[String].map(_.map(BlockedReasonHelper(_).toBlockedReasonSet))
    else
      (obj \ "blockedReasons").validateOptWithNoneOnAnyFailure[Set[BlockedReason.Type]]
  }
  def blockedReasonWrites(cas: CampaignArticleSettings): Option[(String,JsValueWrapper)] = {
    if(CampaignArticleSettingsSerializer.version <= 3)
      cas.blockedReasons.map(set => "blockedWhy" -> JsString(set.map(x => BlockedReasonHelper(x.name)).reduceOption(_ + _).map(_.raw).getOrElse("")))
    else
      cas.blockedReasons.map("blockedReasons" -> _)

  }

  implicit val jsonFormat: Format[CampaignArticleSettings] = Format(Reads[CampaignArticleSettings] {
    case obj: JsObject =>
      for {
        status <- (obj \ "status").tryWithDefaultOnAnyFailure(RssFeedSettings.default.initialArticleStatus)
        isBlacklisted <- (obj \ "isBlacklisted").tryWithDefaultOnAnyFailure(RssFeedSettings.default.initialArticleBlacklisted)
        clickUrl <- (obj \ "clickUrl").validateOptWithNoneOnAnyFailure[String]
        title <- (obj \ "title").validateOptWithNoneOnAnyFailure[String]
        image <- (obj \ "image").validateOptWithNoneOnAnyFailure[String]
        displayDomain <- (obj \ "displayDomain").validateOptWithNoneOnAnyFailure[String]
        isBlocked <- (obj \ "isBlocked").validateOptWithNoneOnAnyFailure[Boolean]
        blockedReasons <- blockedReasonReads(obj)
        articleImpressionPixel <- (obj \ "articleImpressionPixel").validateOptWithNoneOnAnyFailure[String]
        articleClickPixel <- (obj \ "articleClickPixel").validateOptWithNoneOnAnyFailure[String]
        trackingParams <- (obj \ "trackingParams").tryWithDefaultOnAnyFailure(Map.empty[String, String])
      } yield CampaignArticleSettings(
        status,
        isBlacklisted,
        clickUrl,
        title,
        image,
        displayDomain,
        isBlocked,
        blockedReasons,
        articleImpressionPixel,
        articleClickPixel,
        trackingParams
      )
    case _ => JsError()
  }, Writes[CampaignArticleSettings]{case cas =>
    val casRequiredElementList: List[(String,JsValueWrapper)] = List(
      "status" -> cas.status,
      "isBlacklisted" -> cas.isBlacklisted
    )
    val casOptElementList: List[Option[(String,JsValueWrapper)]] = List(
      cas.clickUrl.map("clickUrl" -> _),
      cas.title.map("title" -> _),
      cas.image.map("image" -> _),
      cas.displayDomain.map("displayDomain" -> _),
      cas.isBlocked.map("isBlocked" -> _),
      blockedReasonWrites(cas),
      cas.articleImpressionPixel.map("articleImpressionPixel" -> _),
      cas.articleClickPixel.map("articleClickPixel" -> _),
      if (cas.trackingParams.isEmpty) None else Some("trackingParams" -> cas.trackingParams)
    )
    val casElementList = casRequiredElementList ::: casOptElementList.flatten

    Json.obj(casElementList: _*)
  })

}

object CampaignArticleSettingsSerializer extends Serializer[CampaignArticleSettings] {

  import net.liftweb.json.JsonDSL._

  //this is a var so tha tthe tests can change it and get the correct JSON version out
  var version: Int = CampaignArticleSettingsConverter.writingVersion

  private val campaignArticleSettingsClass = classOf[CampaignArticleSettings]

  def blockedReasonForVersion(cas: CampaignArticleSettings)(implicit format: Formats): (String, JValue) = {
    if (version <= 3)
      ("blockedWhy", cas.blockedReasons.map(_.map(x => BlockedReasonHelper(x.name)).reduceOption(_ + _).map(_.raw).getOrElse("")))
    else
      ("blockedReasons", cas.blockedReasons)
  }

  def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
    case cas: CampaignArticleSettings =>
      val baseBuild = ("status", cas.status.toString) ~
        ("isBlacklisted", cas.isBlacklisted) ~
        ("clickUrl", cas.clickUrl) ~
        ("image", cas.image) ~
        ("title", cas.title) ~
        ("displayDomain", cas.displayDomain) ~
        ("isBlocked" , cas.isBlocked) ~
        blockedReasonForVersion(cas) ~
        ("articleImpressionPixel", cas.articleImpressionPixel) ~
        ("articleClickPixel", cas.articleClickPixel)

      if (cas.trackingParams.isEmpty) baseBuild else baseBuild ~ ("trackingParams", cas.trackingParams)

  }

  def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), CampaignArticleSettings] = {
    case (TypeInfo(`campaignArticleSettingsClass`, _), json) => {
      val status = CampaignArticleStatus.parseOrDefault((json \\ "status").extract[String])
      val isBlacklisted = (json \\ "isBlacklisted").extract[Boolean]
      val clickUrl = (json \\ "clickUrl").extractOpt[String]
      val title = (json \\ "title").extractOpt[String]
      val image = (json \\ "image").extractOpt[String]
      val displayDomain = (json \\ "displayDomain").extractOpt[String]
      val isBlocked = (json \\ "isBlocked").extractOpt[Boolean]
      val blockedReasons = if (version <= 3) {
        (json \\ "blockedWhy").extractOpt[String].map(BlockedReasonHelper(_).toBlockedReasonSet)
      }
      else
        (json \\ "blockedReasons").extractOpt[List[String]].map(_.map(BlockedReason.parseOrDefault).toSet)
      val articleImpressionPixel = (json \\ "articleImpressionPixel").extractOpt[String]
      val articleClickPixel = (json \\ "articleClickPixel").extractOpt[String]
      val articleReviewStatusName = (json \\ "articleReviewStatus").extractOpt[String]
      val articleReviewStatus = articleReviewStatusName.map(ArticleReviewStatus.parseOrDefault)
      val trackingParams = (json \\ "trackingParams").extractOrElse(Map.empty[String, String])

      CampaignArticleSettings(status, isBlacklisted, clickUrl, title, image, displayDomain, isBlocked, blockedReasons,
        articleImpressionPixel, articleClickPixel, trackingParams)
    }
  }
}

object BlockedReason extends GrvEnum[Short] {
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Short, name: String): Type = Type(id, name)

  def defaultValue: Type = UNKNOWN

  val UNKNOWN: Type = Value(0, "Unknown")
  //-- Start of names that match the V3 CampaignArticleSettings blockedWhy strings.
  // Do not change their name while we support V3 reading or writing
  val EMPTY_AUTHOR: Type = Value(1, "Empty Author")
  val EMPTY_IMAGE: Type = Value(2, "Empty Image")
  val EMPTY_TAGS: Type = Value(3, "Empty Tags")
  val IMAGE_NOT_CACHED: Type = Value(4, "Image Not Cached")
  val TAGS_NO_INCLUDES_MATCH: Type = Value(5, "Did Not Match Any Include Tags")
  val TAGS_EXCLUDE_MATCH: Type = Value(6, "Match Exclude Tag Value(s)")
  //-- END V3 names
  val TITLE_NO_INCLUDES_MATCH: Type = Value(7, "Did Not Match Any Include Keywords in Title")
  val TITLE_EXCLUDE_MATCH: Type = Value(8, "Matched Exclude Keyword(s) in Title")

  @transient implicit val byteConverter: ComplexByteConverter[Type] = shortEnumByteConverter(this)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}
