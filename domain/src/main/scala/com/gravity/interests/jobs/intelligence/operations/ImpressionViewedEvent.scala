package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.ArticleKey
import com.gravity.logging.Logstashable
import com.gravity.utilities._
import com.gravity.utilities.analytics.URLUtils.NormalizedUrl
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.eventlogging.FieldValueRegistry
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime.HasDateTime
import com.gravity.utilities.grvz._
import com.gravity.valueclasses.ValueClassesForDomain
import com.gravity.valueclasses.ValueClassesForDomain._
import org.joda.time.DateTime
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection._
import scalaz.syntax.validation._
import scalaz.{NonEmptyList, ValidationNel}


/**
 * An impression recorded when a widget is scrolled into the browser window's viewport (becomes "in view").
 *
 * @param clientTime          Actual time of impression viewed event generated on client side.
 * @param iveVersion          Version of event logic from [[com.gravity.interests.jobs.intelligence.operations.ImpressionViewedEventVersion]].
 * @param iveError            Optional ID of [[com.gravity.interests.jobs.intelligence.operations.ImpressionViewedEventError]].
 * @param iveErrorExtraData   Optional extra error notes.
 * @param hashHex             Impression hash.
 */
@SerialVersionUID(-7885792476635992248l)
case class ImpressionViewedEvent(date: Long,
                                 pageViewIdHash: String,
                                 siteGuid: String,
                                 userGuid: String,
                                 sitePlacementId: Long,
                                 userAgent: String,
                                 remoteIp: String,
                                 clientTime: Long,
                                 gravityHost: String,
                                 iveVersion: Int,
                                 iveError: Int = 0,
                                 iveErrorExtraData: String = "",
                                 hashHex: String,
                                 ordinalArticleKeys: Seq[OrdinalArticleKeyPair] = Nil,
                                 notes: String)
  extends MetricsEvent with DiscardableEvent {

  val dateTime = new DateTime(date)

  override def getSiteGuid: String = siteGuid

  def this(vals: FieldValueRegistry) = this(
    vals.getValue[Long](0),
    vals.getValue[String](1),
    vals.getValue[String](2),
    vals.getValue[String](3),
    vals.getValue[Long](4),
    vals.getValue[String](5),
    vals.getValue[String](6),
    vals.getValue[Long](7),
    vals.getValue[String](8),
    vals.getValue[Int](9),
    vals.getValue[Int](10),
    vals.getValue[String](11),
    vals.getValue[String](12),
    vals.getValue[Seq[OrdinalArticleKeyPair]](13),
    vals.getValue[String](14)
  )

  val metricableIds: Seq[Long] = ordinalArticleKeys.map { oak => oak.ak.articleId }

  // INTEREST-3596
  def isMetricableEmpty: Boolean = metricableIds.isEmpty
  def sizeOfMetricable: Integer = metricableIds.size
  def metricableSiteGuid: String = siteGuid
  def metricableEventDate: DateTime = new DateTime(date)

  /**
   * This is based on the supposition that an autopage event was often not originated by the user.
   */
  def isUserAction: Boolean = notes != "autopage"

  def sg: SiteGuid = siteGuid.asSiteGuid
  def spId: SitePlacementId = sitePlacementId.asSitePlacementId

  override def getArticleIds: scala.Seq[Long] = ordinalArticleKeys.map(_.ak.articleId).toSeq
}

/** Ordinal is 0-based position of article in widget. */
@SerialVersionUID(2617915047192634444l)
case class OrdinalArticleKeyPair(ordinal: Int, ak: ArticleKey) {
  def this(vals: FieldValueRegistry) = this(
    vals.getValue[Int](0),
    ArticleKey(vals.getValue[Long](1))
  )

  /** Helps with input for the /ive endpoint. */
  def this(o: Int, au: NormalizedUrl) = this(o, ArticleKey(au))
}

object OrdinalArticleKeyPair {
 import com.gravity.logging.Logging._
  def fromIveJson(ordinalArticleKeysJson: String): ValidationNel[FailureResult, List[OrdinalArticleKeyPair]] = {
    try {
      Json.fromJson[List[OrdinalArticleKeyPair]](Json.parse(ordinalArticleKeysJson)).toValidation.leftMap {
        jsError => nel(jsError.toFailureResult("Couldn't parse ordinal article key pair input: " + ordinalArticleKeysJson))
      }
    }
    catch {
      case e: Exception =>
        FailureResult(e).failureNel
    }
  }

  implicit val jsonFormat: Format[OrdinalArticleKeyPair] = Format(
    Json.reads[OrdinalArticleKeyPair].orElse((
          (__ \ "o").read[Int] and
          (__ \ "au").read[String]
        )((ordinal, articleUrl) => OrdinalArticleKeyPair(ordinal, ArticleKey(articleUrl)))),
    Json.writes[OrdinalArticleKeyPair]
  )

  case class FromIveJsonFailure(fails: NonEmptyList[FailureResult], spId: SitePlacementId, userGuid: String,
                                userAgent: String, iveError: Option[ImpressionViewedEventError.Type] = None,
                                iveErrorExtraData: Option[String] = None) extends Logstashable {
    import com.gravity.logging.Logstashable._
    override def getKVs: Seq[(String, String)] = Seq(
      Message -> "Failed to parse ordinal article key input",
      Failures -> fails.mkString("\n"),
      SitePlacementId -> spId.raw.toString,
      UserGuid -> userGuid,
      UserAgent -> userAgent,
      ImpressionViewedError -> iveError.fold(ImpressionViewedEventError.NO_ERROR.name)(_.name),
      ImpressionViewedErrorExtra -> iveErrorExtraData.getOrElse("n/a")
    )
  }
}

@SerialVersionUID(-7528919044155211285l)
object ImpressionViewedEventVersion extends GrvEnum[Short] {
  type ImpressionViewedEventVersion = Type
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Short, name: String): Type = Type(id, name)

  val UNKNOWN: Type = Value(0, "Unknown version")
  def defaultValue: Type = UNKNOWN

  val V1: Type = Value(1, ">= 10,000 sq px in view")
}

/**
 * Errors that can occur while attempting to capture [[com.gravity.interests.jobs.intelligence.operations.ImpressionViewedEvent]].
 */
object ImpressionViewedEventError extends GrvEnum[Short] {
  type ImpressionViewedEventError = Type
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Short, name: String): Type = Type(id, name)

  val NO_ERROR: Type = Value(-1, "NO_ERROR")

  /** Should in practice be accompanied by browser exception string. */
  val UNEXPECTED_ERROR: Type = Value(0, "UNEXPECTED_ERROR")
  def defaultValue: Type = UNEXPECTED_ERROR

  val NO_POSTMESSAGE_SUPPORT: Type = Value(1, "NO_POSTMESSAGE_SUPPORT")
  val NO_BOUNDING_CLIENT_RECT_SUPPORT: Type = Value(2, "NO_BOUNDING_CLIENT_RECT_SUPPORT")
  val WIDGET_LOADER_NOT_IN_TOP_WINDOW: Type = Value(3, "WIDGET_LOADER_NOT_IN_TOP_WINDOW")
  val INDETERMINATE_DOM_ELEM: Type = Value(4, "INDETERMINATE_DOM_ELEM")
  val NO_CORS_SUPPORT: Type = Value(5, "NO_CORS_SUPPORT")

  /** To be sent when the client (browser) determines the widget is taking too long to load and aborts loading widget. */
  val CLIENT_CONTROLLED_TIMEOUT: Type = Value(6, "CLIENT_CONTROLLED_TIMEOUT")

  /** To be sent when the server determines the widget took too long to load and should not be rendered. */
  val SERVER_CONTROLLED_TIMEOUT: Type = Value(7, "SERVER_CONTROLLED_TIMEOUT")

  val FALLBACK_WIDGET_SERVED: Type = Value(8, "FALLBACK_WIDGET_SERVED")
  val ELEM_IN_VIEW_FUNC_NOT_AVAIL: Type = Value(9, "ELEM_IN_VIEW_FUNC_NOT_AVAIL")

  /**
   * @param asUrlencoded Result of GrvImpressionViewedEventError.toUrlencoded(), the JS method in the JS version of this
   *                     class: [[com.gravity.interests.jobs.intelligence.operations.ImpressionViewedEventError.jsClass]].
   *
   * @return The impression viewed event error that was transmitted via urlencoded and optional extra data that was
   *         transmitted with it. A tuple like:
   *
   *         (
   *           _: Option[ImpressionViewedEventError],
   *           extraData: Option[String]              // Extra data transmitted with the error
   *         )
   */
  def fromUrlencoded(asUrlencoded: String): (Option[ImpressionViewedEventError], Option[String]) = {
    val parts = asUrlencoded split(",", 2)
    parts match {
      case Array() => (None, None)
      case Array(iveErrorId, _*) =>
        val iveError = get(GrvString(iveErrorId).tryToShort.getOrElse(UNEXPECTED_ERROR.id.toShort))
        (iveError, if(parts.length == 1) None else Some(parts(1)))
    }
  }

  /**
   * @return JS version of this class that is similar in usage and namespaced with Grv* prefix.
   */
  def jsClass: String =
    """
      |/**
      | * Use this constructor to create an IVE error with extra data, or use one of the prefab static IVE error instances.
      | *
      | * @param {Number} id          Valid IVE error ID.
      | * @param          [extraData] Optional extra data (arbitrary type; will be converted to String).
      | */
      |if(!window.GrvImpressionViewedEventError) {
      |  window.GrvImpressionViewedEventError = function(id, extraData) {
      |    this.id = id;
      |    this.extraData = extraData;
      |    this.toUrlencoded = function() {
      |      var urlEncoded = this.id;
      |      if(this.extraData !== undefined) {
      |        urlEncoded += ',' + this.extraData;
      |      }
      |      return urlEncoded;
      |    };
      |  };
      |  %s
      |}
    """.stripMargin toString() format (for {
        (_, value) <- valuesMap
        jsPrefabIveError = "GrvImpressionViewedEventError." + value + " = new GrvImpressionViewedEventError(" + value.id + ");"
      } yield jsPrefabIveError).foldLeft("")(_ + "\n" + _)
}

object ImpressionViewedEvent {
  implicit val iveHasDateTime: HasDateTime[ImpressionViewedEvent] with Object {def getDateTime(t: ImpressionViewedEvent): DateTime} = new HasDateTime[ImpressionViewedEvent] {
    def getDateTime(t: ImpressionViewedEvent): DateTime = new DateTime(t.clientTime) //Maybe we can do something cool with this
  }

  implicit val iveHasSiteGuid: ValueClassesForDomain.HasSiteGuid[ImpressionViewedEvent] with Object {def getSiteGuid(t: ImpressionViewedEvent): SiteGuid} = new HasSiteGuid[ImpressionViewedEvent] {
    def getSiteGuid(t: ImpressionViewedEvent): SiteGuid = t.sg
  }

  implicit val iveHasSpid: ValueClassesForDomain.HasSitePlacementId[ImpressionViewedEvent] with Object {def getSitePlacementId(t: ImpressionViewedEvent): SitePlacementId} = new HasSitePlacementId[ImpressionViewedEvent] {
    def getSitePlacementId(t: ImpressionViewedEvent): SitePlacementId = t.spId
  }

  val empty: ImpressionViewedEvent = ImpressionViewedEvent(System.currentTimeMillis(), "", "", "", -1l, "", "", System.currentTimeMillis(), "", -1, 0, "", "",
    List(OrdinalArticleKeyPair(0, ArticleKey("http://www.article0.com")),
      OrdinalArticleKeyPair(1, ArticleKey("http://www.article1.com"))),
    "")
}
