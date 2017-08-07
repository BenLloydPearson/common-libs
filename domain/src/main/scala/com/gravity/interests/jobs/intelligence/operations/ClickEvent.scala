package com.gravity.interests.jobs.intelligence.operations

import com.gravity.domain.FieldConverters
import com.gravity.interests.interfaces.userfeedback.UserFeedbackOption
import com.gravity.interests.jobs.intelligence.operations.GrccableEvent._
import com.gravity.interests.jobs.intelligence.{ArticleKey, SectionPath}
import com.gravity.utilities._
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.eventlogging.FieldValueRegistry
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime.HasDateTime
import com.gravity.valueclasses.ValueClassesForDomain._
import com.gravity.valueclasses.ValueClassesForUtilities._
import com.gravity.valueclasses.{PubId, ValueClassesForDomain}
import org.joda.time.DateTime

import scalaz.{Failure, Success, ValidationNel}
import scalaz.syntax.validation._
import scala.util.matching.Regex
/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/16/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

/**
  * @param sitePlacementId The site-placement in which the click occurred.
  * @param getChosenUserFeedbackOption None means "no user feedback represented by this click event".
  *                                 Some(UserFeedbackOption.none) means "user unvoted".
  */
@SerialVersionUID(3l)
case class ClickEvent(date: Long, pubSiteGuid: String, advertiserSiteGuid: String, userGuid: String,
                      currentUrl: String, article : ArticleRecoData, countryCodeId : Int, geoLocationDesc : String,
                      impressionHash: String, ipAddress: String, gravityHost: String, isMaintenance: Boolean,
                      currentSectionPath: SectionPath, recommendationScope: String, desiredSectionPaths: SectionPath,
                      affiliateId: String, partnerPlacementId: String, sitePlacementId: Long,
                      chosenUserFeedbackOption: Int, toInterstitial: Boolean,
                      interstitialUrl: String, clickFields: Option[ClickFields]) extends GrccableEvent with MetricsEvent with DiscardableEvent {
  import FieldConverters._
  def clickHash : String = impressionHash + getClickDate + article.key.articleId
  def toDelimitedFieldString: String = grvfields.toDelimitedFieldString(this)(ClickEventConverter)
  def toFieldBytes(): Array[Byte] = grvfields.toBytes(this)(ClickEventConverter)

  @transient private var hashHexOverrideOpt : Option[String] = None
  def hashHex: String = hashHexOverrideOpt.getOrElse(HashUtils.md5(clickHash))
  def setHashHexOverride(hash: String): Unit = {
    hashHexOverrideOpt = Some(hash)
  }

  def getSiteGuid: String = pubSiteGuid
  def getDate: DateTime = new DateTime(date)
  def getClickDate: DateTime = clickFields match {
    case Some(clickData) => new DateTime(clickData.dateClicked)
    case None =>
      //ClickEvent.warn("ClickEvent: Get Click Date called on event with no clickfields: " + toDisplayString())
      grvtime.epochDateTime
  }
  def getCurrentUrl: String = currentUrl
  def getArticleIds: Seq[Long] = Seq(article.key.articleId)

  // INTEREST-3596
  def isMetricableEmpty: Boolean = getArticleIds.isEmpty
  def sizeOfMetricable: Integer = getArticleIds.size
  def metricableSiteGuid: String = getSiteGuid
  def metricableEventDate: DateTime = getClickDate

  def getHashHex: String = hashHex
  def getBucketId: Int = article.recoBucket
  override def getAlgoId: Int = article.recoAlgo
  def getPlacementId: Int = article.placementId

  def getSitePlacementId: Long = article.sitePlacementId

  //var clickFields : Option[ClickFields] = None
  //def setClickFields(fields: ClickFields) { clickFields = Some(fields) }
  //def clearClickFields() { clickFields = None }
  def getClickFields: Option[ClickFields] = clickFields
  def getGeoLocationId: Int = countryCodeId
  def getChosenUserFeedbackOption = if (UserFeedbackOption.contains(chosenUserFeedbackOption)) Option(UserFeedbackOption(chosenUserFeedbackOption)) else None


  def sg: SiteGuid = pubSiteGuid.asSiteGuid
  def spId: SitePlacementId = getSitePlacementId.asSitePlacementId
  def ug: UserGuid = userGuid.asUserGuid
//  def domain: Domain = SplitHost.domain(url).getOrElse(Domain.empty)
  def domain: Domain = URLUtils.extractTopLevelDomain(url.raw).map(_.asDomain).toOption.getOrElse(Domain.empty)
  def url: Url = currentUrl.asUrl

  def ppid: PartnerPlacementId = partnerPlacementId.asPartnerPlacementId

  def pubId: PubId = PubId.apply(sg, domain, ppid)

  def domainForTrackingParams() : String = {
    SplitHost.fullHostFromUrl(getCurrentUrl) match {
      case Some(host) => host.domain + "." + host.tld
      case None => emptyString
    }
  }

  def toDisplayString : String = {
    val sb = new StringBuilder()
    sb.append("ClickEvent date: ").append(date)
      .append(" pubSiteGuid: ").append(pubSiteGuid)
      .append(" advertiserSiteGuid: ").append(advertiserSiteGuid)
      .append(" userGuid: ").append(userGuid)
      .append(" currentUrl: ").append(currentUrl)
      .append(" countryCodeId: ").append(countryCodeId)
      .append(" geoLocationDesc: ").append(geoLocationDesc)
      .append(" impressionHash: ").append(impressionHash)
      .append(" ipAddress: ").append(ipAddress)
      .append(" gravityHost: ").append(gravityHost)
      .append(" isMaintenance: ").append(isMaintenance)
      .append(" currentSectionPath: ").append(currentSectionPath)
      .append(" recommendationScope: ").append(recommendationScope)
      .append(" desiredSectionPaths: ").append(desiredSectionPaths)
      .append(" affiliateId: ").append(affiliateId)
      .append(" partnerPlacementId: ").append(partnerPlacementId)
      .append(" sitePlacementId: ").append(sitePlacementId)
      .append(" chosenUserFeedbackOption: ").append(getChosenUserFeedbackOption)
      .append(" toInterstitial: ").append(toInterstitial)
      .append(" interstitialUrl: ").append(interstitialUrl)
      .append(article.toDisplayString)
    clickFields.map(fields => sb.append(fields.toDisplayString))
    sb.toString()
  }

  def toGrcc3 : String = {
    val asString = toDelimitedFieldString
    val compStr = ArchiveUtils.compressString(asString, withHeader = false)
    ClickEvent.toGrcc3Counter.increment
    "grcc3=" + compStr
  }

  def macroedClickEvent(newIpAddress: String, newGravityHost: String, newIsMaintenence: Boolean, newClickFields: ClickFields, replaceUrlMacrosFn: (ClickEvent, String) => String): ClickEvent = {
    this.copy(ipAddress = newIpAddress, gravityHost = newGravityHost, isMaintenance = newIsMaintenence, article = article.copy(rawUrl = replaceUrlMacrosFn(this, article.rawUrl)), clickFields = Some(newClickFields) )
  }
}

object ClickEvent {
  import com.gravity.logging.Logging._
  import FieldConverters._
  import Counters._

  def apply(
             date: Long, pubSiteGuid: String, advertiserSiteGuid: String, userGuid: String,
             currentUrl: String, article : ArticleRecoData, countryCodeId : Int, geoLocationDesc : String,
             impressionHash: String, ipAddress: String, gravityHost: String, isMaintenance: Boolean,
             currentSectionPath: SectionPath, recommendationScope: String, desiredSectionPaths: SectionPath,
             affiliateId: String, partnerPlacementId: String, sitePlacementId: Long,
             chosenUserFeedbackOptionType: Option[UserFeedbackOption.Type], toInterstitial: Boolean,
             interstitialUrl: String, clickFields: Option[ClickFields]
           ) = new ClickEvent(
    date,
    pubSiteGuid,
    advertiserSiteGuid,
    userGuid,
    currentUrl,
    article,
    countryCodeId,
    geoLocationDesc,
    impressionHash,
    ipAddress,
    gravityHost,
    isMaintenance,
    currentSectionPath,
    recommendationScope,
    desiredSectionPaths,
    affiliateId,
    partnerPlacementId,
    sitePlacementId,
    chosenUserFeedbackOptionType.fold(-1)(_.id),
    toInterstitial,
    interstitialUrl,
    clickFields
  )

  val grcc3Re: Regex = new util.matching.Regex( """grcc3=([^&|"\^]+)""", "value")

  implicit val clickEventHasDateTime: HasDateTime[ClickEvent] with Object {def getDateTime(t: ClickEvent): DateTime} = new HasDateTime[ClickEvent] {
    def getDateTime(t: ClickEvent): DateTime = t.getDate
  }

  implicit val ceHasSiteGuid: ValueClassesForDomain.HasSiteGuid[ClickEvent] with Object {def getSiteGuid(e: ClickEvent): SiteGuid} = new HasSiteGuid[ClickEvent] {
    def getSiteGuid(e: ClickEvent): SiteGuid = e.sg
  }

  implicit val ceHasSitePlacementId: ValueClassesForDomain.HasSitePlacementId[ClickEvent] with Object {def getSitePlacementId(e: ClickEvent): SitePlacementId} = new HasSitePlacementId[ClickEvent] {
    def getSitePlacementId(e: ClickEvent): SitePlacementId = e.spId
  }

  val empty: ClickEvent = ClickEvent(System.currentTimeMillis(), "", "", "", "", ArticleRecoData.empty, -1, "", "", "", "", isMaintenance = true,
    SectionPath.empty, "", SectionPath.empty, "", "", 0L, chosenUserFeedbackOptionType = None, toInterstitial = false, "", None)

  private val toGrcc3Counter = getOrMakePerSecondCounter("Event_ClickEvent", "to grcc3", shouldLog = false)

  def fromGrcc3(grcc3: String,
                grcc3IsRaw: Boolean = false,
                clickFieldsOpt : Option[ClickFields],
                failureHandler: (FailureResult) => Unit = noOpFailureHandler): ValidationNel[FailureResult, ClickEvent] = {

    val extracted = {
      if (grcc3IsRaw) {
        val denamed = tokenize(grcc3, "=", 2).lift(1) match {
          case Some(s) => s
          case None =>
            val msg = "grcc3 " + grcc3 + " was not parsable! It is not in the required form of: grcc3=grcc3Value!"
            val fr = FailureResult(msg)
            failureHandler(fr)
            warn(msg)
            return fr.failureNel

        }
        denamed
      }
      else {
        grcc3
      }
    }

    val decompressed = ArchiveUtils.decompressString(extracted, withHeader = false)

    grvfields.getInstanceFromString[ClickEvent](decompressed)(ClickEventConverter)

  }

  def makeClick(pubSiteGuid: String, advertiserSiteGuid: String, userGuid: String, target: String, currentUrl: String, campaign:String, sponseeGuid: String, sponsorGuid: String, sponsorPoolGuid: String, cpc: Long, placementId: Int, sitePlacementId: Long) : ClickEvent = {
    ClickEvent.empty.copy(
      date = 0l,
      pubSiteGuid = pubSiteGuid,
      advertiserSiteGuid = advertiserSiteGuid,
      userGuid = userGuid,
      currentUrl = currentUrl,
      article = ArticleRecoData(
        ArticleKey(URLUtils.normalizeUrl(target)), target, new DateTime(), placementId, sitePlacementId, "", 0, 0, 0, 0,
        0, sponseeGuid, sponsorGuid, sponsorPoolGuid, "", campaign, cpc, 1, 0, -1, "", List.empty[AlgoSettingsData], 0, ""
      ),
      countryCodeId = 0,
      geoLocationDesc = "static fb",
      ipAddress = "ipAddress",
      gravityHost = "hostname",
      isMaintenance = false
    )
  }

  def checkWhys(whys : List[String]): List[String] = {
    whys.map(why => {
      checkWhyFor(FIELD_DELIM)(checkWhyFor(LIST_DELIM)(checkWhyFor(GRCC_DELIM)(why)))
    })
  }

  def checkWhyFor(badString: String)(why: String): String = {
    if(why.contains(badString)) {
      warn("Why values for recommendations cannot contain " + badString)
      why.replace(badString, "")
    }
    else
      why
  }
}

case class ClickFields(dateClicked: Long, url: String, userAgent: String, referrer: String, rawUrl: String, conversionClickHash: Option[String], trackingParamId: Option[String]) {
  def toDisplayString : String = {
    val sb = new StringBuilder()
    sb.append(" ClickFields  dateClicked: ").append(dateClicked).append(" url: ").append(url).append( "referrer: ").append(referrer).append(" rawUrl: ").append(rawUrl)
    sb.append(" conversionClickHash: ").append(conversionClickHash).append(" trackingParamId: ").append(trackingParamId)
    sb.toString()
  }
}

object ClickFields {
  val empty: ClickFields = ClickFields(0l, "", "", "", "", None, None)
}

