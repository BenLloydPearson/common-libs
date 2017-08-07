package com.gravity.interests.jobs.intelligence.operations

import com.gravity.domain.FieldConverters._
import com.gravity.interests.interfaces.userfeedback.{UserFeedbackPresentation, UserFeedbackVariation}
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.GrccableEvent._
import com.gravity.interests.jobs.intelligence.operations.ImpressionEvent.TrackingParamsMacroEvalFn
import com.gravity.interests.jobs.intelligence.operations.recommendations.model.{LoggedImpressionEvents, LoggedRecoImpressionEvent}
import com.gravity.interests.jobs.intelligence.ui.RedirectUtils
import com.gravity.logging.Logstashable
import com.gravity.utilities._
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.grvstrings._
import com.gravity.valueclasses.PubId
import com.gravity.valueclasses.ValueClassesForDomain._
import com.gravity.valueclasses.ValueClassesForUtilities._
import org.joda.time.DateTime

import scala.collection._


/**
 * @param authoritativeClickUrl The click URL that should generally be used everywhere; may or may not use forwarding
 *                              server depending on site, campaign, article, etc. configuration.
 * @param urlWithGrcc           Simple article URL with GRCC decoration.
 */
case class ClickUrls(authoritativeClickUrl: String, urlWithGrcc: String)

object EventExtraFields {
  def apply(
             renderType: String,
             sitePlacementId: Long,
             partnerPlacementId: String,
             recoBucket: Int,
             referrer: String,
             contextPath: String,
             userFeedbackVariation: Int,
             referrerImpressionHash: String,
             impressionPurposeType: ImpressionPurpose.Type,
             userFeedbackPresentation: Int,
             pageViewGuid: String,
             logMethod: LogMethod.Type
           ) =
    new EventExtraFields(
      renderType = renderType,
      sitePlacementId = sitePlacementId,
      partnerPlacementId = partnerPlacementId,
      recoBucket = recoBucket,
      referrer = referrer,
      contextPath = contextPath,
      userFeedbackVariation = userFeedbackVariation,
      referrerImpressionHash = referrerImpressionHash,
      impressionPurpose = impressionPurposeType.id,
      userFeedbackPresentation = userFeedbackPresentation,
      pageViewGuid = pageViewGuid,
      logMethod.id
    )

  val empty: EventExtraFields = EventExtraFields("", -1L, "", -1, "", "", UserFeedbackVariation.none.id, "",
    ImpressionPurpose.defaultValue.id, UserFeedbackPresentation.none.id, "", LogMethod.unknown.id)
}

/**
  * @param userFeedbackVariation Joins to [[com.gravity.interests.interfaces.userfeedback.UserFeedbackVariation]].
  * @param referrerImpressionHash In the instance this impression is the direct result of another placement's impression
  *                               (e.g. as in the case of a placement linking to an interstitial page that has this
  *                               placement on that page), the referrerImpressionHash will equal the first placement's
  *                               impression hash.
  */
@SerialVersionUID(7l)
case class EventExtraFields(renderType: String,
                            sitePlacementId: Long,
                            partnerPlacementId: String,
                            recoBucket: Int,
                            referrer: String,
                            contextPath: String,
                            userFeedbackVariation: Int,
                            referrerImpressionHash: String,
                            impressionPurpose: Int,
                            userFeedbackPresentation: Int,
                            pageViewGuid: String,
                            logMethod: Int) {

  def getImpressionPurpose = ImpressionPurpose(impressionPurpose)

}

case class NonMagicPlacementWarning(siteGuid: String, userGuid: String, message: String) extends Logstashable {
  import com.gravity.logging.Logstashable._
  override def getKVs: Seq[(String, String)] = {
    Seq(SiteGuid -> siteGuid, UserGuid -> userGuid, Message -> message)
  }
}

trait HasImpressionEvent {
  def impressionEvent: ImpressionEvent
  def loggedImpressionEvents: LoggedImpressionEvents = LoggedRecoImpressionEvent(impressionEvent)
}

@SerialVersionUID(3l)
case class ImpressionEvent(date: Long,
                           siteGuid: String,
                           userGuid: String,
                           currentUrl: String,
                           articlesInReco : Seq[ArticleRecoData],
                           countryCodeId: Int,
                           geoLocationDesc: String,
                           isMobile: Boolean,
                           pageViewIdWidgetLoaderWindowUrl: String,
                           pageViewIdTime: Long,
                           pageViewIdRand: Long,
                           userAgent: String,
                           clientTime: Long,
                           ipAddress: String,
                           gravityHost: String,
                           isMaintenance: Boolean,
                           currentSectionPath: SectionPath,
                           recommendationScope: String,
                           desiredSectionPaths: SectionPath,
                           affiliateId: String,
                           isOptedOut: Boolean,
                           more: EventExtraFields
                            ) extends MetricsEvent with DiscardableEvent {
  import com.gravity.logging.Logging._

  protected lazy val impressionHash : String = siteGuid + userGuid + getDate + currentUrl + clientTime + ipAddress + getSitePlacementId + articlesInReco.map(_.hash).mkString(LIST_DELIM)
  lazy val hashHex: String = HashUtils.md5(impressionHash)
  private var hashHexOverrideOpt : Option[String] = None //not to be confused with crash override
  def getHashHex: String = hashHexOverrideOpt.getOrElse(hashHex)
  lazy val pageViewIdHash: String = new PageViewId(pageViewIdWidgetLoaderWindowUrl, getPageViewIdTime.getMillis, pageViewIdRand).hash
  lazy val pubId: PubId = PubId(sg, url.domainOrEmpty, ppid)

  def setHashHexOverride(hash: String): Unit = {
    hashHexOverrideOpt = Some(hash)
  }

  def sg: SiteGuid = siteGuid.asSiteGuid
  def url: Url = currentUrl.asUrl
  def ppid: PartnerPlacementId = more.partnerPlacementId.asPartnerPlacementId
  def setClickFields(fields: ClickFields) { }
  def getClickFields : Option[ClickFields] = None
  def getArticleIds: Seq[Long] = articlesInReco.map(_.key.articleId)

  // Is domain the best label for this? Returns "google", not "google.com"
  def domain: Option[Domain] = (for {
    url <- currentUrl.noneForEmpty
    host <- SplitHost.fullHostFromUrl(url)
  } yield host.domain).map(_.asDomain)

  //Returns google.com
  def domainWithTld: Option[Domain] = (for {
    url <- currentUrl.noneForEmpty
    host <- SplitHost.fullHostFromUrl(url)
  } yield host.registeredDomain).map(_.asDomain)

  def impressionViewedHash: ImpressionViewedHash = ImpressionViewedHash(sg, userGuid.noneForEmpty.map(_.asUserGuid), getHashHex.asImpressionViewedHashString)

  def isMetricableEmpty: Boolean = getArticleIds.isEmpty
  def sizeOfMetricable: Integer = getArticleIds.size
  def metricableSiteGuid: String = getSiteGuid
  def metricableEventDate: DateTime = getDate
  def getCurrentUrl: String = currentUrl
  def articleHead: ArticleRecoData = articlesInReco.headOption.getOrElse(ArticleRecoData.empty)
  def getBucketId: Int = articleHead.recoBucket
  def getGeoLocationId: Int = countryCodeId
 // override def getAlgoId: Int = articleHead.recoAlgo
  def toDisplayString : String = {
    val sb = new StringBuilder()
    sb.append("ImpressionEvent  Date: ").append(getDate).append(" SiteGuid: ").append(siteGuid).append(" UserGuid: ").append(userGuid)
    sb.append(" currentUrl: ").append(currentUrl).append(" countryCodeId: ").append(countryCodeId).append(" geoLocationDesc: ").append(geoLocationDesc).append(" isMobile: ").append(isMobile)
    sb.append(" pageViewIdWidgetLoaderWinderUrl: ").append(pageViewIdWidgetLoaderWindowUrl).append(" pageViewIdTime: ").append(getPageViewIdTime).append(" userAgent: ").append(userAgent).append(" clientTime: ").append(new DateTime(clientTime))
    sb.append(" currentSectionPath: ").append(currentSectionPath).append(" recommendationScope: ").append(recommendationScope).append(" desiredSectionPaths: ").append(desiredSectionPaths)
    sb.append(" affiliateId: ").append(affiliateId).append(" isOptedOut: ").append(isOptedOut).append(" renderType ")
      .append(more.renderType).append(" sitePlacementId: ").append(more.sitePlacementId).append(" partnerPlacementId: ")
      .append(more.partnerPlacementId).append(" recoBucket: ").append(more.recoBucket).append(" referrer: ")
      .append(more.referrer).append(" contextPath: ").append(more.contextPath)
      .append(" userFeedbackVariation: ").append(more.userFeedbackVariation)
      .append(" referrerImpressionHash: ").append(more.referrerImpressionHash)
      .append(" userFeedbackPresentation: ").append(more.userFeedbackPresentation)
    sb.append(" articles: ")
    articlesInReco.foreach(article => sb.append(article.toDisplayString))
    sb.toString()
  }

  def toDelimitedFieldString(): String = grvfields.toDelimitedFieldString(this)(ImpressionEventConverter)
  def toFieldBytes(): Array[Byte] = grvfields.toBytes(this)(ImpressionEventConverter)

  def isUserFeedbackEvent: Boolean = more.userFeedbackVariation != UserFeedbackVariation.none.id

  def getPlacementId: Int = { //get first non-icky placementId. If there are multiple non-magic
    val placementIds = articlesInReco.map(_.placementId).toSet
    if(placementIds.size > 1) {
      warn(NonMagicPlacementWarning(siteGuid, userGuid, "Event " + this + " had multiple non-magic placements: " + placementIds.mkString(",")))
    }
    placementIds.headOption.getOrElse(-1)
  }

  def getSitePlacementId: Long = more.sitePlacementId

  def getDate: DateTime = new DateTime(date)
  def getSiteGuid: String = siteGuid
  def getPageViewIdTime: DateTime = new DateTime(pageViewIdTime)

  def articleClickEvent(article: ArticleRecoData): ClickEvent = ClickEvent.empty.copy(
      date = date, pubSiteGuid = siteGuid, advertiserSiteGuid = article.sponsorGuid, userGuid = userGuid,
      currentUrl = currentUrl, article = article, countryCodeId = countryCodeId, geoLocationDesc = geoLocationDesc,
      impressionHash = getHashHex, ipAddress = ipAddress, gravityHost = gravityHost,
      isMaintenance = Settings2.isInMaintenanceMode, currentSectionPath = currentSectionPath,
      recommendationScope = recommendationScope, desiredSectionPaths = desiredSectionPaths, affiliateId = affiliateId,
      partnerPlacementId = more.partnerPlacementId
  )

  def clickUrl(articleIndex: Int, use302: Boolean, trackingParamsMacroEvalFn: TrackingParamsMacroEvalFn,
               requiresOutboundTrackingParams: Boolean, addExchangeOmnitureParams: Boolean, https: Boolean = false): ClickUrls = {
    val article = articlesInReco(articleIndex)

    val event = articleClickEvent(article)

    val clickUrl = article.rawUrl
    val grcc = event.toGrcc3
    val displayUrlWithGrcc = URLUtils.replaceParameter(clickUrl, grcc)
    val isSponsored = CampaignType.isSponsoredType(CampaignType(article.campaignType.toByte))

    val useForwardingServerUrl = isSponsored || use302
    val authoritativeClickUrl = {
      if (useForwardingServerUrl)
        RedirectUtils.redirectUrlForUrlWithGrcc3Param(displayUrlWithGrcc, event, currentUrl, more.contextPath, https,
          trackingParamsMacroEvalFn, requiresOutboundTrackingParams, addExchangeOmnitureParams).getOrElse(displayUrlWithGrcc)
      else
        displayUrlWithGrcc
    }

    ClickUrls(authoritativeClickUrl, displayUrlWithGrcc)
  }

  case class ImpressionEventKey(siteGuid: String, userGuid: String, currentUrl: String, geoLocationId : Int) {
    override def equals(otherA: Any) : Boolean = {
      otherA.isInstanceOf[ImpressionEventKey] && {
        val other = otherA.asInstanceOf[ImpressionEventKey]
        siteGuid == other.siteGuid && userGuid == other.userGuid && currentUrl == other.currentUrl && geoLocationId == other.geoLocationId
      }
    }
  }

  lazy val key: ImpressionEventKey = ImpressionEventKey(siteGuid, userGuid, currentUrl: String, countryCodeId)

  def mergeWith(other: ImpressionEvent) : Option[ImpressionEvent] = {
    if(
      key == other.key
    ) {
      Some(ImpressionEvent(getDate, siteGuid, userGuid, currentUrl,
        articlesInReco ++ other.articlesInReco, countryCodeId, geoLocationDesc, isMobile, pageViewIdWidgetLoaderWindowUrl, getPageViewIdTime, pageViewIdRand,
        userAgent, clientTime, ipAddress, gravityHost, isMaintenance, currentSectionPath, recommendationScope, desiredSectionPaths, affiliateId, isOptedOut, more))
    }
    else {
      warn("Attempt to merge non-matching events: " + this + " and " + other)
      None
    }
  }
}

object ImpressionEvent {
 import com.gravity.logging.Logging._

  type TrackingParamsMacroEvalFn = (ClickEvent, String) => String

  //private val toGrcc2 = new Counter("to grcc2", "Event_ImpressionEvent", true, CounterType.PER_SECOND)
  def apply (
              dateTime: DateTime,
              siteGuid: String,
              userGuid: String,
              currentUrl: String,
              articlesInReco : Seq[ArticleRecoData],
              countryCodeId: Int,
              geoLocationDesc: String,
              isMobile: Boolean,
              pageViewIdWidgetLoaderWindowUrl: String,
              pageViewIdDateTime: DateTime,
              pageViewIdRand: Long,
              userAgent: String,
              clientTime: Long,
              ipAddress: String,
              gravityHost: String,
              isMaintenance: Boolean,
              currentSectionPath: SectionPath,
              recommendationScope: String,
              desiredSectionPaths: SectionPath,
              affiliateId: String,
              isOptedOut: Boolean,
              more: EventExtraFields
            ) =
    new ImpressionEvent (
      date = dateTime.getMillis,
      siteGuid = siteGuid,
      userGuid = userGuid,
      currentUrl = currentUrl,
      articlesInReco = articlesInReco,
      countryCodeId = countryCodeId,
      geoLocationDesc = geoLocationDesc,
      isMobile = isMobile,
      pageViewIdWidgetLoaderWindowUrl = pageViewIdWidgetLoaderWindowUrl,
      pageViewIdTime = pageViewIdDateTime.getMillis,
      pageViewIdRand = pageViewIdRand,
      userAgent = userAgent,
      clientTime = clientTime,
      ipAddress = ipAddress,
      gravityHost = gravityHost,
      isMaintenance = isMaintenance,
      currentSectionPath = currentSectionPath,
      recommendationScope = recommendationScope,
      desiredSectionPaths = desiredSectionPaths,
      affiliateId = affiliateId,
      isOptedOut = isOptedOut,
      more = more
    )

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

  def merge(event1: ImpressionEvent, event2: ImpressionEvent): ImpressionEvent = {
    event1.mergeWith(event2).getOrElse(event1)
  }

  def mergeList(events: Seq[ImpressionEvent]): Seq[ImpressionEvent] = {
    events.groupBy(_.key).values.map(list => list.reduce(merge)).toSeq.sortBy(_.userGuid)
  }

  val empty: ImpressionEvent = ImpressionEvent(grvtime.epochDateTime, "", "", "", List.empty[ArticleRecoData], -1, "", isMobile = false,
                              "", grvtime.epochDateTime, -1L, "", -1L, "", "", isMaintenance = false, SectionPath.empty, "",
                              SectionPath.empty, "", isOptedOut = false, EventExtraFields.empty)
}
