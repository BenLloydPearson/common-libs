package com.gravity.api.livetraffic

import com.gravity.domain.Ipv4Address
import com.gravity.interests.jobs.intelligence.operations.{ClickEvent, ImpressionViewedEvent}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvtime.HasDateTime
import com.gravity.valueclasses.{ValueClassesForDomain, RenderTypes}
import com.gravity.valueclasses.ValueClassesForDomain._
import com.gravity.valueclasses.ValueClassesForUtilities._
import org.joda.time.DateTime
import play.api.libs.json.{Format, Json}

import scalaz.syntax.std.option._

/**
 * Created by runger on 4/14/15.
 */

case class LTClick(date: DateTime, siteGuid: String, userGuid: String, currentUrl: String, geoLocationDesc : String, impressionHash: String,
                   ipAddress: String, gravityHost: String, partnerPlacementId: String, userAgent: String)
object LTClick {
  implicit val cefsFmt: Format[LTClick] = Json.format[LTClick]
}


@SerialVersionUID(1L)
case class ClickEventWrapper(clickEvent: ClickEvent) {
  def forSer: LTClick = LTClick(clickEvent.getDate, clickEvent.pubSiteGuid, clickEvent.userGuid, clickEvent.currentUrl, clickEvent.geoLocationDesc,
    clickEvent.impressionHash, clickEvent.ipAddress, clickEvent.gravityHost, clickEvent.partnerPlacementId, clickEvent.clickFields.map(_.userAgent).getOrElse("None"))
}

object ClickEventWrapper {

  import com.gravity.domain.FieldConverters.ClickEventConverter

  implicit object ClickEventWrapperConverter extends FieldConverter[ClickEventWrapper] {
    val fields: FieldRegistry[ClickEventWrapper] = new FieldRegistry[ClickEventWrapper]("ClickEventWrapper")
      .registerField[ClickEvent]("clickEvent", 0, ClickEvent.empty)

    override def toValueRegistry(o: ClickEventWrapper): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.clickEvent)

    override def fromValueRegistry(reg: FieldValueRegistry): ClickEventWrapper = ClickEventWrapper(reg.getValue[ClickEvent](0))
  }

  implicit val cewdt: HasDateTime[ClickEventWrapper] with Object {def getDateTime(e: ClickEventWrapper): DateTime} = new HasDateTime[ClickEventWrapper] {
    def getDateTime(e: ClickEventWrapper): DateTime = implicitly[HasDateTime[ClickEvent]].getDateTime(e.clickEvent)
  }

  implicit val cewsg: ValueClassesForDomain.HasSiteGuid[ClickEventWrapper] with Object {def getSiteGuid(e: ClickEventWrapper): SiteGuid} = new HasSiteGuid[ClickEventWrapper] {
    def getSiteGuid(e: ClickEventWrapper): SiteGuid = implicitly[HasSiteGuid[ClickEvent]].getSiteGuid(e.clickEvent)
  }

  implicit val cewsp: ValueClassesForDomain.HasSitePlacementId[ClickEventWrapper] with Object {def getSitePlacementId(e: ClickEventWrapper): SitePlacementId} = new HasSitePlacementId[ClickEventWrapper] {
    def getSitePlacementId(e: ClickEventWrapper): SitePlacementId = implicitly[HasSitePlacementId[ClickEvent]].getSitePlacementId(e.clickEvent)
  }
}

@SerialVersionUID(1L)
case class ImpressionViewedEventWrapper(ive: ImpressionViewedEvent){

  val s: String = ive.ordinalArticleKeys.map(b => s"${b.ordinal}: ${b.ak.articleId}").reduceLeftOption { (a, b) =>
    a + ", " + b
  }.getOrElse("None")

  def forSer: LTIve = LTIve(new DateTime(ive.date), ive.siteGuid.asSiteGuid, ive.userGuid.asUserGuid, ive.sitePlacementId.asSitePlacementId, ive.userAgent.asUserAgent,
    ive.remoteIp.asIPAddressString, s)
}

object ImpressionViewedEventWrapper {

  import com.gravity.domain.FieldConverters.ImpressionViewedEventConverter

  implicit object ImpressionViewedEventWrapperConverter extends FieldConverter[ImpressionViewedEventWrapper] {
    val fields: FieldRegistry[ImpressionViewedEventWrapper] = new FieldRegistry[ImpressionViewedEventWrapper]("ImpressionViewedEventWrapper")
      .registerField[ImpressionViewedEvent]("ive", 0, ImpressionViewedEvent.empty)

    override def toValueRegistry(o: ImpressionViewedEventWrapper): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.ive)

    override def fromValueRegistry(reg: FieldValueRegistry): ImpressionViewedEventWrapper = ImpressionViewedEventWrapper(reg.getValue[ImpressionViewedEvent](0))
  }

  implicit val cewdt: HasDateTime[ImpressionViewedEventWrapper] with Object {def getDateTime(e: ImpressionViewedEventWrapper): DateTime} = new HasDateTime[ImpressionViewedEventWrapper] {
    def getDateTime(e: ImpressionViewedEventWrapper): DateTime = implicitly[HasDateTime[ImpressionViewedEvent]].getDateTime(e.ive)
  }

  implicit val cewsg: ValueClassesForDomain.HasSiteGuid[ImpressionViewedEventWrapper] with Object {def getSiteGuid(e: ImpressionViewedEventWrapper): SiteGuid} = new HasSiteGuid[ImpressionViewedEventWrapper] {
    def getSiteGuid(e: ImpressionViewedEventWrapper): SiteGuid = implicitly[HasSiteGuid[ImpressionViewedEvent]].getSiteGuid(e.ive)
  }

  implicit val cewsp: ValueClassesForDomain.HasSitePlacementId[ImpressionViewedEventWrapper] with Object {def getSitePlacementId(e: ImpressionViewedEventWrapper): SitePlacementId} = new HasSitePlacementId[ImpressionViewedEventWrapper] {
    def getSitePlacementId(e: ImpressionViewedEventWrapper): SitePlacementId = implicitly[HasSitePlacementId[ImpressionViewedEvent]].getSitePlacementId(e.ive)
  }

  lazy val empty = ImpressionViewedEventWrapper(ImpressionViewedEvent.empty)
}

case class LTIve(date: DateTime, siteGuid: SiteGuid, userGuid: UserGuid, spid: SitePlacementId, ua: UserAgent, ip: IPAddressString, oaKeys: String)
object LTIve {
  implicit val ivewFmt: Format[LTIve] = Json.format[LTIve]
}

@SerialVersionUID(1L)
case class ApiRequestEvent(
                            endpoint: Label
                            , siteGuid: SiteGuid
                            , sitePlacementId: SitePlacementId
                            , renderType: RenderType
                            , userGuid: UserGuid
                            , clientTime: Millis
                            , requestReceivedTime: Millis
                            , queryString: QueryString
                            , hostname: HostName
                            , ipAddress: Ipv4Address
                            , userAgent: UserAgent
                            , maxArticles: OutputLimit
                            , pageIndex: Index
                            , isUserOptedOut: BinaryState
                            , logResult: FeatureToggle
                            , partnerPlacementId: PartnerPlacementId
                            , currentUrl: Option[Url]
                            , imgSize: Option[Size]
                            ) {
  val requestReceivedTimeDateTime: DateTime = new DateTime(requestReceivedTime.raw)
}

//, siteGuid: SiteGuid
//, sitePlacementId: SitePlacementId
//, userGuid: UserGuid

object ApiRequestEvent {

  import Ipv4Address.Ipv4AddressFieldConverter

  val empty: ApiRequestEvent = ApiRequestEvent("".asLabel, "".asSiteGuid, 1234l.asSitePlacementId, RenderTypes.api, "".asUserGuid, 0l.asMillis, 0l.asMillis, "".asQueryString, "".asHostName, Ipv4Address.localhost,
    "".asUserAgent, 1.asOutputLimit, 0.asIndex, BinaryState.no, FeatureToggle.disabled, "".asPartnerPlacementId, Some("".asUrl), None)

  implicit val apirFmt: Format[ApiRequestEvent] = Json.format[ApiRequestEvent]

  implicit object ApiRequestEventHasDateTime extends HasDateTime[ApiRequestEvent] {
    override def getDateTime(t: ApiRequestEvent): DateTime = new DateTime(t.requestReceivedTime.raw)
  }

  implicit val apiRequestEventHasSiteGuid: ValueClassesForDomain.HasSiteGuid[ApiRequestEvent] with Object {def getSiteGuid(e: ApiRequestEvent): SiteGuid} = new HasSiteGuid[ApiRequestEvent] {
    def getSiteGuid(e: ApiRequestEvent): SiteGuid = e.siteGuid
  }

  implicit val apiRequestEventHasSitePlacement: ValueClassesForDomain.HasSitePlacementId[ApiRequestEvent] with Object {def getSitePlacementId(e: ApiRequestEvent): SitePlacementId} = new HasSitePlacementId[ApiRequestEvent] {
    def getSitePlacementId(e: ApiRequestEvent): SitePlacementId = e.sitePlacementId
  }

  implicit object ApiRequestEventConverter extends FieldConverter[ApiRequestEvent] {
    val fields: FieldRegistry[ApiRequestEvent] = new FieldRegistry[ApiRequestEvent]("ApiRequestEvent")
      .registerField[Label]("endpoint", 0, Label("unknown"))
      .registerField[SiteGuid]("siteGuid", 1, SiteGuid("no site guid"))
      .registerField[SitePlacementId]("sitePlacementId", 2, SitePlacementId(-1))
      .registerField[RenderType]("renderType", 3, RenderType("unknown"))
      .registerField[UserGuid]("userGuid", 4, UserGuid(""))
      .registerField[Millis]("clientTime", 5, Millis(-1))
      .registerField[Millis]("requestReceivedTime", 6, Millis(-1))
      .registerField[QueryString]("queryString", 7, QueryString("no query string"))
      .registerField[HostName]("hostname", 8, HostName("unknown hostname"))
      .registerField[Ipv4Address]("ipAddress", 9, Ipv4Address.localhost)
      .registerField[UserAgent]("userAgent", 10, UserAgent("unknown ua"))
      .registerField[OutputLimit]("maxArticles", 11, OutputLimit(-1))
      .registerField[Index]("pageIndex", 12, Index(-1))
      .registerField[BinaryState]("isUserOptedOut", 13, BinaryState.yes)
      .registerField[FeatureToggle]("logResult", 14, FeatureToggle.disabled)
      .registerField[PartnerPlacementId]("partnerPlacementId", 15, PartnerPlacementId("unknown ppid"))
      .registerField[Url]("currentUrl", 16, Url("no url"))
      .registerField[Size]("imgSize", 17, Size(-1))

    def toValueRegistry(o: ApiRequestEvent): FieldValueRegistry = {
      new FieldValueRegistry(fields)
        .registerFieldValue(0, o.endpoint)
        .registerFieldValue(1, o.siteGuid)
        .registerFieldValue(2, o.sitePlacementId)
        .registerFieldValue(3, o.renderType)
        .registerFieldValue(4, o.userGuid)
        .registerFieldValue(5, o.clientTime)
        .registerFieldValue(6, o.requestReceivedTime)
        .registerFieldValue(7, o.queryString)
        .registerFieldValue(8, o.hostname)
        .registerFieldValue(9, o.ipAddress)
        .registerFieldValue(10, o.userAgent)
        .registerFieldValue(11, o.maxArticles)
        .registerFieldValue(12, o.pageIndex)
        .registerFieldValue(13, o.isUserOptedOut)
        .registerFieldValue(14, o.logResult)
        .registerFieldValue(15, o.partnerPlacementId)
        .registerFieldValue(16, o.currentUrl.getOrElse(Url("no url")))
        .registerFieldValue(17, o.imgSize.getOrElse(Size(-1)))
    }

    def fromValueRegistry(vals: FieldValueRegistry): ApiRequestEvent = {
      new ApiRequestEvent(
        vals.getValue[Label](0)
        ,vals.getValue[SiteGuid](1)
        ,vals.getValue[SitePlacementId](2)
        ,vals.getValue[RenderType](3)
        ,vals.getValue[UserGuid](4)
        ,vals.getValue[Millis](5)
        ,vals.getValue[Millis](6)
        ,vals.getValue[QueryString](7)
        ,vals.getValue[HostName](8)
        ,vals.getValue[Ipv4Address](9)
        ,vals.getValue[UserAgent](10)
        ,vals.getValue[OutputLimit](11)
        ,vals.getValue[Index](12)
        ,vals.getValue[BinaryState](13)
        ,vals.getValue[FeatureToggle](14)
        ,vals.getValue[PartnerPlacementId](15)
        ,vals.getValue[Url](16).some
        ,vals.getValue[Size](17).some
      )
    }
  }
}
