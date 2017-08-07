package com.gravity.domain.rtb

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvjson._
import com.gravity.utilities.web.MimeTypes
import com.gravity.valueclasses.ValueClassesForDomain._
import com.gravity.valueclasses.ValueClassesForUtilities._
import play.api.libs.json._

/**
 * Created by runger on 4/30/15.
 */

object GrvRTBRequest

@SerialVersionUID(1l)
object RequestType extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val live: Type = Value(0, "live")
  val test: Type = Value(1, "test")

  val defaultValue: Type = live

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]
}

@SerialVersionUID(1l)
object AllImpsType extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val noOrUnknown: Type = Value(0, "No or Unknown")
  val allImps: Type = Value(1, "All Imps")

  val defaultValue: Type = noOrUnknown

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]
}

@SerialVersionUID(1l)
object AuctionType extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown = Value(0, "unknown")
  val firstPrice = Value(1, "firstPrice")
  val secondPricePlus = Value(2, "secondPricePlus")

  val defaultValue = secondPricePlus

  implicit val fmt = makeJsonFormatByIndex[Type]
}

@SerialVersionUID(1l)
object YesNoType extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val no: Type = Value(0, "no")
  val yes: Type = Value(1, "yes")

  val defaultValue: Type = no

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]
}

@SerialVersionUID(1l)
object RTBDeviceType extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val mobileOrTablet: Type = Value(1, "Mobile/Tablet")
  val pc: Type = Value(2, "Personal Computer")
  val tv: Type = Value(3, "Connected TV")
  val phone: Type = Value(4, "Phone")
  val tablet: Type = Value(5, "Tablet")
  val connectedDevice: Type = Value(6, "Connected Device")
  val setTopBox: Type = Value(7, "Set Top Box")

  val defaultValue: Type = unknown

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]
}

@SerialVersionUID(1l)
object RTBGender extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "u")
  val male: Type = Value(1, "M")
  val female: Type = Value(2, "F")
  val other: Type = Value(3, "O")

  val defaultValue: Type = unknown

  implicit val fmt: Format[Type] = makeJsonFormat[Type] //This one serializes to letters - "M/F/O"
}

@SerialVersionUID(1l)
object LocationType extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val gps: Type = Value(1, "GPS/Location Services")
  val ipAddress: Type = Value(2, "IP Address")
  val user: Type = Value(3, "User provided (e.g., registration data)")

  val defaultValue: Type = unknown

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]

}

@SerialVersionUID(1l)
object ExpDirType extends GrvEnum[Byte] {  // Expandable Direction Type

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val left: Type = Value(1, "Left")
  val right: Type = Value(2, "Right")
  val up: Type = Value(3, "Up")
  val down: Type = Value(4, "Down")
  val fullScreen: Type = Value(5, "Full Screen")

  val defaultValue: Type = unknown

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]
}

@SerialVersionUID(1l)
object APIFramework extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val vpaid1: Type = Value(1, "VPAID 1.0")
  val vpaid2: Type = Value(2, "VPAID 2.0")
  val mraid1: Type = Value(3, "MRAID-1")
  val ormma: Type = Value(4, "ORMMA")
  val mraid2: Type = Value(5, "MRAID-2")

  val defaultValue: Type = unknown

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]
}

case class SeatId(raw: String) extends AnyVal
case class CurrencyCode(raw: String) extends AnyVal
case class IABCategory(raw: String) extends AnyVal
case class PlacementId(raw: String) extends AnyVal
case class Region(raw: String) extends AnyVal
case class UTCOffset(raw: Int) extends AnyVal

case class RegsExt()

case class ContentExt()

case class ProducerExt()

case class VideoExt()

case class BidRequestExt()

case class SegmentExt()

case class DataExt()

case class ImpExt()

case class SiteExt()

case class AppExt()

case class PublisherExt()

case class DeviceExt()

case class DealExt()

case class GeoExt()

case class UserExt()

case class PMPExt()

case class BannerExt()

case class Banner(id: Option[StringId] = None   // Recommended when Banner objects are used with a Video object to represent an array of companion ads.
                  ,w: Option[Width] = None       // Recommended
                  ,h: Option[Height] = None      // Recommended
                  ,wmax: Option[Width] = None
                  ,hmax: Option[Height] = None
                  ,wmin: Option[Width] = None
                  ,hmin: Option[Height] = None
                  ,btype: Option[List[Int]] = None
                  ,battr: Option[List[CreativeAttribute.Type]] = None
                  ,pos: Option[Int] = None
                  ,mimes: Option[List[String]] = None
                  ,topframe: Option[YesNoType.Type] = None
                  ,expdir: Option[List[ExpDirType.Type]] = None
                  ,api: Option[List[APIFramework.Type]] = None
                  ,ext: Option[BannerExt] = None
                   )

case class Segment(id: Option[StringId] = None
                   ,name: Option[String] = None
                   ,value: Option[String] = None
                   ,ext: Option[SegmentExt] = None
                    )

case class Data(id: Option[StringId] = None
                ,name: Option[String] = None
                ,segment: Option[List[Segment]] = None
                ,ext: Option[DataExt] = None
                 )

case class Regs(coppa: Option[YesNoType.Type] = None
                ,ext: Option[RegsExt] = None
                 )

// Improvement: There are a lot of Option[Int] and List[Int] here that could be more typey.
case class Video(mimes: List[String]                  // Required
                 ,minduration: Option[Seconds] = None  // Recommended
                 ,maxduration: Option[Seconds] = None  // Recommended
                 ,protocol: Option[Int] = None         // DEPRECATED in favor of protocols
                 ,protocols: Option[List[Int]] = None  // Recommended
                 ,w: Option[Width] = None              // Recommended
                 ,h: Option[Height] = None             // Recommended
                 ,startdelay: Option[Seconds] = None   // Recommended
                 ,linearity: Option[Int] = None
                 ,sequence: Option[Int] = None
                 ,battr: Option[List[CreativeAttribute.Type]] = None
                 ,maxextended: Option[Int] = None
                 ,minbitrate: Option[Int] = None
                 ,maxbitrate: Option[Int] = None
                 ,boxingallowed: Option[YesNoType.Type] = Option(YesNoType.yes)
                 ,playbackmethod: Option[List[Int]] = None
                 ,delivery: Option[List[Int]] = None
                 ,pos: Option[Int] = None
                 ,companionad: Option[List[Banner]] = None
                 ,api: Option[List[APIFramework.Type]] = None
                 ,companiontype: Option[List[Int]] = None
                 //                ,ext: Option[VideoExt] = None                 // Commented out because it would take us over the 22-param limit.
                  )

case class Producer(id: Option[StringId] = None
                    ,name: Option[Name] = None
                    ,cat: Option[List[IABCategory]] = None
                    ,domain: Option[Domain] = None
                    ,ext: Option[ProducerExt] = None
                     )
case class Content(id: Option[StringId] = None
                   ,episode: Option[Int] = None
                   ,title: Option[Name] = None
                   ,series: Option[Name] = None
                   ,season: Option[String] = None
                   ,producer: Option[Producer] = None
                   ,url: Option[Url] = None
                   ,cat: Option[List[IABCategory]] = None
                   ,videoquality: Option[Int] = None
                   ,context: Option[Int] = None
                   ,contentrating: Option[String] = None // e.g. MPAA
                   ,userrating: Option[String] = None
                   ,qagmediarating: Option[Int] = None
                   ,keywords: Option[String] = None        // A single string of comma-separated keywords.
                   ,livestream: Option[YesNoType.Type] = None
                   ,sourcerelationship: Option[YesNoType.Type] = None
                   ,len: Option[Seconds] = None
                   ,language: Option[Language] = None
                   ,embeddable: Option[YesNoType.Type] = None
                   ,ext: Option[ContentExt] = None
                    )

case class DefaultNativeExt()

case class Deal(
                 id: StringId
                 ,bidfloor: Option[BigDecimal] = Option(BigDecimal(0))  // Serialized as decimal CPM in dollars.
                 ,bidfloorcur: Option[CurrencyCode] = Option(CurrencyCode("USD")) //Currency specified using ISO-4217 alpha codes. This may be different from bid currency returned by bidder if this is allowed by the exchange
                 ,at: Option[AuctionType.Type]
                 ,wseat: Option[List[SeatId]] // Whitelist of buyer seats allowed to bid on this deal
                 ,wadomain: Option[List[Domain]] // Array of advertiser domains (e.g., advertiser.com) allowed to bid on this deal
                 ,ext: Option[DealExt]
                 )

case class PMP(
                private_auction: Option[YesNoType.Type]
                ,deals: Option[List[Deal]]
                ,ext: Option[PMPExt]
                )

case class User(
                 id: Option[StringId] = None // Recommended;
                 ,buyeruid: Option[StringId] = None // Recommended; Buyer-specific ID for the user as mapped by the exchange for the buyer. Atleast one of buyerid or id is recommended.
                 ,yob: Option[Year] = None // Year of birth as a 4-digit integer.
                 ,gender: Option[RTBGender.Type] = None  //Serialized as M, F, O
                 ,keywords: Option[List[KeyWord]] = None //Serialized as comma-separated
                 ,customdata: Option[DataString] = None // Optional feature to pass bidder data that was set in the exchange’s cookie. The string must be in base85 cookie safe characters and be in any format. Proper JSON encoding must be used to include “escaped” quotation marks.
                 ,geo: Option[Geo] = None
                 ,data: Option[List[Data]] = None
                 ,ext: Option[UserExt] = None
                 )

case class Geo(
                lat: Option[Latitude] = None
                ,lon: Option[Longitude] = None
                ,`type`: Option[LocationType.Type] = Some(LocationType.ipAddress)
                ,country: Option[CountryCode3] = None
                ,region: Option[Region] = None
                ,regionfips104: Option[Region] = None
                ,metro: Option[Region] = None
                ,city: Option[Region] = None
                ,zip: Option[Region] = None
                ,utcoffset: Option[UTCOffset] = None
                ,ext: Option[GeoExt] = None
                )

/*
BEST PRACTICE: Proper device IP detection in mobile is not straightforward. Typically it involves starting at the left of the x-forwarded-for header, skipping private carrier networks (e.g., 10.x.x.x or 192.x.x.x), and possibly scanning for known carrier IP ranges. Exchanges are urged to research and implement this feature carefully when presenting device IP values to bidders.
 */
case class Device(
                   ua: Option[UserAgent] = None// Browser user agent string
                   ,geo: Option[Geo] = None // Location of the device assumed to be the user’s current location defined by a Geo object (Section 3.2.12).
                   ,dnt: Option[YesNoType.Type] = None // Standard “Do Not Track” flag as set in the header by the browser, where 0 = tracking is unrestricted, 1 = do not track
                   ,lmt: Option[YesNoType.Type] = None //“Limit Ad Tracking” signal commercially endorsed (e.g., iOS, Android), where 0 = tracking is unrestricted, 1 = tracking must be limited per commercial guidelines.
                   ,ip: Option[IPAddressString] = None //Serialized as dotted quads string
                   //                 ,ipv6: Option[IPAddressString] = None //  IP address closest to device as IPv6.
                   ,devicetype: Option[RTBDeviceType.Type] = None // The general type of device. Refer to List 5.17.
                   ,make: Option[Make] = None // Device make (e.g., “Apple”).
                   ,model: Option[Model] = None // Device model (e.g., “iPhone”).
                   ,os: Option[OS] = None // ￼ Device operating system (e.g., “iOS”).
                   ,osv: Option[VersionString] = None // Device operating system version (e.g., “3.1.2”).
                   ,hwv: Option[VersionString] = None // Hardware version of the device (e.g., “5S” for iPhone 5S).
                   //                 ,h: Option[Height] = None // Physical height of the screen in pixels
                   //                 ,w: Option[Width] = None // Physical width of the screen in pixels.
                   //                 ,ppi: Option[Count] = None // ￼ Screen size as pixels per linear inch.
                   //                 ,pxratio: Option[RatioFloat] = None //  The ratio of physical pixels to device independent pixels.
                   ,js: Option[YesNoType.Type] = None // Support for JavaScript, where 0 = no, 1 = yes.
                   ,flashver: Option[VersionString] = None // Version of Flash supported by the browser
                   ,language: Option[Language] = None //￼ Browser language using ISO-639-1-alpha-2.
                   ,carrier: Option[Name] = None //Carrier or ISP (e.g., “VERIZON”). “WIFI” is often used in mobile to indicate high bandwidth (e.g., video friendly vs. cellular).
                   ,connectiontype: Option[IntId] = None // ￼ Network connection type. Refer to List 5.18.
                   ,ifa: Option[StringId] = None // ID sanctioned for advertiser use in the clear (i.e., not hashed).
                   //                 ,didsha1: Option[Hash] = None //Hardware device ID (e.g., IMEI); hashed via SHA1.
                   //                 ,didmd5: Option[Hash] = None //Hardware device ID (e.g., IMEI); hashed via MD5.
                   //                 ,dpidsha1: Option[Hash] = None //Platform device ID (e.g., Android ID); hashed via SHA1.
                   //                 ,dpidmd5: Option[Hash] = None //Platform device ID (e.g., Android ID); hashed via MD5.
                   //                 ,macsha1: Option[Hash] = None //MAC address of the device; hashed via SHA1.
                   //                 ,macmd5: Option[Hash] = None //MAC address of the device; hashed via MD5.
                   //                 ,ext: Option[DeviceExt] = None
                   )

case class Publisher(
                      id: Option[StringId] = None
                      ,name: Option[Name] = None
                      ,cat: Option[List[IABCategory]] = None
                      ,domain: Option[Domain] = None
                      ,ext: Option[PublisherExt] = None
                      )

case class App(
                id: Option[StringId] = None // Recommended
                ,name: Option[Name] = None
                ,bundle: Option[FullyQualifiedName] = None
                ,domain: Option[Domain] = None
                ,storeurl: Option[Url] = None
                ,cat: Option[List[IABCategory]] = None
                ,sectioncat: Option[List[IABCategory]] = None
                ,pagecat: Option[List[IABCategory]] = None
                ,ver: Option[VersionString] = None
                ,privacypolicy: Option[YesNoType.Type] = None
                ,paid: Option[YesNoType.Type] = None
                ,publisher: Option[Publisher] = None
                ,content: Option[Content] = None
                ,keywords: Option[List[KeyWord]]  = None  //Serialized as comma separated
                ,ext: Option[AppExt] = None
                )

case class Site(
                 id: Option[StringId] = None
                 ,name: Option[Name] = None
                 ,domain: Option[Domain] = None
                 ,cat: Option[List[IABCategory]] = None
                 ,sectioncat: Option[List[IABCategory]] = None
                 ,pagecat: Option[List[IABCategory]] = None
                 ,page: Option[Url] = None
                 ,ref: Option[Url] = None
                 ,search: Option[SearchString] = None
                 ,mobile: Option[YesNoType.Type] = None
                 ,privacypolicy: Option[YesNoType.Type] = None
                 ,publisher: Option[Publisher] = None
                 ,content: Option[Content] = None
                 ,keywords: Option[List[KeyWord]] = None //Serialized as comma separated list
                 ,ext: Option[SiteExt] = None
                 )

case class Imp(
                id: StringId                    // Required; AKA Gravity Auction ID
                ,banner: Option[Banner] = None
                ,video: Option[Video] = None
                ,native: Option[Native] = None
                ,displaymanager: Option[Name] = None
                ,displaymanagerver: Option[VersionString] = None
                ,instl: Option[YesNoType.Type] = Option(YesNoType.no)
                ,tagid: Option[PlacementId] = None
                ,bidfloor: Option[BigDecimal] = Option(BigDecimal(0))  // Serialized as decimal CPM in dollars.
                ,bidfloorcur: Option[CurrencyCode] = Option(CurrencyCode("USD"))
                ,secure: Option[YesNoType.Type] = None
                ,iframebuster: Option[List[Name]] = None
                ,pmp: Option[PMP] = None
                ,ext: Option[ImpExt] = None
                )

case class BidRequest(
                       id: StringId // Required; Unique auction identifier.
                       ,imp: List[Imp] // Required; The impression object is always sent.
                       ,device: Option[Device] = None // Recommended; The device object is always sent.
                       ,site: Option[Site] = None   // Recommended; The site object is sent for all browser (non-app) requests
                       ,app: Option[App] = None  // Recommended; The app object is sent for all mobile app (non browser) requests.
                       ,user: Option[User] = None  // Recommended; The user object is always sent.
                       ,test: Option[RequestType.Type] = Option(RequestType.live) // Indicator of test mode in which auctions are not billable, where 0 = live mode 1 = test mode
                       ,at: Option[AuctionType.Type] = Option(AuctionType.secondPricePlus) // 1 for first price auction, 2 for second price auction
                       ,tmax: Option[Millis] = None // SLA in milliseconds to submit a bid.
                       ,wseat: Option[List[SeatId]] = None //Whitelist of buyer seats allowed to bid on this impression
                       ,allimps: Option[AllImpsType.Type] = Option(AllImpsType.noOrUnknown) //Flag to indicate if Exchange can verify that the impressions offered represent all of the impressions available in context
                       ,cur: Option[List[CurrencyCode]] = None // Array of allowed currencies for bids on this bid request using ISO-4217 alphabetic codes. If only one currency is used by the exchange, this parameter is not required.
                       ,bcat: Option[List[IABCategory]] = None // Blocked Advertiser Categories.
                       ,badv: Option[List[Domain]]  = None// Array of strings of blocked toplevel domains of advertisers. For example, ,“company1.com”, “company2.com”-.
                       ,regs: Option[Regs] = None
                       ,ext: Option[BidRequestExt] = None
                       )

object BidRequestFormats {
  //  implicit val iabFmt = Format(Reads[IABCategory] {
  //    case JsString(str) => JsSuccess(IABCategory.apply(str))
  //    case _ => JsError("expected string for IABCategory")
  //  }, Writes[IABCategory](x => JsString(x.raw)))
  //
  //  implicit val iabFmt = Format(Reads[IABCategory] {
  //    case JsString(str) => JsSuccess(IABCategory.apply(str))
  //    case _ => JsError("expected string for IABCategory")
  //  }, Writes[IABCategory](x => JsString(x.raw)))

  def extFmt[T](t: T): Format[T] = Format(Reads(jv => JsSuccess(t)), Writes((x:T) => JsObject(Seq.empty)))

  implicit val segmentExtFmt: Format[SegmentExt] = extFmt(SegmentExt())
  implicit val dataExtFmt: Format[DataExt] = extFmt(DataExt())
  implicit val impExtFmt: Format[ImpExt] = extFmt(ImpExt())
  implicit val bidReqExtFmt: Format[BidRequestExt] = extFmt(BidRequestExt())
  implicit val ntvReqExtFmt: Format[NativeRequestExt] = extFmt(NativeRequestExt())
  implicit val titleReqExtFmt: Format[TitleRequestExt] = extFmt(TitleRequestExt())
  implicit val imageExtFmt: Format[ImageRequestExt] = extFmt(ImageRequestExt())
  implicit val ntvVidReqExtFmt: Format[NativeVideoRequestExt] = extFmt(NativeVideoRequestExt())
  implicit val ntvDataReqExtFmt: Format[NativeDataRequestExt] = extFmt(NativeDataRequestExt())
  implicit val assetReqExtFmt: Format[AssetRequestExt] = extFmt(AssetRequestExt())
  implicit val pmpExtFmt: Format[PMPExt] = extFmt(PMPExt())
  implicit val bannerExtFmt: Format[BannerExt] = extFmt(BannerExt())
  implicit val dftNativeExtFmt: Format[DefaultNativeExt] = extFmt(DefaultNativeExt())
  implicit val dealExtFmt: Format[DealExt] = extFmt(DealExt())
  implicit val deviceExtFmt: Format[DeviceExt] = extFmt(DeviceExt())
  implicit val geoExtFmt: Format[GeoExt] = extFmt(GeoExt())
  implicit val siteExtFmt: Format[SiteExt] = extFmt(SiteExt())
  implicit val publisherExtFmt: Format[PublisherExt] = extFmt(PublisherExt())
  implicit val AppExtFmt: Format[AppExt] = extFmt(AppExt())
  implicit val UserExtFmt: Format[UserExt] = extFmt(UserExt())

  implicit val producerExtFmt: Format[ProducerExt] = extFmt(ProducerExt())
  implicit val videoExtFmt: Format[VideoExt] = extFmt(VideoExt())
  implicit val contentExtFmt: Format[ContentExt] = extFmt(ContentExt())
  implicit val regsExtFmt: Format[RegsExt] = extFmt(RegsExt())

  implicit val imgTypeFmt : Format[Image.Type] = Image.fmt
  implicit val mimeTypeFmt = MimeTypes.fmt
  implicit val auctionTypeFmt = AuctionType.fmt

  //  implicit val iabCatFmt = Json.format[IABCategory]
  implicit val iabCatFmt: Format[IABCategory] = Format(Reads[IABCategory] {
    case JsString(str) => JsSuccess(IABCategory.apply(str))
    case _ => JsError("expected string for IABCategory")
  }, Writes[IABCategory](x => JsString(x.raw)))

  //  implicit val ccFmt = Json.format[CurrencyCode]
  implicit val currCodeFmt: Format[CurrencyCode] = Format(Reads[CurrencyCode] {
    case JsString(str) => JsSuccess(CurrencyCode.apply(str))
    case _ => JsError("expected string for CurrencyCode")
  }, Writes[CurrencyCode](x => JsString(x.raw)))

  //  implicit val seatIdFmt = Json.format[SeatId]
  implicit val seatIdFmt: Format[SeatId] = Format(Reads[SeatId] {
    case JsString(str) => JsSuccess(SeatId.apply(str))
    case _ => JsError("expected string for SeatId")
  }, Writes[SeatId](x => JsString(x.raw)))

  //  implicit val plIdFmt = Json.format[PlacementId]
  implicit val plIdFmt: Format[PlacementId] = Format(Reads[PlacementId] {
    case JsString(str) => JsSuccess(PlacementId.apply(str))
    case _ => JsError("expected string for PlacementId")
  }, Writes[PlacementId](x => JsString(x.raw)))

  //  implicit val regionFmt = Json.format[Region]
  implicit val regionFmt: Format[Region] = Format(Reads[Region] {
    case JsString(str) => JsSuccess(Region.apply(str))
    case _ => JsError("expected string for Region")
  }, Writes[Region](x => JsString(x.raw)))

  //  implicit val utcFmt = Json.format[UTCOffset]
  implicit val utcFmt: Format[UTCOffset] = Format(Reads[UTCOffset] {
    case JsNumber(str) => JsSuccess(UTCOffset.apply(str.toInt))
    case _ => JsError("expected number for UTCOffset")
  }, Writes[UTCOffset](x => JsNumber(x.raw)))

  implicit val producerFmt: Format[Producer] = Json.format[Producer]
  implicit val contentFmt: Format[Content] = Json.format[Content]
  implicit val bannerFmt: Format[Banner] = Json.format[Banner]

  implicit val titleReqFmt: Format[TitleRequest] = Json.format[TitleRequest]
  implicit val imgReqFmt: Format[ImageRequest] = Json.format[ImageRequest]
  implicit val ntvVideoReqFmt: Format[NativeVideoRequest] = Json.format[NativeVideoRequest]
  implicit val ntvDataReqFmt: Format[NativeDataRequest] = Json.format[NativeDataRequest]

  implicit val assetReqFmt: Format[AssetRequest] with Object {val base: Format[AssetRequest]; def writes(obj: AssetRequest): JsValue; def reads(json: JsValue): JsResult[AssetRequest]} = new Format[AssetRequest] {
    val base: Format[AssetRequest] = Json.format[AssetRequest]

    def reads(json: JsValue): JsResult[AssetRequest] = base
      .compose(withDefault("required", Option(RequiredAsset.notRequired)))
      .reads(json)

    def writes(obj: AssetRequest): JsValue = base.writes(obj)
  }

  implicit val ntvReqFmt: Format[NativeRequest] with Object {val base: Format[NativeRequest]; def writes(obj: NativeRequest): JsValue; def reads(json: JsValue): JsResult[NativeRequest]} = new Format[NativeRequest] {
    val base: Format[NativeRequest] = Json.format[NativeRequest]

    def reads(json: JsValue): JsResult[NativeRequest] = base
      .compose(withDefault("ver"     , Option(VersionString("444"))))
      .compose(withDefault("plcmtcnt", Option(Count(1))))
      .compose(withDefault("seq"     , Option(Index(0))))
      .reads(json)

    def writes(obj: NativeRequest): JsValue = base.writes(obj)
  }

  implicit val ntvFmt: Format[Native] = Json.format[Native]
  implicit val dealFmt: Format[Deal] with Object {val base: Format[Deal]; def writes(obj: Deal): JsValue; def reads(json: JsValue): JsResult[Deal]} = new Format[Deal] {
    val base: Format[Deal] = Json.format[Deal]

    def reads(json: JsValue): JsResult[Deal] = base
      .compose(withDefault("bidfloor"   , Option(BigDecimal(0))))
      .compose(withDefault("bidfloorcur", Option(CurrencyCode("USD"))))
      .reads(json)

    def writes(obj: Deal): JsValue = base.writes(obj)
  }

  implicit val pmpFmt: Format[PMP] = Json.format[PMP]
  implicit val geoFmt: Format[Geo] = Json.format[Geo]

  implicit val pubFmt: Format[Publisher] = Json.format[Publisher]

  implicit val videoFmt: Format[Video] with Object {val base: Format[Video]; def writes(obj: Video): JsValue; def reads(json: JsValue): JsResult[Video]} = new Format[Video] {
    val base: Format[Video] = Json.format[Video]

    def reads(json: JsValue): JsResult[Video] = base
      .compose(withDefault("boxingallowed", Option(YesNoType.yes)))
      .reads(json)

    def writes(obj: Video): JsValue = base.writes(obj)
  }
  
  implicit val impFmt: Format[Imp] with Object {val base: Format[Imp]; def writes(obj: Imp): JsValue; def reads(json: JsValue): JsResult[Imp]} = new Format[Imp] {
    val base: Format[Imp] = Json.format[Imp]

    def reads(json: JsValue): JsResult[Imp] = base
      .compose(withDefault("instl"      , Option(YesNoType.no)))
      .compose(withDefault("bidfloor"   , Option(BigDecimal(0))))
      .compose(withDefault("bidfloorcur", Option(CurrencyCode("USD"))))
      .reads(json)

    def writes(obj: Imp): JsValue = base.writes(obj)
  }
  
  implicit val deviceFmt: Format[Device] = Json.format[Device]
  implicit val siteFmt: Format[Site] = Json.format[Site]
  implicit val appFmt: Format[App] = Json.format[App]
  implicit val segmentFmt: Format[Segment] = Json.format[Segment]
  implicit val dataFmt: Format[Data] = Json.format[Data]
  implicit val userFmt: Format[User] = Json.format[User]
  implicit val regsFmt: Format[Regs] = Json.format[Regs]

  implicit val bidReqFmt: Format[BidRequest] with Object {val base: Format[BidRequest]; def writes(obj: BidRequest): JsValue; def reads(json: JsValue): JsResult[BidRequest]} = new Format[BidRequest] {
    val base: Format[BidRequest] = Json.format[BidRequest]

    def reads(json: JsValue): JsResult[BidRequest] = base
      .compose(withDefault("test"   , Option(RequestType.live)))
      .compose(withDefault("at"     , Option(AuctionType.secondPricePlus)))
      .compose(withDefault("allimps", Option(AllImpsType.noOrUnknown)))
      .reads(json)

    def writes(obj: BidRequest): JsValue = base.writes(obj)
  }
}

object Native {
  import BidRequestFormats._

  def fromNativeRequest(nativeReq: NativeRequest              // Required; A "Native Markup Request Object" per OpenRTB Native Ads Specification
                        ,ver: Option[VersionString] = None    // Recommended
                        ,api: Option[List[APIFramework.Type]] = None
                        ,battr: Option[List[CreativeAttribute.Type]] = None
                        ,ext: Option[DefaultNativeExt] = None): Native = {
    Native(Json.stringify(Json.toJson(nativeReq)), ver, api, battr, ext)
  }
}

case class Native(request: String                      // Required; A String containing an [e.g. JSON]-encoded NativeRequest, a "Native Markup Request Object" per OpenRTB Native Ads Specification
                  ,ver: Option[VersionString] = None   // Recommended
                  ,api: Option[List[APIFramework.Type]] = None
                  ,battr: Option[List[CreativeAttribute.Type]] = None
                  ,ext: Option[DefaultNativeExt] = None
                   )

//
//case class GrvBidRequest(
//                           id: String // Unique auction identifier.
//                           ,imp: Imp // The impression object is always sent.
//                           ,device: Option[Device] // The device object is always sent.
//                           ,site: Option[Site] // The site object is sent for all browser (non-app) requests.
//                           ,app: Option[App] // The app object is sent for all mobile app (non browser) requests.
//                           ,user: Option[User] // The user object is always sent.
//                           ,test: Option[FeatureToggle] // Indicator of test mode in which auctions are not billable, where 0 = live mode 1 = test mode
//                           ,at: Option[AuctionType.Type] // 1 for first price auction, 2 for second price auction
//                           ,tmax: Option[Millis] = None // SLA in milliseconds to submit a bid.
//                           ,wseat: List[SeatId] //Whitelist of buyer seats allowed to bid on this impression
//                           ,allimps: Option[FeatureToggle] //Flag to indicate if Exchange can verify that the impressions offered represent all of the impressions available in context
//                           ,cur: Option[List[CurrencyCode]] // Array of allowed currencies for bids on this bid request using ISO-4217 alphabetic codes. If only one currency is used by the exchange, this parameter is not required.
//                           ,bcat: List[IABCategory] // Blocked Advertiser Categories.
//                           ,badv: List[Domain] // Array of strings of blocked toplevel domains of advertisers. For example, ,“company1.com”, “company2.com”-.
//                           ,regs: Option[Regs]
//                           ,ext: Option[Ext]
//                           ) extends BidRequest(id, imp, device, site, app, user, test, at, tmax, wseat, allimps, cur, bcat, badv, regs, ext)
