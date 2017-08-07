package com.gravity.domain.rtb

import com.gravity.domain.MicroDollar
import com.gravity.domain.rtb.RequestType._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvjson._
import com.gravity.valueclasses.ValueClassesForUtilities._
import com.gravity.valueclasses.ValueClassesForDomain._
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scalaz.{Failure, Success, ValidationNel, Validation, NonEmptyList}
import scalaz.syntax.validation._
import scalaz.syntax.std.option._

/**
 * Created by runger on 4/30/15.
 */

object GrvRTBResponse

@SerialVersionUID(1l)
object NoBidReason extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknownError: Type = Value(0, "Unknown Error")
  val technicalError: Type = Value(1, "Technical Error")
  val invalidRequest: Type = Value(2, "Invalid Request")
  val knownWebSpider: Type = Value(3, "Known Web Spider")
  val nonHuman: Type = Value(4, "Suspected Non-Human Traffic")
  val cloudIP: Type = Value(5, "Cloud, Data center, or Proxy IP")
  val unsupportedDevice: Type = Value(6, "Unsupported Device")
  val blockedPub: Type = Value(7, "Blocked Publisher or Site")
  val unmatchedUser: Type = Value(8, "Unmatched User")

  val defaultValue: Type = unknownError

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]
}

@SerialVersionUID(1l)
object CreativeAttribute extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val audioAutoInit: Type = Value(1, "Audio Ad (Auto-Play)")
  val audioUserInit: Type = Value(2, "Audio Ad (User Initiated)")
  val expandableAutoInit: Type = Value(3, "Expandable (Automatic)")
  val expandableUserInitClick: Type = Value(4, "Expandable (User Initiated - Click)")
  val expandableUserInitRollover: Type = Value(5, "Expandable (User Initiated - Rollover)")
  val inBannerVideoAutoInit: Type = Value(6, "In-Banner Video Ad (Auto-Play)")
  val inBannerVideoUserInit: Type = Value(7, "In-Banner Video Ad (User Initiated)")
  val pop: Type = Value(8, "Pop (e.g., Over, Under, or Upon Exit)")
  val provocative: Type = Value(9, "Provocative or Suggestive Imagery")
  val animationSmileys: Type = Value(10, "Shaky, Flashing, Flickering, Extreme Animation, Smileys")
  val surveys: Type = Value(11, "Surveys")
  val textOnly: Type = Value(12, "Text Only")
  val userInteractive: Type = Value(13, "User Interactive (e.g., Embedded Games)")
  val windowsDialog: Type = Value(14, "Windows Dialog or Alert Style")
  val audioOnOff: Type = Value(15, "Has Audio On/Off Button")
  val canBeSkipped: Type = Value(16, "Ad Can be Skipped (e.g., Skip Button on Pre-Roll Video)")

  val defaultValue: Type = unknown

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]
}

case class BidResponseExt()

case class SeatBidExt()

case class BidExt(
                     cpcPrice: Option[MicroDollar]
                     ,cpmPrice: Option[MicroDollar]
                     ,cpaPrice: Option[MicroDollar]
                     ,reportTag1: Option[Label]
                     ,reportTag2: Option[Label]
                     ,reportTag3: Option[Label]
)

case class Bid(
                  id: StringId // Required; Bidder generated bid ID to assist with logging/tracking
                  ,impid: StringId // Required; ID of the Imp object in the related bid request
                  ,price: Option[BigDecimal] // Deviation from Standard -- required by OpenRTB, but not used by Gravity, so optional.
                  ,adid: Option[StringId] = None // ID of a preloaded ad to be served if the bid wins
                  ,nurl: Option[Url] = None // Win notice URL called by the exchange if the bid wins; optional means of serving ad markup
                  ,adm: Option[String] = None // Ad Markup. If this is a response to a Native Impression Bid Request, contains a (JSON-)encoded NativeResponse
                  ,adomain: Option[List[Domain]] = None // Advertiser domain for block list checking
                  ,bundle: Option[FullyQualifiedName] = None
                  ,iurl: Option[Url] = None // URL without cache-busting to an image that is representative of the content of the campaign for ad quality/safety checking
                  ,cid: Option[StringId] = None //Campaign ID to assist with ad quality checking; the collection of creatives for which iurl should be representative
                  ,crid: Option[StringId] = None //Creative ID to assist with ad quality checking
                  ,cat: Option[List[IABCategory]] = None // IAB content categories of the creative
                  ,attr: Option[List[CreativeAttribute.Type]] = None // Set of attributes describing the creative
                  ,dealid: Option[StringId] = None // Reference to the deal.id from the bid request if this bid pertains to a private marketplace direct deal
                  ,h: Option[Height] = None
                  ,w: Option[Width] = None
                  ,ext: Option[BidExt] = None
                )

//case class GrvBid(
//                 id: StringId // Bidder generated bid ID to assist with logging/tracking
//                 ,impid: StringId // ID of the Imp object in the related bid request
//                 ,price: MicroDollar
//                 ,adid: Option[StringId] // ID of a preloaded ad to be served if the bid wins
//                 ,nurl: Option[Url] // Win notice URL called by the exchange if the bid wins; optional means of serving ad markup
//                 ,adm: Option[Url] // Optional means of conveying ad markup in case the bid wins; supersedes the win notice if markup is included in both.
//                 ,adomain: List[Domain] // Advertiser domain for block list checking
//                 ,bundle: Option[FullyQualifiedName]
//                 ,iurl: Option[Url] // URL without cache-busting to an image that is representative of the content of the campaign for ad quality/safety checking
//                 ,cid: Option[StringId] //Campaign ID to assist with ad quality checking; the collection of creatives for which iurl should be representative
//                 ,crid: Option[StringId] //Creative ID to assist with ad quality checking
//                 ,cat: List[IABCategory] // IAB content categories of the creative
//                 ,attr: List[CreativeAttribute.Type] // Set of attributes describing the creative
//                 ,dealid: Option[StringId] // Reference to the deal.id from the bid request if this bid pertains to a private marketplace direct deal
//                 ,h: Option[Height]
//                 ,w: Option[Width]
//                 ,ext: Option[BidExt]
//                   ) extends Bid(id, impid, price, adid, nurl, adm, adomain, bundle, iurl, cid, crid, cat, attr, dealid, h, w, ext)

case class SeatBid(
                  bid: List[Bid]
                  ,seat: Option[StringId] = None
                  ,group:Option[YesNoType.Type] = Option(YesNoType.no) //Only type "no" is supported (this field can be ignored)
                  ,ext: Option[SeatBidExt] = None        //Ignored
                    )

case class BidResponse(
                      id: StringId
                      ,seatbid: List[SeatBid]
                      ,bidid: Option[StringId]
                      ,cur: Option[CurrencyCode] = Option(CurrencyCode("USD")) // Ignored
                      ,customdata: Option[String] = None   //Ignored
                      ,nbr: Option[NoBidReason.Type] = None //Ignored
                      ,ext: Option[BidResponseExt] = None //Ignored
                        )

object RtbResponseJsonFormats {

  import BidRequestFormats._

  implicit val linkResponseExtFmt: Format[LinkResponseExt] = placeholderFmt[LinkResponseExt]
  implicit val titleResponseExtFmt: Format[TitleResponseExt] = placeholderFmt[TitleResponseExt]
  implicit val imageResponseExtFmt: Format[ImageResponseExt] = placeholderFmt[ImageResponseExt]
  implicit val assetResponseExtFmt: Format[AssetResponseExt] = placeholderFmt[AssetResponseExt]
  implicit val nativeDataResponseExtFmt: Format[NativeDataResponseExt] = placeholderFmt[NativeDataResponseExt]
  implicit val nativeResponseExtFmt: Format[NativeResponseExt] = placeholderFmt[NativeResponseExt]
  implicit val bidExtFmt: Format[BidExt] = placeholderFmt[BidExt]
  implicit val seatBidExtFmt: Format[SeatBidExt] = placeholderFmt[SeatBidExt]
  implicit val bidResponseExtFmt: Format[BidResponseExt] = placeholderFmt[BidResponseExt]

  // Using formatNullableIterable here allows empty lists to be omitted when serializing to JSON.
  implicit val linkResponseFmt: OFormat[LinkResponse] = (
    (__ \ "url").format[Url] and
      (__ \ "clicktrackers").formatNullableIterable[List[Url]] and
      (__ \ "fallback").formatNullable[Url] and
      (__ \ "ext").formatNullable[LinkResponseExt]
    )(LinkResponse.apply, unlift(LinkResponse.unapply))

  implicit val titleResponseFmt: Format[TitleResponse] = Json.format[TitleResponse]

  implicit val imageResponseFmt: Format[ImageResponse] = Json.format[ImageResponse]

  implicit val nativeVideoResponseFmt: Format[NativeVideoResponse] = Json.format[NativeVideoResponse]

  implicit val nativeDataResponseFmt: Format[NativeDataResponse] = Json.format[NativeDataResponse]

  implicit val assetResponseFmt: Format[AssetResponse] with Object {val base: Format[AssetResponse]; def writes(obj: AssetResponse): JsValue; def reads(json: JsValue): JsResult[AssetResponse]} = new Format[AssetResponse] {
    val base: Format[AssetResponse] = Json.format[AssetResponse]

    def reads(json: JsValue): JsResult[AssetResponse] = base
      .compose(withDefault("required", Option(RequiredAsset.notRequired)))
      .reads(json)

    def writes(obj: AssetResponse): JsValue = base.writes(obj)
  }

  implicit val nativeResponseFmt: Format[NativeResponse] with Object {val base: OFormat[NativeResponse]; def writes(obj: NativeResponse): JsValue; def reads(json: JsValue): JsResult[NativeResponse]} = new Format[NativeResponse] {
    // Using formatNullableIterable here allows empty lists to be omitted when serializing to JSON.
    val base: OFormat[NativeResponse] = (
      (__ \ "ver").formatNullable[VersionInt] and
        (__ \ "assets").formatNullableIterable[List[AssetResponse]] and
        (__ \ "link").format[LinkResponse] and
        (__ \ "imptrackers").formatNullableIterable[List[Url]] and
        (__ \ "jstracker").formatNullable[Url] and
        (__ \ "ext").formatNullable[NativeResponseExt]
      )(NativeResponse.apply, unlift(NativeResponse.unapply))

    def reads(json: JsValue): JsResult[NativeResponse] = base
      .compose(withDefault("ver"        , Option(VersionInt(1))))
      .reads(json)

    def writes(obj: NativeResponse): JsValue = base.writes(obj)
  }

  implicit val bidFmt: Format[Bid] = Json.format[Bid]

  implicit val seatBidRespFmt: Format[SeatBid] with Object {val base: Format[SeatBid]; def writes(obj: SeatBid): JsValue; def reads(json: JsValue): JsResult[SeatBid]} = new Format[SeatBid] {
    val base: Format[SeatBid] = Json.format[SeatBid]

    def reads(json: JsValue): JsResult[SeatBid] = base
      .compose(withDefault("group", Option(YesNoType.no)))
      .reads(json)

    def writes(obj: SeatBid): JsValue = base.writes(obj)
  }

  implicit val bidRespFmt: Format[BidResponse] with Object {val base: Format[BidResponse]; def writes(obj: BidResponse): JsValue; def reads(json: JsValue): JsResult[BidResponse]} = new Format[BidResponse] {
    val base: Format[BidResponse] = Json.format[BidResponse]

    def reads(json: JsValue): JsResult[BidResponse] = base
      .compose(withDefault("cur", Option(CurrencyCode("USD"))))
      .reads(json)

    def writes(obj: BidResponse): JsValue = base.writes(obj)
  }
}