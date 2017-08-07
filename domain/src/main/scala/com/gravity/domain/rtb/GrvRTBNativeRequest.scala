package com.gravity.domain.rtb

import com.gravity.domain.{StrictUserGuid, Ipv4Address, MicroDollar}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.dbpedia.Language
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvz._
import com.gravity.utilities.web.MimeTypes.MimeType
import com.gravity.valueclasses.ValueClassesForUtilities._
import com.gravity.utilities.grvjson._
import com.gravity.valueclasses.ValueClassesForDomain._
import play.api.libs.json._
import play.api.libs.functional.syntax._

import scalaz.{Failure, Success, ValidationNel, Validation, NonEmptyList}
import scalaz.syntax.validation._
import scalaz.syntax.std.option._

/**
 * Created by runger on 4/30/15.
 */


@SerialVersionUID(1l)
object Layout extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val contentWall: Type = Value(1, "Content Wall")
  val appWall: Type = Value(2, "App Wall")
  val newsFeed: Type = Value(3, "News Feed")
  val chatList: Type = Value(4, "Chat List")
  val carousel: Type = Value(5, "Carousel")
  val contentStream: Type = Value(6, "Content Stream")
  val grid: Type = Value(7, "Grid adjoining the content")

  val defaultValue: Type = unknown

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]

}

@SerialVersionUID(1l)
object AdUnit extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val paidSearch: Type = Value(1, "Paid Search Units")
  val recommendation: Type = Value(2, "Recommendation Widgets")
  val promotedListing: Type = Value(3, "Promoted Listings")
  val inAdWithNative: Type = Value(4, "In-Ad (IAB Standard) with Native Element Units")
  val custom: Type = Value(5, "Custom")

  val defaultValue: Type = unknown

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]
}

@SerialVersionUID(1l)
object Image extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val icon: Type = Value(1, "Icon")
  val logo: Type = Value(2, "Logo")
  val main: Type = Value(3, "Main")

  val defaultValue: Type = unknown

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]
}

@SerialVersionUID(1l)
object NativeDataAssetTypes extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")     // NOT IN THE STANDARD
  val sponsored: Type = Value(1, "sponsored")   // (text) Sponsored By message where response should contain the brand name of the sponsor.
  val desc: Type = Value(2, "desc")        // (text) Descriptive text associated with the product or service being advertised.
  val rating: Type = Value(3, "rating")      // (number formatted as string) Rating of the product being offered to the user. For example an app’s rating in an app store from 0-5.
  val likes: Type = Value(4, "likes")       // (number formatted as string) Number of social ratings or “likes” of the product being offered to the user.
  val downloads: Type = Value(5, "downloads")   // (number formatted as string) Number of downloads/installs of this product.
  val price: Type = Value(6, "price")       // (number formatted as string) Price for product / app / in-app purchase. Value should include currency symbol in localised format.
  val saleprice: Type = Value(7, "saleprice")   // (number formatted as string) Sale price that can be used together with price to indicate a discounted price compared to a regular price. Value should include currency symbol in localised format.
  val phone: Type = Value(8, "phone")       // (formatted string) Phone number
  val address: Type = Value(9, "address")     // (text) Address
  val desc2: Type = Value(10, "desc2")      // (text) Additional descriptive text associated with the product or service being advertised
  val displayurl: Type = Value(11, "displayurl") // (text) Display URL for the text ad
  val ctatext: Type = Value(12, "ctatext")    // (text) CTA description - descriptive text describing a ‘call to action’ button for the destination URL.
  // exchangeSpecifc numbers should start at 500.

  val defaultValue: Type = unknown

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]
}

@SerialVersionUID(1l)
object RequiredAsset extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val notRequired: Type = Value(0, "not required")
  val required: Type = Value(1, "required")

  val defaultValue: Type = notRequired

  implicit val fmt: Format[Type] = makeJsonFormatByIndex[Type]
}

case class NativeRequestExt()

case class AssetRequestExt()

case class ImageRequestExt()

case class TitleRequestExt()

case class NativeDataRequestExt()

case class NativeVideoRequestExt()

object RtbRequestJsonFormats {}

case class NativeDataRequest(
                            `type`: NativeDataAssetTypes.Type
                            ,len: Option[Size]
                            ,ext: Option[NativeDataRequestExt]
                              )

case class NativeVideoRequest(
                             mimes: List[MimeType]
                             ,minduration: Seconds
                             ,maxduration: Seconds
                             ,protocols: List[IntId]  //Really should be an enum when this is used
                             ,ext: Option[NativeVideoRequestExt]
                               )

case class ImageRequest(
                `type`: Option[Image.Type] = None
                ,w: Option[Width] = None
                ,wmin: Option[Width] = None  //One of these, prefer wmin
                ,h: Option[Height] = None
                ,hmin: Option[Height] = None
                ,mimes: Option[List[MimeType]] = Option(List.empty)
                ,ext: Option[ImageRequestExt] = None
                  )

case class TitleRequest(
                          len: Size // Required; Maximum length of the text in the title element.
                          ,ext: Option[TitleRequestExt] = None
                          )

case class AssetRequest(
                          id: IntId   // Required
                          ,required: Option[RequiredAsset.Type] = Some(RequiredAsset.notRequired)
                          ,title: Option[TitleRequest] = None
                          ,img: Option[ImageRequest] = None
                          ,video: Option[NativeVideoRequest] = None
                          ,data: Option[NativeDataRequest] = None
                          ,ext: Option[AssetRequestExt] = None
                          )

case class NativeRequest(
                          // The OpenRTB Dynamic Native Ads API Specification Version 1
                          // documents ver as being a "String with default value 1"
                          // (with no quotes around the 1, as if it were an integer).
                          // The corresponding NativeResponse.ver is defined as
                          // an integer with a default value of (integer) 1.
                          ver: Option[VersionString] = Option(VersionString("1"))
                          ,layout: Option[Layout.Type] = None  // Recommended
                          ,adunit: Option[AdUnit.Type] = None  // Recommended
                          ,plcmtcnt: Option[Count] = Option(Count(1))
                          ,seq: Option[Index] = Option(Index(0))
                          ,assets: List[AssetRequest] = List.empty   // Required
                          ,ext: Option[NativeRequestExt] = None
                   )

object NativeRequest {
  import BidRequestFormats._

  def fromNative(native: Native): ValidationNel[FailureResult, NativeRequest] =
    fromJsonStr(native.request)

  def fromJsonStr(request: String): ValidationNel[FailureResult, NativeRequest] =
    Json.fromJson[NativeRequest](Json.parse(request)).toValidation.leftMap(e => nel(FailureResult(e.toString)))
}

//
//object GrvRTBNativeRequest {
//
//  type ¬[A] = A => Nothing
//  type ∨[T, U] = ¬[¬[T] with ¬[U]]
//  type ¬¬[A] = ¬[¬[A]]
//  type |∨|[T, U] = { type λ[X] = ¬¬[X] <:< (T ∨ U) }
//
//}

//case class TitleAsset(
//                       id: Int
//                       ,required: Option[BinaryState]
//                       ,title: Title
//                       ,ext: Option[AssetExt]
//                       ) extends Asset(id, required, ext)

//object Asset {
//  type AssetType[T] = (Title |∨| Image |∨| Data)#λ[T]
//  type TitleUnionImage[T] = (Title |∨| Image)#λ[T]
//  type TitleUnionImageUnionData[T, TT] = (TitleUnionImage[TT] |∨| Data)#λ[T]
//
//  implicit val assetReads
//
////  implicit def assetReads[T: AssetType]: Reads[T] = Reads(jv => {
////    (jv \ "title").asOpt[String] match {
////      case Some(t) => JsSuccess(Title())
////      case None => (jv \ "url").asOpt[String] match {
////        case Some(u) => JsSuccess(Image())
////        case None => (jv \ "data").asOpt[String] match {
////          case Some(d) => JsSuccess(Data())
////          case None => JsError("not title image or data")
////        }
////      }
////    }
////  })
//}