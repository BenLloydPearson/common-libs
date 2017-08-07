package com.gravity.domain.rtb

import com.gravity.domain.{StrictUserGuid, Ipv4Address, MicroDollar}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.dbpedia.Language
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvz._
import com.gravity.valueclasses.ValueClassesForUtilities._
import com.gravity.valueclasses.ValueClassesForDomain._
import play.api.libs.json._
import play.api.libs.functional.syntax._

import scalaz.{Failure, Success, ValidationNel, Validation, NonEmptyList}
import scalaz.syntax.validation._
import scalaz.syntax.std.option._

/**
 * Created by runger on 5/7/15.
 */
object GrvRTBNativeResponse

case class AssetResponseExt()
case class TitleResponseExt()
case class ImageResponseExt()
case class NativeDataResponseExt()
case class LinkResponseExt()
case class NativeResponseExt()

case class LinkResponse(
                 url: Url
                 ,clicktrackers: List[Url] = List.empty
                 ,fallback: Option[Url] = None
                 ,ext: Option[LinkResponseExt] = None
                 )

case class NativeDataResponse(
                               label: Option[Label] = None
                               ,value: Text
                               ,ext: Option[NativeDataResponseExt] = None
                               )

case class NativeVideoResponse(
                                vasttag: XmlString
                                )

case class ImageResponse(
                  url: Url
                  ,w: Option[Width] = None
                  ,h: Option[Height] = None
                  ,ext: Option[ImageResponseExt] = None
                  )

case class TitleResponse(
                  text: Text
                  ,ext: Option[TitleResponseExt] = None
                  )

case class AssetResponse(
                  id: IntId //Unique asset ID, assigned by exchange, must match one of the asset IDs in request
                  ,required: Option[RequiredAsset.Type] = Some(RequiredAsset.notRequired) //Set to 1 if asset is required
                  ,title: Option[TitleResponse] = None //Title object for title assets
                  ,img: Option[ImageResponse] = None //Image object for image assets
                  ,video: Option[NativeVideoResponse] = None //Video object for video assets
                  ,data: Option[NativeDataResponse] = None //Data object for ratings, prices, etc.
                  ,link: Option[LinkResponse] = None //Link object for call to actions. The link object applies if the asset item is activated (clicked). If there is no link object on the asset, the parent link object on the bid response applies.
                  ,ext: Option[AssetResponseExt] = None //Bidders are encouraged not to use asset.ext for exchanging text assets. Use data.ext with custom type instead.
                  )

case class NativeResponse(
                         ver: Option[VersionInt] = Option(VersionInt(1))
                         ,assets: List[AssetResponse] = List.empty
                         ,link: LinkResponse
                         ,imptrackers: List[Url] = List.empty
                         ,jstracker: Option[Url] = None
                         ,ext: Option[NativeResponseExt] = None
                         ) {
  import RtbResponseJsonFormats._

  def toJsonStr: String = Json.stringify(Json.toJson(this))
}

object NativeResponse {
  import RtbResponseJsonFormats._

  def fromJsonStr(request: String): ValidationNel[FailureResult, NativeResponse] =
    Json.fromJson[NativeResponse](Json.parse(request)).toValidation.leftMap(e => nel(FailureResult(e.toString)))
}
