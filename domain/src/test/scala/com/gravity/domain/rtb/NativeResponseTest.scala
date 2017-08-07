package com.gravity.domain.rtb

import com.gravity.utilities.BaseScalaTest
import com.gravity.valueclasses.ValueClassesForUtilities._
import play.api.libs.json.Json

import scalaz.syntax.std.option._

/**
 * Created by runger on 5/7/15.
 */

class NativeResponseTest extends BaseScalaTest {

  import RtbResponseJsonFormats._

  val linkResp: LinkResponse = LinkResponse("http://landingpage.com".asUrl)
  val imgResp: ImageResponse = ImageResponse(
    url = Url("http://thumby.grvcdn.com/t/600x600/North/?url=http://upload.wikimedia.org/wikipedia/commons/5/59/500_x_300_Ramosmania_rodriguesii_%28Rubiaceae%29.jpg")
    ,w = Width(600).some
    ,h = Height(600).some
  )
  val nativeDataResp: NativeDataResponse = NativeDataResponse(
    label = "stars".asLabel.some
    ,value = Text("4")
  )

  val assetResp: AssetResponse = AssetResponse(
    id = 42.asIntId
    ,title = TitleResponse(Text("A Creative Title")).some
    ,img = imgResp.some
    ,data = nativeDataResp.some
  )

  val nativeResp: NativeResponse = NativeResponse(
    link   = LinkResponse(url = Url("http://sample.com/clickurl.html")),
    assets = List(assetResp)
  )

  val bids: List[Bid] = List(
    Bid("INTELCAMP123".asStringId, "IMPID123".asStringId
      ,None //Not in use due to CPC bid
      ,Some("AD123".asStringId),Some("http://partner.com/winNotification".asUrl), Some(nativeResp.toJsonStr)))

  val seatBids: List[SeatBid] = List(SeatBid(bids, Some("SEAT123".asStringId)))

  val bidResponse: BidResponse = BidResponse("BIDREQ123".asStringId, seatBids, Some("BIDRESP123".asStringId), None, None, None, None)

  val noBidResponse: BidResponse = BidResponse("BIDREQ123".asStringId, List.empty, Some("BIDRESP123".asStringId), None, None, Some(NoBidReason.unmatchedUser), None)

//  val fullNativeResp = bidResponse.copy(seatbid = seatBids.copy())

  test("link response"){
    val expected = "{\"url\":\"http://landingpage.com\"}"
    val linkJ = Json.stringify(Json.toJson(linkResp))
    println(linkJ)
    linkJ should be(expected)
  }

  test("asset response"){
    val expected = Json.parse("""{"id":42,"required":0,"title":{"text":"A Creative Title"},"img":{"url":"http://thumby.grvcdn.com/t/600x600/North/?url=http://upload.wikimedia.org/wikipedia/commons/5/59/500_x_300_Ramosmania_rodriguesii_%28Rubiaceae%29.jpg","w":600,"h":600},"data":{"label":"stars","value":"4"}} """)
    val assetJ = Json.toJson(assetResp)
    println(assetJ)
    assetJ should be(expected)
  }

  test("native response"){
    val expected = Json.parse("""{"id":"BIDREQ123","seatbid":[{"bid":[{"id":"INTELCAMP123","impid":"IMPID123","adid":"AD123","nurl":"http://partner.com/winNotification","adm":"{\"ver\":1,\"assets\":[{\"id\":42,\"required\":0,\"title\":{\"text\":\"A Creative Title\"},\"img\":{\"url\":\"http://thumby.grvcdn.com/t/600x600/North/?url=http://upload.wikimedia.org/wikipedia/commons/5/59/500_x_300_Ramosmania_rodriguesii_%28Rubiaceae%29.jpg\",\"w\":600,\"h\":600},\"data\":{\"label\":\"stars\",\"value\":\"4\"}}],\"link\":{\"url\":\"http://sample.com/clickurl.html\"}}"}],"seat":"SEAT123","group":0}],"bidid":"BIDRESP123"} """)
    val nativeRespJ = Json.toJson(bidResponse)
    println(nativeRespJ)
    nativeRespJ should be(expected)
  }

  ignore("no bid response"){}
}
