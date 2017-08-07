package com.gravity.domain.rtb

import com.gravity.domain.rtb.BidRequestFormats._
import com.gravity.test.domainTesting
import com.gravity.utilities.BaseScalaTest
import com.gravity.valueclasses.ValueClassesForUtilities._
import play.api.libs.json.{JsError, JsSuccess, Json}

import scalaz.syntax.std.option._


/**
 * Created by runger on 5/4/15.
 */


class NativeRequestTest extends BaseScalaTest with domainTesting {

  val imgAsset: ImageRequest = ImageRequest(w = Some(500.asWidth), h = Some(300.asHeight))

  val titleAsset: TitleRequest = TitleRequest(30.asSize)

  val assetList: List[AssetRequest] = List(
    AssetRequest(id = 1230.asIntId, title = Some(titleAsset))
    , AssetRequest(id = 1230.asIntId, img = Some(imgAsset))
  )

  val nativeReq: NativeRequest = NativeRequest(layout = Some(Layout.contentWall), adunit = Some(AdUnit.recommendation), plcmtcnt = Some(4.asCount), assets = assetList)

  val native: Native = Native.fromNativeRequest(nativeReq)

  val imps: List[Imp] = List(
    Imp("AUCTION123".asStringId, native = Some(native))
  )

  val device: Device = Device(ip = Some("123.222.111.2".asIPAddressString))

  val site: Option[Site] = Option(Site(id = StringId("SiteId1").some, domain = Some("domain.com".asDomain), page = Some("domain.com/page123.html".asUrl)))

  val geo: Geo = Geo()

  val user: User = User(Some("GRVUSER123".asStringId), Some("PARTNERUSER123".asStringId), None, None, None, None, Some(geo), None, None)

  val request: BidRequest = BidRequest("12345ABC".asStringId, imps, Some(device), site, None, Some(user), Some(RequestType.test), Some(AuctionType.firstPrice)
    , Some(100.asMillis), None, Some(AllImpsType.noOrUnknown), None, List(IABCategory("test")).some, List("ford.com".asDomain).some, None, None)

  def testDeserialize(context: String, testCase: OpenRtbBidRequestTestCases.TestCase): Unit =  {
    println(s"$context, Test Case `${testCase.caseName}`")

    Json.fromJson[BidRequest](Json.parse(testCase.json)) match {
      case JsSuccess(gotObj, _)    =>
        testCase.optBidReq match {
          case Some(wantBidReq) =>
            gotObj should be(wantBidReq)

          case None =>
        }

        testCase.optNatReq match {
          case Some(wantNativeReq) =>
            gotObj.imp.headOption.flatMap(_.native).flatMap(NativeRequest.fromNative(_).toOption) should be(Some(wantNativeReq))

          case None =>
        }

      case gotBad @ JsError(fails) =>
        gotBad.toString should be("")
    }
  }

  test("Test JSON Deserialization of BidRequest Samples from OpenRTB Specification") {
    for {
      testCase <- OpenRtbBidRequestTestCases.examples
    } {
      // Confirm that the JSON String in the TestCase deserializes properly (and into the expected objects, if known).
      testDeserialize("JSON Deserialization", testCase)
    }
  }

  test("Test JSON Round-Trip of BidRequest Samples from OpenRTB Specification") {
    for {
      testCase <- OpenRtbBidRequestTestCases.examples

      if testCase.optBidReq.isDefined

      bidReq = testCase.optBidReq.get
    } {
      // Serialize the testCase's BidRequest/Option[NativeRequest] into a JSON String.
      val jsonStr = testCase.optNatReq match {
        case None =>
          Json.stringify(Json.toJson(bidReq))

        case Some(natReq) =>
          // NOTE: This is kind of brutal, and doesn't support tests of multiple different NativeRequests in multiple Imp's.
          Json.stringify(Json.toJson(bidReq.copy(imp = bidReq.imp.map(thisImp =>
            thisImp.copy(
              native = thisImp.native.map(_.copy(request = Json.stringify(Json.toJson(natReq))
              )))))))
      }

      // Confirm that the JSON String in the TestCase deserializes into the expected objects.
      testDeserialize("JSON Round-Trip", testCase.copy(json = jsonStr))
    }
  }
}

// Samples from OpenRTB API Specification Version 2.3.1 FINAL
object OpenRtbBidRequestTestCases {
  case class TestCase(caseName: String, json: String, optBidReq: Option[BidRequest], optNatReq: Option[NativeRequest])

  val example1: TestCase = TestCase(
    "OpenRTB API Spec 2.3.1 - BidRequest Samples - Example 1 – Simple Banner",

    """{
      |  "id": "80ce30c53c16e6ede735f123ef6e32361bfc7b22",
      |  "at": 1,
      |  "cur": [ "USD" ],
      |  "imp": [
      |    {
      |      "id": "1",
      |      "bidfloor": 0.03,
      |      "banner": { "h": 250, "w": 300, "pos": 0 }
      |    }
      |  ],
      |  "site": {
      |    "id": "102855",
      |    "cat": [ "IAB3-1" ],
      |    "domain": "www.foobar.com",
      |    "page": "http://www.foobar.com/1234.html ",
      |    "publisher": {
      |      "id": "8953",
      |      "name": "foobar.com",
      |      "cat": [ "IAB3-1" ],
      |      "domain": "foobar.com"
      |    }
      |  },
      |  "device": {
      |    "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.13 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2", "ip": "123.145.167.10"
      |  },
      |  "user": { "id": "55816b39711f9b5acf3b90e313ed29e51665623f" }
      |}""".stripMargin,

    BidRequest(
      id  = StringId("80ce30c53c16e6ede735f123ef6e32361bfc7b22"),
      at  = Option(AuctionType.firstPrice),
      cur = Option(List(CurrencyCode("USD"))),
      imp = List(Imp(
        id       = StringId("1"),
        bidfloor = Option(BigDecimal(0.03)),
        banner   = Option(Banner(h = Option(Height(250)), w = Option(Width(300)), pos = Option(0)))
      )),
      site = Option(Site(
        id        = Option(StringId("102855")),
        cat       = Option(List(IABCategory("IAB3-1"))),
        domain    = Option(Domain("www.foobar.com")),
        page      = Option(Url("http://www.foobar.com/1234.html ")),
        publisher = Option(Publisher(
          id         = Option(StringId("8953")),
          name       = Option(Name("foobar.com")),
          cat        = Option(List(IABCategory("IAB3-1"))),
          domain     = Option(Domain("foobar.com"))
        ))
      )),
      device = Option(Device(
        ua = Option(UserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.13 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2")),
        ip = Option(IPAddressString("123.145.167.10"))
      )),
      user = Option(User(id = Option(StringId("55816b39711f9b5acf3b90e313ed29e51665623f"))))
    ).some,

    None  // Not a NativeRequest
  )

  val example2: TestCase = TestCase(
    "OpenRTB API Spec 2.3.1 - BidRequest Samples - Example 2 – Expandable Creative",

    """{
      |  "id": "123456789316e6ede735f123ef6e32361bfc7b22",
      |  "at": 2,
      |  "cur": [
      |    "USD"
      |  ],
      |  "imp": [
      |    {
      |      "id": "1",
      |      "bidfloor": 0.03,
      |      "iframebuster": [
      |        "vendor1.com",
      |        "vendor2.com"
      |      ],
      |      "banner": {
      |        "h": 250,
      |        "w": 300,
      |        "pos": 0,
      |        "battr": [
      |          13
      |        ],
      |        "expdir": [
      |          2,
      |          4
      |        ]
      |      }
      |    }
      |  ],
      |  "site": {
      |    "id": "102855",
      |    "cat": [
      |      "IAB3-1"
      |    ],
      |    "domain": "www.foobar.com",
      |    "page": "http://www.foobar.com/1234.html",
      |    "publisher": {
      |      "id": "8953",
      |      "name": "foobar.com",
      |      "cat": [
      |        "IAB3-1"
      |      ],
      |      "domain": "foobar.com"
      |    }
      |  },
      |  "device": {
      |    "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.13 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2",
      |    "ip": "123.145.167.10"
      |  },
      |  "user": {
      |    "id": "55816b39711f9b5acf3b90e313ed29e51665623f",
      |    "buyeruid": "545678765467876567898765678987654",
      |    "data": [
      |      {
      |        "id": "6",
      |        "name": "Data Provider 1",
      |        "segment": [
      |          {
      |            "id": "12341318394918",
      |            "name": "auto intenders"
      |          },
      |          {
      |            "id": "1234131839491234",
      |            "name": "auto enthusiasts"
      |          },
      |          {
      |            "id": "23423424",
      |            "name": "data-provider1-age",
      |            "value": "30-40"
      |          }
      |        ]
      |      }
      |    ]
      |  }
      |}""".stripMargin,

    None,
    None
  )

  val example3: TestCase = TestCase(
    "OpenRTB API Spec 2.3.1 - BidRequest Samples - Example 3 – Mobile",

    """{
      |  "id": "IxexyLDIIk",
      |  "at": 2,
      |  "bcat": [
      |    "IAB25",
      |    "IAB7-39",
      |    "IAB8-18",
      |    "IAB8-5",
      |    "IAB9-9"
      |  ],
      |  "badv": [
      |    "apple.com",
      |    "go-text.me",
      |    "heywire.com"
      |  ],
      |  "imp": [
      |    {
      |      "id": "1",
      |      "bidfloor": 0.5,
      |      "instl": 0,
      |      "tagid": "agltb3B1Yi1pbmNyDQsSBFNpdGUY7fD0FAw",
      |      "banner": {
      |        "w": 728,
      |        "h": 90,
      |        "pos": 1,
      |        "btype": [
      |          4
      |        ],
      |        "battr": [
      |          14
      |        ],
      |        "api": [
      |          3
      |        ]
      |      }
      |    }
      |  ],
      |  "app": {
      |    "id": "agltb3B1Yi1pbmNyDAsSA0FwcBiJkfIUDA",
      |    "name": "Yahoo Weather",
      |    "cat": [
      |      "IAB15",
      |      "IAB15-10"
      |    ],
      |    "ver": "1.0.2",
      |    "bundle": "com.yahoo.wxapp",
      |    "storeurl": "https://itunes.apple.com/id628677149",
      |    "publisher": {
      |      "id": "agltb3B1Yi1pbmNyDAsSA0FwcBiJkfTUCV",
      |      "name": "yahoo",
      |      "domain": "www.yahoo.com"
      |    }
      |  },
      |  "device": {
      |    "dnt": 0,
      |    "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 6_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3",
      |    "ip": "123.145.167.189",
      |    "ifa": "AA000DFE74168477C70D291f574D344790E0BB11",
      |    "carrier": "VERIZON",
      |    "language": "en",
      |    "make": "Apple",
      |    "model": "iPhone",
      |    "os": "iOS",
      |    "osv": "6.1",
      |    "js": 1,
      |    "connectiontype": 3,
      |    "devicetype": 1,
      |    "geo": {
      |      "lat": 35.012345,
      |      "lon": -115.12345,
      |      "country": "USA",
      |      "metro": "803",
      |      "region": "CA",
      |      "city": "Los Angeles",
      |      "zip": "90049"
      |    }
      |  },
      |  "user": {
      |    "id": "ffffffd5135596709273b3a1a07e466ea2bf4fff",
      |    "yob": 1984,
      |    "gender": "M"
      |  }
      |}""".stripMargin,

    None,
    None
  )

  val example4: TestCase = TestCase(
    "OpenRTB API Spec 2.3.1 - BidRequest Samples - Example 4 – Video",

    """{
      |  "id": "1234567893",
      |  "at": 2,
      |  "tmax": 120,
      |  "imp": [
      |    {
      |      "id": "1",
      |      "bidfloor": 0.03,
      |      "video": {
      |        "w": 640,
      |        "h": 480,
      |        "pos": 1,
      |        "startdelay": 0,
      |        "minduration": 5,
      |        "maxduration": 30,
      |        "maxextended": 30,
      |        "minbitrate": 300,
      |        "maxbitrate": 1500,
      |        "api": [
      |          1,
      |          2
      |        ],
      |        "protocols": [
      |          2,
      |          3
      |        ],
      |        "mimes": [
      |          "video/x-flv",
      |          "video/mp4",
      |          "application/x-shockwave-flash",
      |          "application/javascript"
      |        ],
      |        "linearity": 1,
      |        "boxingallowed": 1,
      |        "playbackmethod": [
      |          1,
      |          3
      |        ],
      |        "delivery": [
      |          2
      |        ],
      |        "battr": [
      |          13,
      |          14
      |        ],
      |        "companionad": [
      |          {
      |            "id": "1234567893-1",
      |            "w": 300,
      |            "h": 250,
      |            "pos": 1,
      |            "battr": [
      |              13,
      |              14
      |            ],
      |            "expdir": [
      |              2,
      |              4
      |            ]
      |          },
      |          {
      |            "id": "1234567893-2",
      |            "w": 728,
      |            "h": 90,
      |            "pos": 1,
      |            "battr": [
      |              13,
      |              14
      |            ]
      |          }
      |        ],
      |        "companiontype": [
      |          1,
      |          2
      |        ]
      |      }
      |    }
      |  ],
      |  "site": {
      |    "id": "1345135123",
      |    "name": "Site ABCD",
      |    "domain": "siteabcd.com",
      |    "cat": [
      |      "IAB2-1",
      |      "IAB2-2"
      |    ],
      |    "page": "http://siteabcd.com/page.htm",
      |    "ref": "http://referringsite.com/referringpage.htm",
      |    "privacypolicy": 1,
      |    "publisher": {
      |      "id": "pub12345",
      |      "name": "Publisher A"
      |    },
      |    "content": {
      |      "id": "1234567",
      |      "series": "All About Cars",
      |      "season": "2",
      |      "episode": 23,
      |      "title": "Car Show",
      |      "cat": [
      |        "IAB2-2"
      |      ],
      |      "keywords": "keyword-a,keyword-b,keyword-c"
      |    }
      |  },
      |  "device": {
      |    "ip": "64.124.253.1",
      |    "ua": "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16",
      |    "os": "OS X",
      |    "flashver": "10.1",
      |    "js": 1
      |  },
      |  "user": {
      |    "id": "456789876567897654678987656789",
      |    "buyeruid": "545678765467876567898765678987654",
      |    "data": [
      |      {
      |        "id": "6",
      |        "name": "Data Provider 1",
      |        "segment": [
      |          {
      |            "id": "12341318394918",
      |            "name": "auto intenders"
      |          },
      |          {
      |            "id": "1234131839491234",
      |            "name": "auto enthusiasts"
      |          }
      |        ]
      |      }
      |    ]
      |  }
      |}""".stripMargin,

    None,
    None
  )

  val example5: TestCase = TestCase(
    "OpenRTB API Spec 2.3.1 - BidRequest Samples - Example 5 – PMP with Direct Deal",

    """{
      |  "id": "80ce30c53c16e6ede735f123ef6e32361bfc7b22",
      |  "at": 1, "cur": [ "USD" ],
      |  "imp": [
      |    {
      |      "id": "1", "bidfloor": 0.03,
      |      "banner": {
      |        "h": 250, "w": 300, "pos": 0
      |      },
      |      "pmp": {
      |        "private_auction": 1,
      |        "deals": [
      |          {
      |            "id":"AB-Agency1-0001",
      |            "at": 1, "bidfloor": 2.5,
      |            "wseat": [ "Agency1" ]
      |          },
      |          {
      |            "id":"XY-Agency2-0001",
      |            "at": 2, "bidfloor": 2,
      |            "wseat": [ "Agency2" ]
      |          }
      |        ]
      |      }
      |    }
      |  ],
      |  "site": {
      |    "id": "102855",
      |    "domain": "www.foobar.com",
      |    "cat": [ "IAB3-1" ],
      |    "page": "http://www.foobar.com/1234.html",
      |    "publisher": {
      |      "id": "8953", "name": "foobar.com",
      |      "cat": [ "IAB3-1" ],
      |      "domain": "foobar.com"
      |    }
      |  },
      |  "device": {
      |    "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.13 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2",
      |    "ip": "123.145.167.10"
      |  },
      |  "user": {
      |    "id": "55816b39711f9b5acf3b90e313ed29e51665623f"
      |  }
      |}""".stripMargin,

    None,
    None
  )

  // Currently, all of Gravity's BidRequests will be Native requests.
  val example6NativeRequest: NativeRequest = NativeRequest(
    layout   = Option(Layout.contentWall),
    adunit   = Option(AdUnit.recommendation),
    plcmtcnt = Option(Count(4)),
    assets   = List(
      AssetRequest(
        id = IntId(1230),
        required = Option(RequiredAsset.required),
        title = Option(TitleRequest(len = Size(30)))
      ),
      AssetRequest(
        id = IntId(1231),
        required = Option(RequiredAsset.required),
        img = Option(ImageRequest(
          w = Option(Width(500)),
          h = Option(Height(300)),
          mimes = Option(List())
        ))
      )
    )
  )

  val example6: TestCase = TestCase(
    "OpenRTB API Spec 2.3.1 - BidRequest Samples - Example 6 – Native Ad",
    """{
      |  "id": "80ce30c53c16e6ede735f123ef6e32361bfc7b22",
      |  "at": 1,
      |  "cur": [
      |    "USD"
      |  ],
      |  "imp": [
      |    {
      |      "id": "1",
      |      "bidfloor": 0.03,
      |      "native": {
      |        "request": "{\"ver\":\"1\",\"layout\":1,\"adunit\":2,\"plcmtcnt\":4,\"seq\":0,\"assets\":[{\"id\":1230,\"required\":1,\"title\":{\"len\":30}},{\"id\":1231,\"required\":1,\"img\":{\"w\":500,\"h\":300,\"mimes\":[]}}]}",
      |        "ver": "1.0",
      |        "api": [
      |          3
      |        ],
      |        "battr": [
      |          13,
      |          14
      |        ]
      |      }
      |    }
      |  ],
      |  "site": {
      |    "id": "102855",
      |    "cat": [
      |      "IAB3-1"
      |    ],
      |    "domain": "www.foobar.com",
      |    "page": "http://www.foobar.com/1234.html ",
      |    "publisher": {
      |      "id": "8953",
      |      "name": "foobar.com",
      |      "cat": [
      |        "IAB3-1"
      |      ],
      |      "domain": "foobar.com"
      |    }
      |  },
      |  "device": {
      |    "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.13 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2",
      |    "ip": "123.145.167.10"
      |  },
      |  "user": {
      |    "id": "55816b39711f9b5acf3b90e313ed29e51665623f"
      |  }
      |}""".stripMargin,

    BidRequest(
      id  = StringId("80ce30c53c16e6ede735f123ef6e32361bfc7b22"),
      at  = Option(AuctionType.firstPrice),
      cur = Option(List(CurrencyCode("USD"))),

      imp = List(Imp(
        id       = StringId("1"),
        bidfloor = Option(BigDecimal(0.03)),
        native   = Option(Native.fromNativeRequest(
          nativeReq = example6NativeRequest,
          ver       = Option(VersionString("1.0")),
          api       = Option(List(APIFramework.mraid1)),
          battr     = Option(List(CreativeAttribute.userInteractive, CreativeAttribute.windowsDialog))
        ))
      )),
      site = Option(Site(
        id        = Option(StringId("102855")),
        cat       = Option(List(IABCategory("IAB3-1"))),
        domain    = Option(Domain("www.foobar.com")),
        page      = Option(Url("http://www.foobar.com/1234.html ")),
        publisher = Option(Publisher(
          id         = Option(StringId("8953")),
          name       = Option(Name("foobar.com")),
          cat        = Option(List(IABCategory("IAB3-1"))),
          domain     = Option(Domain("foobar.com"))
        ))
      )),
      device = Option(Device(
        ua = Option(UserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.13 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2")),
        ip = Option(IPAddressString("123.145.167.10"))
      )),
      user = Option(User(id = Option(StringId("55816b39711f9b5acf3b90e313ed29e51665623f"))))
    ).some,

    example6NativeRequest.some
  )

  val examples: List[TestCase] = List(
    example1,
    example2,
    example3,
    example4,
    example5,
    example6    // A NativeRequest, such as is used by Gravity.
  )
}
