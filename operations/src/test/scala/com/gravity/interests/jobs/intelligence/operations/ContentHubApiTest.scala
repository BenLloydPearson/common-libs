package com.gravity.interests.jobs.intelligence.operations

import java.text.MessageFormat

import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.operations.ContentHubAndRelegenceApiFormats._
import com.gravity.interests.jobs.intelligence.operations.ContentHubAndRelegenceApis._
import com.gravity.interests.jobs.intelligence.operations.ContentHubApi._
import com.gravity.interests.jobs.intelligence.operations.ContentHubApiFormats._
import com.gravity.interests.jobs.intelligence.operations.RelegenceApi._
import com.gravity.interests.jobs.intelligence.operations.RelegenceApiFormats._
import com.gravity.test.{SerializationTesting, operationsTesting}
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.grvstrings._
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import play.api.data.validation.ValidationError
import play.api.libs.json._

import scala.io.Source
import scalaz.syntax.std.option._

class ContentHubApiTest extends BaseScalaTest with operationsTesting with SerializationTesting {
  implicit val dateTimeFormatter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC)

  def valErrToString(valErr: ValidationError): String =
    new MessageFormat(valErr.message).format(valErr.args.toArray)

  def jsErrorToString(jsError: JsError): String = {
    val errHeader = s"${jsError.errors.size} errors:\n"

    val errDetail = (for {
      fail <- jsError.errors
      (jsPath, valErrs) = fail
    } yield {
      s"""${jsPath.toJsonString}: ${valErrs.map{valErrToString}.mkString("; ")}"""
    }).sorted.mkString("  ", "\n  ", "\n")

    (errHeader + errDetail)
  }

  // Confirm that the JSON String in the TestCase deserializes properly (and into the expected objects, if known).
  def testJsonDeserialize[T](context: String, testCase: TestCase[T])(implicit jsFmt: Format[T]): Unit =  {
    testCase.optJsonStr.foreach { jsonStr =>
      println(s"$context, Test Case `${testCase.caseName}`")

      val jsValue = Json.parse(jsonStr)

      Json.fromJson(jsValue)(jsFmt) match {
        case JsSuccess(gotObj, _) =>
          testCase.optObject.foreach { wantObj =>
            gotObj should be(wantObj)
            testCase.validator(gotObj, jsValue)
          }

        case jsErr: JsError =>
          jsErrorToString(jsErr) should be("")
      }
    }
  }

  def testJsonDeserialize[T](testCases: Seq[TestCase[T]])(implicit jsFmt: Format[T]): Unit =  {
    for {
      testCase <- testCases
    } {
      testJsonDeserialize("JSON Deserialization", testCase)
    }
  }

  // Confirm that the optObject in the TestCase, when serialized as JSON, deserializes back into the original object.
  def testJsonRoundTrip[T](context: String, testCase: TestCase[T])(implicit jsFmt: Format[T]): Unit = {
    testCase.optObject.foreach { obj =>
      println(s"$context, Test Case `${testCase.caseName}`")

      // Serialize the testCase's object into a JSON String.
      val jsonStr = Json.stringify(Json.toJson(obj)(jsFmt))

      // Confirm that jsonStr deserializes into the expected object.
      testJsonDeserialize("JSON Round-Trip", testCase.copy(optJsonStr = jsonStr.some))
    }
  }

  def testJsonRoundTrip[T](testCases: Seq[TestCase[T]])(implicit jsFmt: Format[T]): Unit =  {
    for {
      testCase <- testCases
    } {
      testJsonRoundTrip("JSON Round-Trip", testCase)
    }
  }

  test("Test ByteConverter Round-Type of sampleArtStoryInfos") {
    for { sample <- TestCases.sampleArtStoryInfos } {
      deepCloneWithComplexByteConverter(sample) should be(sample)
    }
  }

  test("Test JSON Deserialization of tcChGetChannelItemsResponses") {
    testJsonDeserialize(TestCases.tcChGetChannelItemsResponses)
  }

  ignore("Test JSON Round-Trip of tcChGetChannelItemsResponses") {
    testJsonRoundTrip(TestCases.tcChGetChannelItemsResponses)
  }

  // tcRgHeirarchyByNodeTypeResponses
  // tcRgHeirarchyBySubjectResponses

  test("Test JSON Deserialization of tcRgHeirarchyByNodeTypeResponses") {
    testJsonDeserialize(TestCases.tcRgHeirarchyByNodeTypeResponses)
  }

  ignore("Test JSON Round-Trip of tcRgHeirarchyByNodeTypeResponses") {
    testJsonRoundTrip(TestCases.tcRgHeirarchyByNodeTypeResponses)
  }

  test("Test JSON Deserialization of tcRgHeirarchyBySubjectResponses") {
    testJsonDeserialize(TestCases.tcRgHeirarchyBySubjectResponses)
  }

  ignore("Test JSON Round-Trip of tcRgHeirarchyBySubjectResponses") {
    testJsonRoundTrip(TestCases.tcRgHeirarchyBySubjectResponses)
  }

  test("Test JSON Deserialization of tcRgMapperResponses") {
    testJsonDeserialize(TestCases.tcRgMapperResponses)
  }

  ignore("Test JSON Round-Trip of tcRgMapperResponses") {
    testJsonRoundTrip(TestCases.tcRgMapperResponses)
  }

  test("Test JSON Deserialization of tcRgAutoCompleteResponses") {
    testJsonDeserialize(TestCases.tcRgAutoCompleteResponses)
  }

  ignore("Test JSON Round-Trip of tcRgAutoCompleteResponses") {
    testJsonRoundTrip(TestCases.tcRgAutoCompleteResponses)
  }

  test("Test JSON Deserialization of tcRgByNodeTypeResponses") {
    testJsonDeserialize(TestCases.tcRgByNodeTypeResponses)
  }

  ignore("Test JSON Round-Trip of tcRgByNodeTypeResponses") {
    testJsonRoundTrip(TestCases.tcRgByNodeTypeResponses)
  }

  test("Test JSON Deserialization of tcRgByRelegenceIdResponses") {
    testJsonDeserialize(TestCases.tcRgByRelegenceIdResponses)
  }

  ignore("Test JSON Round-Trip of tcRgByRelegenceIdResponses") {
    testJsonRoundTrip(TestCases.tcRgByRelegenceIdResponses)
  }

  test("Test JSON Deserialization of tcRgRelatedResponses") {
    testJsonDeserialize(TestCases.tcRgRelatedResponses)
  }

  ignore("Test JSON Round-Trip of tcRgRelatedResponses") {
    testJsonRoundTrip(TestCases.tcRgRelatedResponses)
  }

  test("Test JSON Deserialization of tcRgTrendingResponses") {
    testJsonDeserialize(TestCases.tcRgTrendingResponses)
  }

  ignore("Test JSON Round-Trip of tcRgTrendingResponses") {
    testJsonRoundTrip(TestCases.tcRgTrendingResponses)
  }

  test("Test JSON Deserialization of tcRgTagArticleResponses") {
    testJsonDeserialize(TestCases.tcRgTagArticleResponses)
  }

  ignore("Test JSON Round-Trip of tcRgTagArticleResponses") {
    testJsonRoundTrip(TestCases.tcRgTagArticleResponses)
  }

  test("Test JSON Deserialization of tcRgStoryResponses") {
    testJsonDeserialize(TestCases.tcRgStoryResponses)
  }

  ignore("Test JSON Round-Trip of tcRgStoryResponses") {
    testJsonRoundTrip(TestCases.tcRgStoryResponses)
  }

  test("Test JSON Deserialization of tcRgStoriesResponses") {
    testJsonDeserialize(TestCases.tcRgStoriesResponses)
  }

  ignore("Test JSON Round-Trip of tcRgStoriesResponses") {
    testJsonRoundTrip(TestCases.tcRgStoriesResponses)
  }

  test("Test JSON Deserialization of tcRgRelatedArticlesResponses") {
    testJsonDeserialize(TestCases.tcRgRelatedArticlesResponses)
  }

  ignore("Test JSON Round-Trip of tcRgRelatedArticlesResponses") {
    testJsonRoundTrip(TestCases.tcRgRelatedArticlesResponses)
  }

  test("Test JSON Deserialization of tcChRgRelegences") {
    testJsonDeserialize(TestCases.tcChRgRelegences)
  }

  test("Test JSON Round-Trip of tcChRgRelegences") {
    testJsonRoundTrip(TestCases.tcChRgRelegences)
  }
  
  test("Test JSON Deserialization of tcCRAltSizes") {
    testJsonDeserialize(TestCases.tcCRAltSizes)
  }

  test("Test JSON Round-Trip of tcCRAltSizes") {
    testJsonRoundTrip(TestCases.tcCRAltSizes)
  }

  test("Test JSON Deserialization of tcCRMediaImages") {
    testJsonDeserialize(TestCases.tcCRMediaImages)
  }

  test("Test JSON Round-Trip of tcCRMediaImages") {
    testJsonRoundTrip(TestCases.tcCRMediaImages)
  }

  test("Test JSON Deserialization of tcChSearchSourcesAutoCompleteResponses") {
    testJsonDeserialize(TestCases.tcChSearchSourcesAutoCompleteResponses)
  }

  ignore("Test JSON Round-Trip of tcChSearchSourcesAutoCompleteResponses") {
    testJsonRoundTrip(TestCases.tcChSearchSourcesAutoCompleteResponses)
  }

  test("Test JSON Deserialization of tcChGetSourcesResponses") {
    testJsonDeserialize(TestCases.tcChGetSourcesResponses)
  }

  ignore("Test JSON Round-Trip of tcChGetSourcesResponses") {
    testJsonRoundTrip(TestCases.tcChGetSourcesResponses)
  }

  test("Test JSON Deserialization of tcChSearchLocalNewsResponses") {
    testJsonDeserialize(TestCases.tcChSearchLocalNewsResponses)
  }

  ignore("Test JSON Round-Trip of tcChSearchLocalNewsResponses") {
    testJsonRoundTrip(TestCases.tcChSearchLocalNewsResponses)
  }

  test("Test JSON Deserialization of tcChSearchAggregatedArticlesResponses") {
    testJsonDeserialize(TestCases.tcChSearchAggregatedArticlesResponses)
  }

  ignore("Test JSON Round-Trip of tcChSearchAggregatedArticlesResponses") {
    testJsonRoundTrip(TestCases.tcChSearchAggregatedArticlesResponses)
  }

  test("Test JSON Deserialization of tcChSearchArticlesResponses") {
    testJsonDeserialize(TestCases.tcChSearchArticlesResponses)
  }

  ignore("Test JSON Round-Trip of tcChSearchArticlesResponses") {
    testJsonRoundTrip(TestCases.tcChSearchArticlesResponses)
  }

  test("Test JSON Deserialization of tcChGetArticleResponses") {
    testJsonDeserialize(TestCases.tcChGetArticleResponses)
  }

  test("Test JSON Round-Trip of tcChGetArticleResponses") {
    testJsonRoundTrip(TestCases.tcChGetArticleResponses)
  }

  test("Print ContentHub Sources By group_id") {
    Json.fromJson[ChGetSourcesResponse](Json.parse(ChTestStringLoader.getTestString("ChGetSources01Rsp.json"))) match {
      case JsSuccess(response, _) =>
        for ((group_id, sources) <- response.data.sources.groupBy(_.group_id).toSeq.sortBy(_._1.tryToInt.getOrElse(-1))) {
          println(s"""group_id "$group_id":""")
          sources.sortBy(_.title).foreach { source =>
            println(s"""  id="${source.id}", ${source.title}""")
          }
          println()
        }

        println(s"${response.data.sources.size} Sources")

      case jsErr: JsError =>
        jsErrorToString(jsErr) should be("")
    }
  }

//  test("Print ContentHub Sources By platform") {
//    Json.fromJson[ChGetSourcesResponse](Json.parse(ChTestStringLoader.getTestString("ChGetSources01Rsp.json"))) match {
//      case JsSuccess(response, _) =>
//        for ((platform, sources) <- response.data.sources.groupBy(_.platform).toSeq.sortBy(_._1.tryToInt.getOrElse(-1))) {
//          println(s"""platform "$platform":""")
//          sources.sortBy(_.title).foreach { source =>
//            println(s"""  id="${source.id}", ${source.title}""")
//          }
//          println()
//        }
//
//        println(s"${response.data.sources.size} Sources")
//
//      case jsErr: JsError =>
//        jsErrorToString(jsErr) should be("")
//    }
//  }

  test("Print NodeType Heirarchy") {
    var numEntries = 0
    var maxId      = 0

    Json.fromJson[RgHeirarchyByNodeTypeResponse](Json.parse(ChTestStringLoader.getTestString("RgTaxoBrowserHeirByNodeTypeRsp.json"))) match {
      case JsSuccess(response, _) =>
        def printNode(node: RgIdNameNode, indent: String): Unit = {
          if (indent == "")
            println()

          println(s"$indent${node.name} (${node.id})")
          printNodes(node.children, indent + "  ")

          numEntries += 1
          maxId = Math.max(maxId, node.id)
        }

        def printNodes(nodes: Seq[RgIdNameNode], indent: String): Unit = {
          nodes.foreach { node =>
            printNode(node, indent)
          }
        }

        printNodes(response.data, "")
        println()
        println(s"$numEntries Entries, Max ID=$maxId")

      case jsErr: JsError =>
        jsErrorToString(jsErr) should be("")
    }
  }

  test("Print Subject Heirarchy") {
    var numEntries = 0
    var maxId = 0

    Json.fromJson[RgHeirarchyBySubjectResponse](Json.parse(ChTestStringLoader.getTestString("RgTaxoBrowserHeirSubjectRsp.json"))) match {
      case JsSuccess(response, _) =>
        def printNode(node: RgTaggableIdNameNode, indent: String): Unit = {
          if (indent == "")
            println()

          val taggableStr = if (node.isTaggable.getOrElse(false)) "" else "--- Not Taggable ---"

          println(s"$indent${node.name} (${node.id}) ${taggableStr}")
          printNodes(node.children, indent + "  ")

          numEntries += 1
          maxId = Math.max(maxId, node.id)
        }

        def printNodes(nodes: Seq[RgTaggableIdNameNode], indent: String): Unit = {
          nodes.foreach { node =>
            printNode(node, indent)
          }
        }

        printNodes(response.data, "")
        println()
        println(s"$numEntries Entries, Max ID=$maxId")

      case jsErr: JsError =>
        jsErrorToString(jsErr) should be("")
    }
  }
}

case class TestCase[T](caseName: String, optJsonStr: Option[String], optObject: Option[T], validator: (T, JsValue) => Unit = (_: T, _: JsValue) => {})

object TestCases {
  val sampleArtStoryInfos =
    Seq(
      ArtStoryInfo(Int.MaxValue.asInstanceOf[Long] + 1L, System.currentTimeMillis.some, 1467319018548L, 1467319991370L, 1467319582720L,
        "Serial's Adnan Syed is granted new trial", 1441.7, 21496, 100641, 34029, 1234, 3587, 860, 0.69512194,
        Map(3460263L -> HbRgTagRef("California State University", 0.9146), 3460264L -> HbRgTagRef("California State Universities", 0.9147)),
        Map(979432L -> HbRgTagRef("Arts and Entertainment", 0.8))
      ),
      ArtStoryInfo(Int.MaxValue.asInstanceOf[Long] + 2L, None, 1467319018548L, 1467319991370L, 1467319582720L,
        "Serial's Adnan Syed is granted new trial", 1441.7, 21496, 100641, 34029, 1234, 3587, 860, 0.69512194,
        Map(),
        Map()
      )
    )

  val tcChRgRelegences = Seq(
    TestCase[ChRgRelegence]("Empty ChRelegence", "{}".some, ChRgRelegence.empty.some),

    TestCase[ChRgRelegence](
      "ChRelegence Captured 06/2016",
      """|{
         |  "topic": {
         |    "heat": 0,
         |    "id": 746418845613735936,
         |    "document_count": 3,
         |    "created": "2016-06-24T19:04:56+00:00"
         |  },
         |  "duplicate": [
         |    {
         |      "type": "Partial",
         |      "original_doc_id": 746453038540439552
         |    }
         |  ],
         |  "categories": [
         |    {
         |      "name": "U.S.",
         |      "id": 146
         |    }
         |  ],
         |  "tags": [
         |    {
         |      "hits": [
         |        {
         |          "field": "Body",
         |          "length": 10,
         |          "offset": 0
         |        }
         |      ],
         |      "name": "Duarte, California",
         |      "node_types": [
         |        {
         |          "name": "location",
         |          "id": 30
         |        },
         |        {
         |          "name": "Other",
         |          "id": 48
         |        },
         |        {
         |          "name": "city",
         |          "id": 33
         |        }
         |      ],
         |      "in_headline": false,
         |      "freebase_mid": "m.0r06n",
         |      "score": 1,
         |      "disambiguator": "Location",
         |      "type": "Entity",
         |      "id": 3485827
         |    },
         |    {
         |      "hits": [
         |        {
         |          "field": "Body",
         |          "length": 13,
         |          "offset": 245
         |        },
         |        {
         |          "field": "Body",
         |          "length": 13,
         |          "offset": 1453
         |        }
         |      ],
         |      "name": "Camp W.G. Williams",
         |      "node_types": [
         |        {
         |          "name": "location",
         |          "id": 30
         |        },
         |        {
         |          "name": "Other",
         |          "id": 48
         |        }
         |      ],
         |      "in_headline": false,
         |      "freebase_mid": "m.04gpjyx",
         |      "score": 0.9583,
         |      "disambiguator": "Location",
         |      "type": "Entity",
         |      "id": 4157770
         |    },
         |    {
         |      "name": "U.S. News",
         |      "score": 1,
         |      "most_granular": true,
         |      "disambiguator": "News and Politics",
         |      "type": "Subject",
         |      "id": 981465
         |    },
         |    {
         |      "name": "News",
         |      "score": 1,
         |      "most_granular": false,
         |      "disambiguator": "News and Politics",
         |      "type": "Subject",
         |      "id": 981523
         |    },
         |    {
         |      "name": "News and Politics",
         |      "score": 1,
         |      "most_granular": false,
         |      "disambiguator": "Subject",
         |      "type": "Subject",
         |      "id": 981525
         |    }
         |  ]
         |}""".stripMargin.some,

      None
    )
  )

  val tcCRAltSizes = Seq(
    TestCase[Seq[CRAltSize]](
      "Problematic alternate_sizes captured 7/20/16 (and munged to be a better test)",
      """[
        |  {
        |    "url": "https://tctechcrunch2011.files.wordpress.com/2016/07/image-1.jpg?w=100",
        |    "width": 100,
        |    "height": 150
        |  },
        |  {
        |    "url": "https://tctechcrunch2011.files.wordpress.com/2016/07/image-1.jpg?w=200",
        |    "width": "200",
        |    "height": 300
        |  },
        |  {
        |    "url": "https://tctechcrunch2011.files.wordpress.com/2016/07/image-1.jpg?w=400",
        |    "width": 400,
        |    "height": "600"
        |  }
        |]
        |""".stripMargin.some,

      Seq(
        CRAltSize("https://tctechcrunch2011.files.wordpress.com/2016/07/image-1.jpg?w=100", width = 100.some, height = 150.some, None, None),
        CRAltSize("https://tctechcrunch2011.files.wordpress.com/2016/07/image-1.jpg?w=200", width = 200.some, height = 300.some, None, None),
        CRAltSize("https://tctechcrunch2011.files.wordpress.com/2016/07/image-1.jpg?w=400", width = 400.some, height = 600.some, None, None)
      ).some
    )
  )
  
  val tcCRMediaImages = Seq(
    TestCase[CRMediaImage](
      "CRMediaImage Captured 06/25/16",
      """|{
         |            "extracted_from_html": null,
         |            "original_markup": null,
         |            "tags": [],
         |            "url": "https://tctechcrunch2011.files.wordpress.com/2015/05/instant-articles-ad.png",
         |            "anchor_url": null,
         |            "height": 713,
         |            "credit": "",
         |            "media_medium": "image",
         |            "headline": null,
         |            "alternate_sizes": [
         |              {
         |                "url": "https://tctechcrunch2011.files.wordpress.com/2015/05/instant-articles-ad.png?w=150",
         |                "width": 150,
         |                "height": 104
         |              },
         |              {
         |                "url": "https://tctechcrunch2011.files.wordpress.com/2015/05/instant-articles-ad.png?w=300",
         |                "width": 300,
         |                "height": 209
         |              },
         |              {
         |                "url": "https://tctechcrunch2011.files.wordpress.com/2015/05/instant-articles-ad.png?w=680",
         |                "width": 680,
         |                "height": 474
         |              }
         |            ],
         |            "provider": "TechCrunch",
         |            "title": "Instant Articles Ad",
         |            "caption": "National Geographic will run ads inside Instant Articles promoting its paid subscriptions ",
         |            "width": 1024,
         |            "provider_asset_id": "1157628"
         |          }""".stripMargin.some,

      CRMediaImage(
        tags = Nil.some,
        url = "https://tctechcrunch2011.files.wordpress.com/2015/05/instant-articles-ad.png".some,
        height = 713.some,
        credit = "".some,
        media_medium = "image".some,
        alternate_sizes = Seq(
          CRAltSize(url = "https://tctechcrunch2011.files.wordpress.com/2015/05/instant-articles-ad.png?w=150", width = 150.some, height = 104.some, None, None),
          CRAltSize(url = "https://tctechcrunch2011.files.wordpress.com/2015/05/instant-articles-ad.png?w=300", width = 300.some, height = 209.some, None, None),
          CRAltSize(url = "https://tctechcrunch2011.files.wordpress.com/2015/05/instant-articles-ad.png?w=680", width = 680.some, height = 474.some, None, None)
        ).some,
        provider = "TechCrunch".some,
        title = "Instant Articles Ad".some,
        caption = "National Geographic will run ads inside Instant Articles promoting its paid subscriptions ".some,
        width = 1024.some,
        provider_asset_id = "1157628".some
      ).some
    )
  )

  val tcRgHeirarchyByNodeTypeResponses = Seq[TestCase[RgHeirarchyByNodeTypeResponse]](
    TestCase[RgHeirarchyByNodeTypeResponse](
      "RgHeirarchyByNodeTypeResponse Captured 07/01/16",

      ChTestStringLoader.getTestString("RgTaxoBrowserHeirByNodeTypeRsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcRgHeirarchyBySubjectResponses = Seq[TestCase[RgHeirarchyBySubjectResponse]](
    TestCase[RgHeirarchyBySubjectResponse](
      "RgHeirarchyBySubjectResponse Captured 07/01/16",

      ChTestStringLoader.getTestString("RgTaxoBrowserHeirSubjectRsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcRgRelatedArticlesResponses = Seq[TestCase[RgRelatedArticlesResponse]](
    TestCase[RgRelatedArticlesResponse](
      "RgRelatedArticlesResponse Captured 07/06/16",

      ChTestStringLoader.getTestString("RgRelatedArticles01Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcRgAutoCompleteResponses = Seq[TestCase[RgAutoCompleteResponse]](
    TestCase[RgAutoCompleteResponse](
      "RgAutoCompleteResponse Captured 07/06/16",

      ChTestStringLoader.getTestString("RgAutoComplete01Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    ),

    TestCase[RgAutoCompleteResponse](
      "RgAutoCompleteResponse Captured 07/06/16",

      ChTestStringLoader.getTestString("RgAutoComplete02Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcRgMapperResponses = Seq[TestCase[RgMapperResponse]](
    TestCase[RgMapperResponse](
      "RgMapperResponse Captured 07/01/16",

      ChTestStringLoader.getTestString("RgTaxoBrowserMapper01Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcRgByRelegenceIdResponses = Seq[TestCase[RgByRelegenceIdResponse]](
    TestCase[RgByRelegenceIdResponse](
      "RgRelatedResponse Captured 07/01/16",

      ChTestStringLoader.getTestString("RgTaxoBrowserById01Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    ),

    TestCase[RgByRelegenceIdResponse](
      "RgRelatedResponse Captured 07/06/16",

      ChTestStringLoader.getTestString("RgTaxoBrowserById02Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    ),

    TestCase[RgByRelegenceIdResponse](
      "RgRelatedResponse Captured 07/06/16",

      ChTestStringLoader.getTestString("RgTaxoBrowserById03Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcRgByNodeTypeResponses = Seq[TestCase[RgByNodeTypeResponse]](
    TestCase[RgByNodeTypeResponse](
      "RgRelatedResponse Captured 07/01/16",

      ChTestStringLoader.getTestString("RgTaxoBrowserByNodeType01Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcRgRelatedResponses = Seq[TestCase[RgRelatedTagsResponse]](
    TestCase[RgRelatedTagsResponse](
      "RgRelatedResponse Captured 07/01/16",

      ChTestStringLoader.getTestString("RgTaxoBrowserRelatedToEntity01Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    ),

    TestCase[RgRelatedTagsResponse](
      "RgRelatedResponse Captured 07/01/16",

      ChTestStringLoader.getTestString("RgTaxoBrowserRelatedToEntity02Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    ),

    TestCase[RgRelatedTagsResponse](
      "RgRelatedResponse Captured 07/01/16",

      ChTestStringLoader.getTestString("RgTaxoBrowserRelatedToSubject01Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    ),

    TestCase[RgRelatedTagsResponse](
      "RgRelatedResponse Captured 07/01/16",

      ChTestStringLoader.getTestString("RgTaxoBrowserRelatedToSubject02Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcRgTrendingResponses = Seq[TestCase[RgTrendingTopicsResponse]](
    TestCase[RgTrendingTopicsResponse](
      "RgTrendingResponse Captured 07/01/16",

      ChTestStringLoader.getTestString("RgTrendingTop10Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcRgTagArticleResponses = Seq[TestCase[RgTagArticleResponse]](
    TestCase[RgTagArticleResponse](
      "RgTagArticleResponse Captured 06/30/16",

      ChTestStringLoader.getTestString("RgTagArticleFromUrl01Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcRgStoryResponses = Seq[TestCase[RgStory]](
    TestCase[RgStory](
      "RgStory Captured 06/30/16",

      ChTestStringLoader.getTestString("RgStory6045894Top10ArticlesRsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    ),

    TestCase[RgStory](
      "RgStory Captured 06/30/16",

      ChTestStringLoader.getTestString("RgStory6045894Top100ArticlesRsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcRgStoriesResponses = Seq[TestCase[RgStoriesResponse]](
    TestCase[RgStoriesResponse](
      "RgStoriesResponse Captured 06/30/16",

      ChTestStringLoader.getTestString("RgStoriesTop10StoriesRsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcChGetChannelItemsResponses = Seq[TestCase[ChGetChannelItemsResponse]](
    TestCase[ChGetChannelItemsResponse](
      "ChGetChannelItems01Rsp Captured 10/16/16",

      ChTestStringLoader.getTestString("ChGetChannelItems01Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    ),

    TestCase[ChGetChannelItemsResponse](
      "ChGetChannelItems02Rsp Captured 10/19/16",

      ChTestStringLoader.getTestString("ChGetChannelItems02Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcChSearchSourcesAutoCompleteResponses = Seq[TestCase[ChSearchSourcesAutoCompleteResponse]](
    TestCase[ChSearchSourcesAutoCompleteResponse](
      "SearchSourcesAutoCompleteRsp Captured 06/25/16",

      ChTestStringLoader.getTestString("ChSearchSourcesAutoComplete01Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcChGetSourcesResponses = Seq[TestCase[ChGetSourcesResponse]](
    TestCase[ChGetSourcesResponse](
      "GetSources90045Rsp Captured 06/25/16",

      ChTestStringLoader.getTestString("ChGetSources01Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcChSearchLocalNewsResponses = Seq[TestCase[ChSearchLocalNewsResponse]](
    TestCase[ChSearchLocalNewsResponse](
      "SearchLocalNews90045Rsp Captured 06/25/16",

      ChTestStringLoader.getTestString("ChSearchLocalNews90045Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcChSearchAggregatedArticlesResponses = Seq[TestCase[ChSearchAggregatedArticlesResponse]](
    TestCase[ChSearchAggregatedArticlesResponse](
      "SearchAggregatedArticlesResponse Captured 06/23/16",

      ChTestStringLoader.getTestString("ChSearchAggregatedArticles01_SkiRsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcChSearchArticlesResponses = Seq[TestCase[ChSearchArticlesResponse]](
    TestCase[ChSearchArticlesResponse](
      "SearchArticlesResponse Captured 06/25/16",
      ChTestStringLoader.getTestString("ChSearchArticles01_SkiRsp.json").some,
      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    ),

    TestCase[ChSearchArticlesResponse](
      "SearchArticlesResponse Captured 06/26/16",
      ChTestStringLoader.getTestString("ChSearchArticles02_DefaultRsp.json").some,
      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    )
  )

  val tcChGetArticleResponses = Seq[TestCase[ChGetArticleResponse]](
    TestCase[ChGetArticleResponse](
      "GetArticleResponse Captured 06/24/16",

      ChTestStringLoader.getTestString("ChGetArticle01Rsp.json").some,

      None // This test does not specify an expected built object -- just see if the JSON can be parsed.
    ),

    TestCase(
      "GetArticleResponse Round-Trip #1",

      None,

      ChArticle(
        _score = None,
        author = CRAuthor("Eliot Nelson".some, `type` = "2".some, id = "275955".some).some,
        categories = Nil,
        content = "On dit que les sourcils façonnent le visage: raison de plus pour en prendre bien soin! Voici les tendances de la saison et quelques informations pour être au poil.\r\n\r\n<strong>L’arche parfaite (Défilé Carolina Herrera) </strong> \r\nCeux-ci sont obtenus avec beaucoup de précision: on suit la ligne naturelle de notre arcade sourcilière et on enlève tout ce qui l’entrave. Pour ajouter de la définition, on utilise un gel teinté d’une couleur qui ressemble à notre poil naturel.\r\n\r\n<img alt=\"carolina herrera\" src=\"http://i.huffpost.com/gen/3739358/thumbs/o-CAROLINA-HERRERA-570.jpg?7\" />\r\n\r\n<strong>Photo: Défilé Carolina Herrera</strong>\r\n\r\n<strong>Sourcils brossés (Défilé DKNY)</strong>\r\nLe look est frappant et très facile à obtenir: sur des sourcils ayant poussé librement dans les dernières semaines (on cache les ciseaux!), on brosse vers le haut. On obtient la tenue à l’aide d’un gel transparent pour les sourcils.\r\n\r\n<img alt=\"dkny\" src=\"http://i.huffpost.com/gen/3739458/thumbs/o-DKNY-570.jpg?5\" />\r\n\r\n<strong>Photo: Défilé DKNY</strong>\r\n\r\n<strong>Sourcils naturels (Collection Nicole Miller)</strong>\r\nHyper simple, mais qui demande tout de même du travail: on maintient notre ligne et on coupe les poils de façon régulière. Nul besoin de les placer ou d’exagérer la couleur, car on veut l’effet parfaitement naturel.\r\n\r\n<img alt=\"nicole miller\" src=\"http://i.huffpost.com/gen/3739510/thumbs/o-NICOLE-MILLER-570.jpg?7\" />\r\n\r\n<strong>Photo: Collection Nicole Miller</strong>\r\n\r\n\r\n<strong>Les tendances sourcils de l'automne/hiver 2015-2016</strong>\r\n\r\n<!--HH--236SLIDEEXPAND--467292--HH-->\r\n<br>\r\n\r\n<strong>Sourcils fournis (CollectionTory Burch)</strong>\r\nSur des sourcils bien entretenus, on comble les endroits parsemés à l’aide d’un produit d’un ton plus foncé que notre couleur naturelle. On repasse ensuite partout pour uniformiser la couleur.\r\n\r\n<img alt=\"tory burch\" src=\"http://i.huffpost.com/gen/3739692/thumbs/o-TORY-BURCH-570.jpg?3\" />\r\n\r\n<strong>Photo: Collection Tory Burch</strong>\r\n\r\n<strong>Fournis et brossés (Collection: Cushnie et Ochs)</strong>\r\nLe nec plus ultra de la saison: l’union du sourcil brossé et du sourcil fourni. On commence par brosser avec un gel teinté, puis on vient fournir avec un crayon ou un fard à sourcil. \r\n\r\n<img alt=\"cushnie et ochs\" src=\"http://i.huffpost.com/gen/3739586/thumbs/o-CUSHNIE-ET-OCHS-570.jpg?7\" />\r\n\r\n<strong>Photo: Collection Cushnie et Ochs</strong>\r\n\r\n<strong>5 trucs essentiels pour des sourcils impeccables</strong>\r\n\r\n<strong>1 - Avoir la bonne pince en main</strong>\r\nL’idéal est d’en posséder deux: une plate et l’autre pointue. La pince plate est parfaite pour retirer les poils, car sa forme s’appuie sur l’arcade sourcilière pour aller cherche le poil à la racine. La pince pointue est redoutable pour retirer les poils incarnés sans endommager la peau.\r\n\r\n<strong>2 - Épiler après la douche</strong>\r\nLes poils sont plus mous et les follicules s’ouvrent après une bonne douche chaude, ce qui aide l’extraction.\r\n\r\n<strong>3 - Oublier le miroir grossissant</strong>\r\nCe miroir peut donner l’effet qu’il y a plus de poils à enlever qu’en vrai, et mener à un sourcil trop épilé. On choisit plutôt un miroir régulier dans un bon éclairage. Après quelques poils enlevés, on prend du recul pour vérifier l’effet sur notre visage.\r\n\r\n<strong>4 - Remplir avant d’épiler</strong>\r\nEn remplissant les sourcils à l’aide d’un crayon avant de les épiler, on s’assure de ne pas trop en enlever.\r\n\r\n<strong>5 - Respecter les règles</strong>\r\nOn suit la règle universelle suivante: le sourcil commence au-dessus du coin interne de l’œil, atteint son point le plus haut lorsqu’il est aligné avec le point externe de l’iris, et se termine lorsqu’il est en angle de 45 degrés au coin externe de l’œil. \r\n\r\n\r\n<strong><a href=\"https://www.facebook.com/HuffPostQuebecStyle\"><img src=\"http://big.assets.huffingtonpost.com/FBLOGO_0.png\"></a> <a href=\"https://www.facebook.com/HuffPostQuebecStyle\" target=\"_hplink\">Abonnez-vous à HuffPost Québec Style sur Facebook</a><br> <a href=\"https://twitter.com/huffpostqcstyle\"><img src=\"http://big.assets.huffingtonpost.com/TWITTERBIRD.png\"></a> <a href=\"https://twitter.com/huffpostqcstyle\" target=\"_hplink\">Suivez HuffPost Québec Style sur Twitter</a><br class=\"ethanmobile\" /></strong>\r\n\r\n\r\n <!--HH--236SLIDEEXPAND--456004--HH--> ",
        content_hash = "mmh3-278668057183367062943589265468308093925",
        content_length = 4333,
        content_type = None,
        crawled = new DateTime(),
        crawled_ts = 1449158818132600000L,
        guid = "8708140",
        id = "8Y10Ju/1449158818132600000",
        image_count = 100,
        lang = "en".some,
        license_id = "License ID1",
        link = "http://quebec.huffingtonpost.ca/2015/12/03/tendances-sourcils-lautomnehiver-2015-2016-astuces-_n_8708140.html",
        links = None,
        media = CRMedia(
          audios = Nil.some,
          images = Seq(
            CRMediaImage(
              url = "http://i.huffpost.com/gen/3739358/thumbs/o-CAROLINA-HERRERA-570.jpg?7".some,
              caption = "carolina herrera".some,
              extracted_from_html = true.some,
              original_markup = "<img alt=\"carolina herrera\" src=\"http://i.huffpost.com/gen/3739358/thumbs/o-CAROLINA-HERRERA-570.jpg?7\"/>".some,
              anchor_url = "".some
            ),
            CRMediaImage(
              url = "http://big.assets.huffingtonpost.com/FBLOGO_0.png".some,
              extracted_from_html = true.some,
              original_markup = "<a href=\"https://www.facebook.com/HuffPostQuebecStyle\"><img src=\"http://big.assets.huffingtonpost.com/FBLOGO_0.png\"/></a>".some,
              anchor_url = "https://www.facebook.com/HuffPostQuebecStyle".some
            )
          ).some,
          videos = Nil.some,
          slideshows = Nil.some
        ).some,
        plain_text = "On dit que les sourcils façonnent le visage: raison de plus pour en prendre bien soin! Voici les tendances de la saison et quelques informations pour être au poil.\r\n\r\nL’arche parfaite (Défilé Carolina Herrera)  \r\nCeux-ci sont obtenus avec beaucoup de précision: on suit la ligne naturelle de notre arcade sourcilière et on enlève tout ce qui l’entrave. Pour ajouter de la définition, on utilise un gel teinté d’une couleur qui ressemble à notre poil naturel.\r\n\r\n\r\n\r\nPhoto: Défilé Carolina Herrera\r\n\r\nSourcils brossés (Défilé DKNY)\r\nLe look est frappant et très facile à obtenir: sur des sourcils ayant poussé librement dans les dernières semaines (on cache les ciseaux!), on brosse vers le haut. On obtient la tenue à l’aide d’un gel transparent pour les sourcils.\r\n\r\n\r\n\r\nPhoto: Défilé DKNY\r\n\r\nSourcils naturels (Collection Nicole Miller)\r\nHyper simple, mais qui demande tout de même du travail: on maintient notre ligne et on coupe les poils de façon régulière. Nul besoin de les placer ou d’exagérer la couleur, car on veut l’effet parfaitement naturel.\r\n\r\n\r\n\r\nPhoto: Collection Nicole Miller\r\n\r\n\r\nLes tendances sourcils de l'automne/hiver 2015-2016\r\n\r\n\r\n\r\n\r\n\r\nSourcils fournis (CollectionTory Burch)\r\nSur des sourcils bien entretenus, on comble les endroits parsemés à l’aide d’un produit d’un ton plus foncé que notre couleur naturelle. On repasse ensuite partout pour uniformiser la couleur.\r\n\r\n\r\n\r\nPhoto: Collection Tory Burch\r\n\r\nFournis et brossés (Collection: Cushnie et Ochs)\r\nLe nec plus ultra de la saison: l’union du sourcil brossé et du sourcil fourni. On commence par brosser avec un gel teinté, puis on vient fournir avec un crayon ou un fard à sourcil. \r\n\r\n\r\n\r\nPhoto: Collection Cushnie et Ochs\r\n\r\n5 trucs essentiels pour des sourcils impeccables\r\n\r\n1 - Avoir la bonne pince en main\r\nL’idéal est d’en posséder deux: une plate et l’autre pointue. La pince plate est parfaite pour retirer les poils, car sa forme s’appuie sur l’arcade sourcilière pour aller cherche le poil à la racine. La pince pointue est redoutable pour retirer les poils incarnés sans endommager la peau.\r\n\r\n2 - Épiler après la douche\r\nLes poils sont plus mous et les follicules s’ouvrent après une bonne douche chaude, ce qui aide l’extraction.\r\n\r\n3 - Oublier le miroir grossissant\r\nCe miroir peut donner l’effet qu’il y a plus de poils à enlever qu’en vrai, et mener à un sourcil trop épilé. On choisit plutôt un miroir régulier dans un bon éclairage. Après quelques poils enlevés, on prend du recul pour vérifier l’effet sur notre visage.\r\n\r\n4 - Remplir avant d’épiler\r\nEn remplissant les sourcils à l’aide d’un crayon avant de les épiler, on s’assure de ne pas trop en enlever.\r\n\r\n5 - Respecter les règles\r\nOn suit la règle universelle suivante: le sourcil commence au-dessus du coin interne de l’œil, atteint son point le plus haut lorsqu’il est aligné avec le point externe de l’iris, et se termine lorsqu’il est en angle de 45 degrés au coin externe de l’œil. \r\n\r\n\r\n Abonnez-vous à HuffPost Québec Style sur Facebook\r\n  Suivez HuffPost Québec Style sur Twitter\r\n\r\n\r\n\r\n ".some,
        profane = false,
        provider = None,
        published = new DateTime(),
        reading_time = None,
        relegence = ChRgRelegence(
          categories = Nil.some,
          duplicate = Seq(ChRgDuplicate("Total", 82269175)).some,
          tags = Nil.some,
          topic = ChRgTrendingTopic(id = 82270196, parent_id = None, document_count = 1, created = new DateTime(), heat = 0, avg_access_time = None).some,
          authority_score = None
        ).some,
        slideshow_count = 2,
        slideshow_images_count = 56,
        snippet = "On dit que les sourcils façonnent le visage: raison de plus pour en prendre bien soin! ".some,
        source = ChSourceForGetArticle(
          id = "Source Id",
          groups = Seq(2, 3, 5, 7, 11).some,
          location = ChLocation(34.00201, -118.430832).some,
          publisher_id = "The Publisher ID".some,
          title = "Source Title".some,
          zipcode = "90045".some
        ),
        store = ChStore(
          entry_source = "Le Huffington Post Québec".some,
          entry_mobile_headline = "".some,
          entry_mobile_link = "http://m.huffpost.com/qc/entry/8708140".some
        ).some,
        sub_headline = None,
        summary = None,
        tags = Seq("beauté québec"),
        title = "Les tendances sourcils de l'automne/hiver 2015-2016 et les astuces pour les obtenir",
        update_received = new DateTime(),
        updated = new DateTime(),
        video_count = 12345
      ).some
    )
  )
}

object ChTestStringLoader {
  def getTestString(strName: String) = {
    Source.fromInputStream(this.getClass.getResourceAsStream(strName)).mkString
  }
}

