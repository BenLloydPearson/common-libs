package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.operations.{ImpressionViewedEvent, ImpressionViewedEventVersion, OrdinalArticleKeyPair, TestPageViewId}
import com.gravity.utilities.eventlogging.FieldValueRegistry
import com.gravity.utilities.grvfields._
import com.gravity.utilities.{BaseScalaTest, grvfields}
import org.joda.time.DateTime
import org.junit.Assert._
import play.api.libs.json.Json

import scalaz.{Failure, Success}


class ImpressionViewedEventTest extends BaseScalaTest {
  val pageViewIdHash: String = TestPageViewId.hash
  val siteGuid = "abc123"
  val userGuid = "userAbc123"
  val sitePlacementId = 1
  val userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36"
  val remoteIp = "108.13.230.170"
  val gravityHost = "mbpro.fflintstone"
  val impressionHash = "exampleImpHash"
  val ordinalArticleKeys: List[OrdinalArticleKeyPair] = List(
    OrdinalArticleKeyPair(2, ArticleKey("http://example.com/article-1.html")),
    OrdinalArticleKeyPair(5, ArticleKey("http://example.com/article-2.html"))
  )
  val testEvent: ImpressionViewedEvent = ImpressionViewedEvent(
    System.currentTimeMillis()  ,
    pageViewIdHash,
    siteGuid,
    userGuid,
    sitePlacementId,
    userAgent,
    remoteIp,
    System.currentTimeMillis(),
    gravityHost,
    ImpressionViewedEventVersion.V1.id,
    hashHex = impressionHash,
    ordinalArticleKeys = ordinalArticleKeys,
    notes = "notes"
  )

  import com.gravity.domain.FieldConverters._

  test("field read write") {
    val fieldString = grvfields.toDelimitedFieldString(testEvent)
    FieldValueRegistry.getInstanceFromString[ImpressionViewedEvent](fieldString) match {
      case Success(a: ImpressionViewedEvent) =>
        assertTrue(a.isInstanceOf[ImpressionViewedEvent])
        assertEquals(fieldString, a.toDelimitedFieldString)
      case Failure(fails) => assertTrue(fails.toString(), false)
    }
  }

  test("ordinal-article-key pairs deserialization") {
    val json1 = """[{"o":2,"au":"http://foobar.com/"},{"o":5,"au":"http://baz.com/"}]"""
    val result1 = OrdinalArticleKeyPair.fromIveJson(json1)
    result1 should be('success)
    result1.foreach {
      case pairs =>
        pairs.size should be(2)
        pairs should contain(OrdinalArticleKeyPair(2, ArticleKey("http://foobar.com/")))
        pairs should contain(OrdinalArticleKeyPair(5, ArticleKey("http://baz.com/")))
    }

    val json2 = """[]"""
    val result2 = OrdinalArticleKeyPair.fromIveJson(json2)
    result2 should be('success)
    result2.foreach {
      case pairs =>
        pairs should be(Nil)
    }

    val json3 = "bad json"
    val result3 = OrdinalArticleKeyPair.fromIveJson(json3)
    result3 should be ('failure)
  }

  test("ordinal-article-key pairs using implicit writer can be deserialized") {
    val oakpList = List(
      OrdinalArticleKeyPair(2, ArticleKey("http://foobar.com/")),
      OrdinalArticleKeyPair(5, ArticleKey("http://baz.com/"))
    )
    val json = Json.toJson(oakpList)
    val json1 = Json.prettyPrint(json)

    val result = OrdinalArticleKeyPair.fromIveJson(json1)
    result should be('success)
    result.foreach {
      case pairs =>
        pairs.size should be(2)
        pairs should contain(OrdinalArticleKeyPair(2, ArticleKey("http://foobar.com/")))
        pairs should contain(OrdinalArticleKeyPair(5, ArticleKey("http://baz.com/")))
    }
  }

  test("non-list ordinal-article-key pair deserialization should fail") {
    val oakpList = OrdinalArticleKeyPair(2, ArticleKey("http://foobar.com/"))
    val json = Json.toJson(oakpList)
    val json1 = Json.prettyPrint(json)

    val result = OrdinalArticleKeyPair.fromIveJson(json1)
    result should be('failure)
  }
}
