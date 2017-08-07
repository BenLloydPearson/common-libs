package com.gravity.domain.aol

import com.gravity.utilities.{GrvzAssertions, BaseScalaTest}
import play.api.libs.json._
import com.gravity.utilities.grvstrings.emptyString

class AolLinkTest extends BaseScalaTest with GrvzAssertions {
  test("AolLink JSON read internal format") {
    implicit val reads = AolLink.internalJsonReads

    Json.fromJson[AolLink](Json.parse("""{"text":"hello"}""")) should be(JsSuccess(AolLink(emptyString, "hello")))
    Json.fromJson[AolLink](Json.parse("""{"url":"","text":"hello"}""")) should be(JsSuccess(AolLink(emptyString, "hello")))
    Json.fromJson[AolLink](Json.parse("""{"url":"http://example.com/","text":"hello","showVideoIcon":false}""")) should be(JsSuccess(AolLink("http://example.com/", "hello", showVideoIcon = false)))
    Json.fromJson[AolLink](Json.parse("""{"url":"foobar","text":"hello","showVideoIcon":false}""")) should be(a [JsError])
  }

  test("AolLink JSON read AOL feed format") {
    implicit val reads = AolLink.aolFeedJsonReads

    Json.fromJson[AolLink](Json.parse("""{"href":"","text":"hello"}""")) should be(JsSuccess(AolLink("http://www.aol.com/", "hello")))
    Json.fromJson[AolLink](Json.parse("""{"href":"http://example.com/","text":"hello"}""")) should be(JsSuccess(AolLink("http://example.com/", "hello")))
    Json.fromJson[AolLink](Json.parse("""{"href":"foobar","text":"hello"}""")) should be(a [JsError])
  }

  test("AolLink JSON write") {
    implicit val reads = AolLink.internalJsonReads

    val link1 = AolLink("http://example.com/", "hello", showVideoIcon = false)
    Json.fromJson[AolLink](Json.parse(Json.stringify(Json.toJson(link1)))) should be(JsSuccess(link1))
  }
}