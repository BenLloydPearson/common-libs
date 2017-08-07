package com.gravity.utilities.web

import javax.servlet.http._

import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.grvstrings.emptyString
import com.gravity.utilities.web.http.{GravityHttpServletRequest, ValidatedUrl}
import com.gravity.valueclasses.ValueClassesForUtilities.{StringToUtilitiesValueClasses, Url}
import org.apache.http.HttpHeaders
import org.mockito.Mockito._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

class httpTest extends BaseScalaTest {
  test("ValidatedUrl") {
    ValidatedUrl(emptyString.asUrl) should be(None)
    ValidatedUrl("lolwut".asUrl) should be(None)
    ValidatedUrl("lolwut.com".asUrl) should be(None)
    ValidatedUrl("http://lolwut".asUrl) should be(None)
    ValidatedUrl("http://lolwut.com/".asUrl).get.url.raw should be("http://lolwut.com/")
    ValidatedUrl("https://lolwut.com/".asUrl).get.url.raw should be("https://lolwut.com/")
    ValidatedUrl("https://www.lolwut/".asUrl) should be(None)
    ValidatedUrl("https://www.foo.com/bar/baz.html".asUrl).get.url.raw should be("https://www.foo.com/bar/baz.html")
  }

  test("ValidatedUrl + bogus or no TLD") {
    val v = ValidatedUrl(_: Url, validateTld = false)

    v(emptyString.asUrl) should be(None)
    v("lolwut".asUrl) should be(None)
    v("lolwut.com".asUrl) should be(None)
    v("http://lolwut".asUrl).get.url.raw should be("http://lolwut")
    v("http://lolwut.com/".asUrl).get.url.raw should be("http://lolwut.com/")
    v("https://lolwut.com/".asUrl).get.url.raw should be("https://lolwut.com/")
    v("https://www.lolwut/".asUrl) should be(None)
    v("https://www.foo.com/bar/baz.html".asUrl).get.url.raw should be("https://www.foo.com/bar/baz.html")
  }

  test("Can extract the version from Accept header") {
    val request = mock[HttpServletRequest]
    when (request.getHeader(HttpHeaders.ACCEPT)) thenReturn "Accept: application/json; version=3"

    val gravityRequest = new GravityHttpServletRequest(request)
    val versionOpt = gravityRequest.getVersionHeader
    assert(versionOpt.isDefined)
    val version = versionOpt.get
    assert(version == 3)
  }

  test("Returns None when the Accept header doesn't have a version") {
    val request = mock[HttpServletRequest]
    when (request.getHeader(HttpHeaders.ACCEPT)) thenReturn "Accept: application/json"

    val gravityRequest = new GravityHttpServletRequest(request)
    val versionOpt = gravityRequest.getVersionHeader
    assert(versionOpt.isEmpty)
  }

  test("Returns None when the Accept header accepts all and doesn't have a version") {
    val request = mock[HttpServletRequest]
    when (request.getHeader(HttpHeaders.ACCEPT)) thenReturn "Accept: */*"

    val gravityRequest = new GravityHttpServletRequest(request)
    val versionOpt = gravityRequest.getVersionHeader
    assert(versionOpt.isEmpty)
  }

  test("Returns None when the Accept header isn't present") {
    val request = mock[HttpServletRequest]
    when (request.getHeader(HttpHeaders.ACCEPT)) thenReturn ""

    val gravityRequest = new GravityHttpServletRequest(request)
    val versionOpt = gravityRequest.getVersionHeader
    assert(versionOpt.isEmpty)
  }

  test("Can extract the ApiVersion from Accept header") {
    val request = mock[HttpServletRequest]
    when (request.getHeader(HttpHeaders.ACCEPT)) thenReturn "Accept: application/json; version=1"

    val gravityRequest = new GravityHttpServletRequest(request)
    val versionOpt = gravityRequest.getApiVersion
    assert(versionOpt.isDefined)
    val version = versionOpt.get
    assert(version == ApiVersion1_0)
  }

  test("Will return None when no ApiVersion matches the Accept header") {
    val request = mock[HttpServletRequest]
    when (request.getHeader(HttpHeaders.ACCEPT)) thenReturn "Accept: application/json; version=99"

    val gravityRequest = new GravityHttpServletRequest(request)
    val versionOpt = gravityRequest.getApiVersion
    assert(versionOpt.isEmpty)
  }
}
