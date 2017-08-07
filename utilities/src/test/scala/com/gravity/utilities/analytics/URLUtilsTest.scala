package com.gravity.utilities.analytics

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 6/3/11
 * Time: 12:33 PM
 */

import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.grvstrings.emptyString
import com.gravity.valueclasses.ValueClassesForUtilities._
import org.junit.Assert
import org.junit.Assert._

class URLUtilsTest extends BaseScalaTest {
  test("extractParameterMap for full URL") {
    val url = "http://example.com/some/path?param1=foo&param2=bar&param3=baz#some-fragment"
    val params = URLUtils.extractParameterMap(url)
    println("foo bar")
    params should be (Map("param1" -> "foo", "param2" -> "bar", "param3" -> "baz"))
  }

  test("extractParameterMap for query portion only") {
    val url = "param1=foo&param2=bar&param3=baz#some-fragment"
    val params = URLUtils.extractParameterMap(url)
    params should be (Map("param1" -> "foo", "param2" -> "bar", "param3" -> "baz"))
  }

  test("extractParameterMap for empty string") {
    URLUtils.extractParameterMap("") should be (Map.empty[String, String])
  }

  test("clean") {
    var url = "http://www.readwriteweb.com/archives/google_acquires_postrank_a_fork_in_the_road_for_th.php#disqus_thread"

    val expected = "http://www.readwriteweb.com/archives/google_acquires_postrank_a_fork_in_the_road_for_th.php"
    var actual = URLUtils.clean(url)

    assertEquals("Failed to handle only hash!", expected, actual)

    url = "http://www.readwriteweb.com/archives/google_acquires_postrank_a_fork_in_the_road_for_th.php?utm_source=feedburner&q=something+here&utm_medium=feed&utm_campaign=Feed%3A+readwriteweb+(ReadWriteWeb)"

    actual = URLUtils.clean(url)

    assertEquals("Failed to handle only query!", expected, actual)

    url = "http://www.readwriteweb.com/archives/google_acquires_postrank_a_fork_in_the_road_for_th.php?utm_source=feedburner&q=something+here&utm_medium=feed&utm_campaign=Feed%3A+readwriteweb+(ReadWriteWeb)#disqus_thread"

    actual = URLUtils.clean(url)

    assertEquals("Failed to handle query WITH hash!", expected, actual)
  }

  test("normalizeUrl") {
    val url = "http://www.readwriteweb.com/archives/google_acquires_postrank_a_fork_in_the_road_for_th.php?utm_source=feedburner&q=something+here&id=12345&utm_medium=feed&utm_campaign=Feed%3A+readwriteweb+(ReadWriteWeb)#disqus_thread"

    val expected = "http://www.readwriteweb.com/archives/google_acquires_postrank_a_fork_in_the_road_for_th.php?q=something+here&id=12345"
    val actual = URLUtils.normalizeUrl(url)

    assertEquals(expected, actual)
  }

  test("normalizeEmptyUrl") {
    val expected = emptyString
    val actual = URLUtils.normalizeUrl(emptyString)
    assertEquals(expected, actual)
  }

  test("normalizeNullUrl") {
    val url: String = null
    val expected = emptyString
    val actual = URLUtils.normalizeUrl(url)
    assertEquals(expected, actual)
  }

  test("normalizeHashOnlyUrl") {
    val expected = "#"
    val actual = URLUtils.normalizeUrl("#")
    assertEquals(expected, actual)
  }

  test("appendParameter") {
    val url = "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/05/22/BA1P1OLN18.DTL"
    val res1 = URLUtils.appendParameter(url,"grcc=blahblah")
    val res2 = URLUtils.appendParameter(url,"?grcc=blahblah")
    val res3 = URLUtils.appendParameter(url,"&grcc=blahblah")
    val prefUrl1 = "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/05/22/BA1P1OLN18.DTL&grcc=blahblah"
    Assert.assertEquals(prefUrl1,res1)
    Assert.assertEquals(prefUrl1,res2)
    Assert.assertEquals(prefUrl1,res3)

    val url2 = "http://blog.sfgate.com/dailydish/2012/05/20/billboard-music-awards-red-carpet/"
    val res21 = URLUtils.appendParameter(url2,"grcc=blahblah")
    val res22 = URLUtils.appendParameter(url2,"?grcc=blahblah")
    val res23 = URLUtils.appendParameter(url2,"&grcc=blahblah")
    val prefUrl2 = "http://blog.sfgate.com/dailydish/2012/05/20/billboard-music-awards-red-carpet/?grcc=blahblah"

    Assert.assertEquals(prefUrl2,res21)
    Assert.assertEquals(prefUrl2,res22)
    Assert.assertEquals(prefUrl2,res23)

  }

  test("isSameDomainLink") {
    val url = "http://www.readwriteweb.com/archives/google_acquires_postrank_a_fork_in_the_road_for_th.php?utm_source=feedburner&q=something+here&utm_medium=feed&utm_campaign=Feed%3A+readwriteweb+(ReadWriteWeb)#disqus_thread"
    val baseUrl = "http://www.readwriteweb.com/"

    assertTrue("The following url should be considered in the same domain as: %s%n%s".format(baseUrl, url), URLUtils.isSameDomainLink(baseUrl, url))
  }

  test("mergeParameterExists") {

    val url = "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/05/22/BA1P1OLN18.DTL&grcc=blahblah"

    assertEquals(url, URLUtils.mergeParameter(url, "grcc=foofoo"))
  }

  test("mergeParameterDoesntExist") {

    val url = "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/05/22/BA1P1OLN18.DTL&grcc=blahblah"
    assertEquals(url + "&grcc2=foofoo", URLUtils.mergeParameter(url, "grcc2=foofoo"))

    val url2 = "http://www.sfgate.com/cgi-bin/article.cgi?grcc2=blahblah"
    assertEquals(url2, URLUtils.mergeParameter(url2, "grcc2=foofoo"))

    val url3 = "http://www.sfgate.com/cgi-bin/article.cgi?foo=bar&grcc2=blahblah"
    assertEquals(url3, URLUtils.mergeParameter(url3, "grcc2=foofoo"))

    val url4 = "http://www.sfgate.com/cgi-bin/article.cgi?foo=bar&agrcc2=blahblah"
    assertEquals(url4 + "&grcc2=foofoo", URLUtils.mergeParameter(url4, "grcc2=foofoo"))

    val url5 = "http://www.sfgate.com/cgi-bin/article.cgi?foo=bar&agrcc2=blahblah"
    assertEquals(url5 + "&grcc2=foofoo", URLUtils.mergeParameter(url5, "grcc2=foofoo"))

  }

  test("replaceParameter") {

    val url = "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/05/22/BA1P1OLN18.DTL&grcc=blahblah"
    assertEquals(url + "&grcc2=foo/foo", URLUtils.replaceParameter(url, "grcc2=foo/foo"))

    val url2 = "http://www.sfgate.com/cgi-bin/article.cgi?grcc2=blahblah"
    assertEquals("http://www.sfgate.com/cgi-bin/article.cgi?grcc2=foofoo", URLUtils.replaceParameter(url2, "grcc2=foofoo"))

    val url3 = "http://www.sfgate.com/cgi-bin/article.cgi?foo=bar&grcc2=blahblah"
    assertEquals("http://www.sfgate.com/cgi-bin/article.cgi?foo=bar&grcc2=foofoo", URLUtils.replaceParameter(url3, "grcc2=foofoo"))

    val url4 = "http://www.sfgate.com/cgi-bin/article.cgi?foo=bar&agrcc2=blahblah"
    assertEquals(url4 + "&grcc2=foofoo", URLUtils.replaceParameter(url4, "grcc2=foofoo"))

    val url5 = "http://www.sfgate.com/cgi-bin/article.cgi?foo=bar&agrcc2=blahblah"
    assertEquals(url5 + "&grcc2=foofoo", URLUtils.replaceParameter(url5, "grcc2=foofoo"))

  }


  test("applyParameters") {

    val url = "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/05/22/BA1P1OLN18.DTL&grcc=blahblah"

    assertEquals(url + "&grcc2=foo/foo", URLUtils.applyParameters(URLUtils.mergeParameter)(url, Map("grcc" -> "foo/foo", "grcc2" -> "foo/foo")))
    assertEquals(url + "&grcc=foo/foo&grcc2=foo/foo", URLUtils.applyParameters(URLUtils.appendParameter)(url, Map("grcc" -> "foo/foo", "grcc2" -> "foo/foo")))
    assertEquals(url + "&domain=%Domain%", URLUtils.applyParameters(URLUtils.appendParameter)(url, Map("domain" -> "%Domain%")))
    assertEquals("http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/05/22/BA1P1OLN18.DTL&grcc=foo/foo#anchor", URLUtils.applyParameters(URLUtils.replaceParameter)(url + "#anchor", Map("grcc" -> "foo/foo")))

  }

  test("applyParametersBaseUrl") {

    val url = "http://www.sfgate.com/cgi-bin/article.cgi"

    assertEquals(url + "?grcc=foofoo&grcc2=foofoo", URLUtils.applyParameters(URLUtils.mergeParameter)(url, Map("grcc" -> "foofoo", "grcc2" -> "foofoo")))
    assertEquals(url + "?grcc=foofoo&grcc2=foofoo", URLUtils.applyParameters(URLUtils.appendParameter)(url, Map("grcc" -> "foofoo", "grcc2" -> "foofoo")))
    assertEquals(url + "?grcc=foofoo&grcc2=foofoo", URLUtils.applyParameters(URLUtils.replaceParameter)(url, Map("grcc" -> "foofoo", "grcc2" -> "foofoo")))

  }

  test("replaceHost") {
    assertEquals("foo://blorg", URLUtils.replaceHost("foo://bar", "blorg"))
    assertEquals("foo://blorg/", URLUtils.replaceHost("foo://bar/", "blorg"))
    assertEquals("foo://blorg/blech/bloop", URLUtils.replaceHost("foo://bar/blech/bloop", "blorg"))
  }

  test("safeReplaceHost") {
    assertEquals("foo://blorg", URLUtils.safeReplaceHost("foo://bar".asUrl, "blorg"))
    assertEquals("foo://blorg/", URLUtils.safeReplaceHost("foo://bar/".asUrl, "blorg"))
    assertEquals("foo://blorg/blech/bloop", URLUtils.safeReplaceHost("foo://bar/blech/bloop".asUrl, "blorg"))
    assertEquals("foo://blorg:8080/blech/bloop", URLUtils.safeReplaceHost("foo://bar:8080/blech/bloop".asUrl, "blorg"))
  }

  test("urlIsValid") {
    assertEquals(false, URLUtils.urlIsValid(emptyString, validResponseCodes = Set.empty[Int]))
    assertEquals(false, URLUtils.urlIsValid("  ", validResponseCodes = Set.empty[Int]))
    assertEquals(false, URLUtils.urlIsValid("http:/foobar", mustHaveScheme = true, validResponseCodes = Set.empty[Int]))
    assertEquals(true, URLUtils.urlIsValid("http:/foobar", mustHaveScheme = false, validResponseCodes = Set.empty[Int]))
    assertEquals(false, URLUtils.urlIsValid("foobar", validResponseCodes = Set.empty[Int]))
    assertEquals(false, URLUtils.urlIsValid("http://", mustHaveScheme = true, validResponseCodes = Set.empty[Int]))
    assertEquals(true, URLUtils.urlIsValid("http://foobar", validResponseCodes = Set.empty[Int]))
    assertEquals(true, URLUtils.urlIsValid("http://www.example.com/", validResponseCodes = Set.empty[Int]))
  }

  test("getTLD") {
    val url = "http://www.readwriteweb.com/archives/google_acquires_postrank_a_fork_in_the_road_for_th.php?utm_source=feedburner&q=something+here&utm_medium=feed&utm_campaign=Feed%3A+readwriteweb+(ReadWriteWeb)"
    assertEquals("readwriteweb.com", URLUtils.extractTopLevelDomain(url).getOrElse("fail"))
    val url2 = "http://www.test.foo.bar.sub.com"
    assertEquals("sub.com", URLUtils.extractTopLevelDomain(url2).getOrElse("fail"))

  }

  test("nextQueryStringParamSeparator") {
    assertSame('?', URLUtils.nextQueryStringParamSeparator("http://foobar.com/"))
    assertSame('&', URLUtils.nextQueryStringParamSeparator("http://foobar.com/?baz=123"))
  }

  test("getParamValue") {
    assertSame(None, URLUtils.getParamValue(emptyString, "baz"))
    assertSame(None, URLUtils.getParamValue("http://foobar.com/", "baz"))
    assertSame(None, URLUtils.getParamValue(emptyString, emptyString))
    assertSame(None, URLUtils.getParamValue("http://foobar.com/?baz=123", emptyString))
    assertEquals(Some("123"), URLUtils.getParamValue("http://foobar.com/?baz=123", "baz"))
    assertEquals(Some("schmoop"), URLUtils.getParamValue("http://foobar.com/?baz=123&blarg=schmoop", "blarg"))
    assertEquals(Some(emptyString), URLUtils.getParamValue("http://foobar.com/?baz=123&blarg=", "blarg"))
    assertEquals(Some(emptyString), URLUtils.getParamValue("http://foobar.com/?baz=&blarg=schmoop", "baz"))
  }

  test("rfc1738EncodeUnsafeQueryStringChars") {
    URLUtils.rfc1738EncodeUnsafeQueryStringChars("") should be("")
    URLUtils.rfc1738EncodeUnsafeQueryStringChars("http://") should be("http://")
    URLUtils.rfc1738EncodeUnsafeQueryStringChars("http://|<>|><><><.com/fuck/you") should be("http://|<>|><><><.com/fuck/you")
    URLUtils.rfc1738EncodeUnsafeQueryStringChars("http://fuck.who/|<>?|<>%20#foobar") should be("http://fuck.who/|<>?%7C%3C%3E%20#foobar")
  }

  test("seoize") {
    URLUtils.seoize("") should be ("")
    URLUtils.seoize("foobar") should be ("foobar")
    URLUtils.seoize("FOO+_bar123") should be ("foo-bar123")
    URLUtils.seoize("+++&foo+bar*") should be ("foo-bar")
    URLUtils.seoize("+++&fü+bar*") should be ("fü-bar")
  }
}