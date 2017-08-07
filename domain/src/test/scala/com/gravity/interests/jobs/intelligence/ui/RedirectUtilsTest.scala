package com.gravity.interests.jobs.intelligence.ui

import com.gravity.test.domainTesting
import com.gravity.utilities.BaseScalaTest

/**
 * Created by agrealish14 on 8/18/15.
 */
class RedirectUtilsTest extends BaseScalaTest with domainTesting {

  test("Test huffpo redirect") {

    val expected = "http://grvrdr.huffingtonpost.com/302/redirect"

    val url1 = "http://live.huffingtonpost.com/r/highlight/salma-hayek-explains-the-real-reason-hollywood-studios-dont-want-her/55c3a7d202a760daa30000e7?source=gravityRR&cps=gravity_5118_-7260787118194368692"
    assert(RedirectUtils.redirectPrefix(Some(url1), "contextPath") == expected)

    val url2 = "http://www.huffingtonpost.com/entry/carly-fiorina-paid-leave_55ccec50e4b0898c4886de6e"
    assert(RedirectUtils.redirectPrefix(Some(url2), "contextPath") == expected)

    val url3 = "http://huffingtonpost.com/entry/carly-fiorina-paid-leave_55ccec50e4b0898c4886de6e"
    assert(RedirectUtils.redirectPrefix(Some(url3), "contextPath") == expected)
  }

  test("Test kitchendaily redirect") {

    val expected = "http://grvrdr.kitchendaily.com/302/redirect"

    val url1 = "http://www.kitchendaily.com/read/trick-hard-and-soft-boiled-eggs"
    assert(RedirectUtils.redirectPrefix(Some(url1), "contextPath") == expected)

    val url2 = "http://kitchendaily.com/read/trick-hard-and-soft-boiled-eggs"
    assert(RedirectUtils.redirectPrefix(Some(url2), "contextPath") == expected)
  }

  test("Test aol redirect") {

    val expected = "http://grvrdr.aol.com/302/redirect"

    val url1 = "http://www.aol.com/article/2015/08/18/fda-approves-female-sex-pill-but-with-safety-restrictions/21224387/"
    assert(RedirectUtils.redirectPrefix(Some(url1), "contextPath") == expected)

    val url2 = "http://aol.com/article/2015/08/18/fda-approves-female-sex-pill-but-with-safety-restrictions/21224387/"
    assert(RedirectUtils.redirectPrefix(Some(url2), "contextPath") == expected)
  }

  test("Test on.aol redirect") {

    val expected = "http://grvrdr.aol.com/302/redirect"

    val url1 = "http://on.aol.com/video/519019204"
    assert(RedirectUtils.redirectPrefix(Some(url1), "contextPath") == expected)
  }

  test("Test no redirect") {

    val expected = "http://localhost:8080/302/redirect"

    val url1 = "http://www.dummysite.com/article/519019204"
    assert(RedirectUtils.redirectPrefix(Some(url1), "contextPath") == expected)
  }

  test("Secure fallback redirect") {
    RedirectUtils.redirectPrefix(None, "", https = true) should be("https://localhost:8080/302/redirect")
  }
}