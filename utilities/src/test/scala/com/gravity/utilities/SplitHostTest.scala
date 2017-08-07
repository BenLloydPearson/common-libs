package com.gravity.utilities

import org.junit.Test
import org.scalatest.Matchers
import org.scalatest.junit.{AssertionsForJUnit, ShouldMatchersForJUnit}
import org.scalatest.OptionValues._

class SplitHostTest extends AssertionsForJUnit with Matchers {

  @Test def splitHost() {
    val commonUrl = "www.google.com/"
    SplitHost.fromUrl(commonUrl).value should equal (FullHost(Some("www"), "google", "com"))

    val intlUrl = "http://forums.bbc.co.uk/"
    SplitHost.fromUrl(intlUrl).value should equal (FullHost(Some("forums"), "bbc", "co.uk"))

    val intlNoSubUrl = "http://google.com.au"
    SplitHost.fromUrl(intlNoSubUrl).value should equal (FullHost(None, "google", "com.au"))

    val intlException = "http://www.parliament.uk"
    SplitHost.fromUrl(intlException).value should equal (FullHost(Some("www"), "parliament", "uk"))

    val localhost = "localhost"
    SplitHost.fromUrl(localhost).value should equal (UnknownDomain(None, "localhost"))

    val dotsOnly = "..."
    SplitHost.fromUrl(dotsOnly) should be ('empty)

    val empty = ""
    SplitHost.fromUrl(empty) should be ('empty)

    val urlInUrl = "http://13139002.dyo.gs/url/http://www.clashmusic.com/reviews/primal-scream-mc5-black-to-comm"
    SplitHost.fromUrl(urlInUrl).value should equal (FullHost(Some("13139002"), "dyo", "gs"))

    val ip = "123.255.123.255/index.php"
    val ipSpl = SplitHost.fromUrl(ip)
    ipSpl.value should equal (IpHost("123.255.123.255"))

    val query = "google.com?q=cats"
    SplitHost.fromUrl(query).value should equal (FullHost(None, "google", "com"))

    val guavaExceptionByTheirConventionNotOurs = "joe.blogspot.com"
    SplitHost.fromUrl(guavaExceptionByTheirConventionNotOurs).value should equal (FullHost(Some("joe"), "blogspot", "com"))
  }

  @Test def splitHostConvenienceVals() {
    val commonUrl = "http://www.google.com/?q=Kanye+West"
    val commonSpl = SplitHost.fullHostFromUrl(commonUrl).get
    commonSpl.registeredDomain should equal ("google.com")
    commonSpl.rejoin should equal ("www.google.com")
    commonSpl.rejoinNoWWW should equal ("google.com")

    val intlUrl = "http://forums.bbc.co.uk"
    val intlSpl = SplitHost.fullHostFromUrl(intlUrl).get
    intlSpl.registeredDomain should equal ("bbc.co.uk")
    intlSpl.rejoin should equal ("forums.bbc.co.uk")
    intlSpl.rejoinNoWWW should equal ("forums.bbc.co.uk")
  }

  @Test def takeRight() {
    val deepSubdomain = "money.news.yahoo.co.uk"
    val deepSubdomainSpl = SplitHost.fullHostFromUrl(deepSubdomain).get
    deepSubdomainSpl takeRight (1) should equal ("co.uk")
    deepSubdomainSpl takeRight (2) should equal ("yahoo.co.uk")
    deepSubdomainSpl takeRight (3) should equal ("news.yahoo.co.uk")
    deepSubdomainSpl takeRight (4) should equal (deepSubdomain)
  }

}
