package com.gravity.utilities.analytics

import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.analytics.UrlMatcher._

class URLMatcherTest extends BaseScalaTest {
  val allPaths: List[String] = combinations(combinations(List("", "/dir1", "/dir2"), List("", "/dira", "/dirb")), List("", "/art1.html", "/art2.html"))
  val allSampleUrlStrs: Set[String] = withTrailingSlash( allUrlStrings(List(     "sample.com"), allPaths) ).toSet
  val allBigAssUrlStrs: Set[String] = withTrailingSlash( allUrlStrings(List("bigassample.com"), allPaths) ).toSet
  val allUrlStrs: Set[String] = allSampleUrlStrs ++ allBigAssUrlStrs

  def combinations(prefixes: List[String], suffixes: List[String]): List[String] = for {
    prefix <- prefixes
    suffix <- suffixes
  } yield prefix + suffix

  def withHttpAndHttps(strs: List[String]): List[String] = combinations(List("http://", "https://"), strs)

  def withTrailingSlash(strs: List[String]): List[String] = strs.flatMap(s =>
    if (s.endsWith("/") || s.endsWith(".html"))
      List(s)
    else
      List(s, s + "/"))

  def allUrlStrings(host: String, paths: List[String]): List[String] =  {
    val hosts = combinations(List("", "www.", "ab.", "cde."), List(host))
//    val hosts = combinations(List(""), List(host))
    val auths = combinations(hosts, List("", ":80", ":8080"))
    withHttpAndHttps(combinations(auths, paths))
  }

  def allUrlStrings(hosts: List[String], paths: List[String]): List[String] =
    hosts.flatMap(h => allUrlStrings(h, paths))

//  def alsoWith(transform: (String) => (String), strs: List[String]) = strs ::: strs.map(transform)
//
//  def alsoWithPrefix(prefix: String, strs: List[String]) = strs ::: strs.map(prefix + _)
//
//  def alsoWithWww(strs: List[String]) = alsoWithPrefix("www.", strs)
//  def alsoWithHttp(strs: List[String]) = alsoWithPrefix("http://", strs)
//  def alsoWithHttps(strs: List[String]) = alsoWithPrefix("https://", strs)
//
//  def withPort(port: Int, strs: List[String]) = port match {
//    case -1 => strs
//    case port => strs.map(_ + s":$port")
//  }
//
//  def alsoWithPort(port: Int, strs: List[String]) = alsoWith(_ + s":$port", strs)

  test("Sanity Checks") {
    assert(
      None != toOptUrlMatcher(List(IncExcUrlStrings(List(""), List(""), List(""), List("")))),
      "All-empty patterns should result in a non-empty matcher."
    )

    assert(
      None != toOptUrlMatcher(List(IncExcUrlStrings(List("www.sample.com"), List(""), List(""), List("")))),
      "Non-empty include host should result in a non-empty matcher."
    )

    assert(
      None != toOptUrlMatcher(List(IncExcUrlStrings(List(":80"), List(""), List(""), List("")))),
      "Non-empty include port should result in a non-empty matcher."
    )

    assert(
      None != toOptUrlMatcher(List(IncExcUrlStrings(List(""), List("/dir1/famous-cats-in-history.html"), List(""), List("")))),
      "Non-empty include path should result in a non-empty matcher."
    )

    assert(
      None != toOptUrlMatcher(List(IncExcUrlStrings(List(""), List(""), List("www.sample.com"), List("")))),
      "Non-empty exclude host should result in a non-empty matcher."
    )

    assert(
      None != toOptUrlMatcher(List(IncExcUrlStrings(List(""), List(""), List(":80"), List("")))),
      "Non-empty exclude port should result in a non-empty matcher."
    )

    assert(
      None != toOptUrlMatcher(List(IncExcUrlStrings(List(""), List(""), List(""), List("/dir1/famous-dogs-in-history.html")))),
      "Non-empty exclude path should result in a non-empty matcher."
    )
  }

  def matchingUrls(urlStrs: Set[String], incExcStrs: List[IncExcUrlStrings]): Set[_ <: String] = {
    toOptUrlMatcher(incExcStrs) match {
      case None => Set.empty
      case Some(matcher) => urlStrs.filter(str => matcher(UrlFields(str)))
    }
  }

  test("A few large group match tests") {
    val matchSamOnly     = matchingUrls(allUrlStrs, List(IncExcUrlStrings(List("sample.com"), List(""), List(""), List(""))))
    val matchSamWwwOnly  = matchingUrls(allUrlStrs, List(IncExcUrlStrings(List("www.sample.com"), List(""), List(""), List(""))))
    val matchSamWwwHttps = matchingUrls(allUrlStrs, List(IncExcUrlStrings(List("https://www.sample.com"), List(""), List(""), List(""))))

    // Matches only HTTP explicitly in the pattern.
    val matchSamWwwHttp1 = matchingUrls(allUrlStrs, List(IncExcUrlStrings(List("http://www.sample.com"), List(""), List(""), List(""))))

    // Matches only HTTP by excluding HTTPS.
    val matchSamWwwHttp2 = matchingUrls(allUrlStrs, List(IncExcUrlStrings(List("www.sample.com"), List(""), List("https://"), List(""))))
    val matchSamAbOnly   = matchingUrls(allUrlStrs, List(IncExcUrlStrings(List("ab.sample.com"), List(""), List(""), List(""))))
    val matchSamCdeOnly  = matchingUrls(allUrlStrs, List(IncExcUrlStrings(List("cde.sample.com"), List(""), List(""), List(""))))
    val matchSamMulti    = matchingUrls(allUrlStrs, List(IncExcUrlStrings(List("*.sample.com"), List(""), List(""), List(""))))
    val matchSamNone1    = matchingUrls(allUrlStrs, List(IncExcUrlStrings(List("f.sample.com"), List(""), List(""), List(""))))

    val matchAll1 = matchingUrls(allUrlStrs, List(IncExcUrlStrings(List("*:*"), List(""), List(""), List(""))))
    val matchAll2 = matchingUrls(allUrlStrs, List(IncExcUrlStrings(List("sample.com:*", "*.sample.com:*", "bigassample.com:*", "*.bigassample.com:*"), List(""), List(""), List(""))))

//    val matchBig = matchingUrls(allUrlStrs, List(IncExcUrlStrings(List("bigassample.com"), List(""), List(""), List(""))))

//    matchAll.toList.sorted.foreach(println)

    assert(matchAll1.size == allUrlStrs.size, "size of matchAll1 should match size of everything")
    assert(matchAll1      == allUrlStrs     , "matchAll1 should match everything")
    assert(matchAll2.size == allUrlStrs.size, "size of matchAll2 should match size of everything")
    assert(matchAll2      == allUrlStrs     , "matchAll2 should match everything")

    val numSubHosts = 3
    val numHostVariants = numSubHosts + 1
    val numInvalidPorts = 2      // 1/2 of the ports don't match their protocol (1/3 = 8080, 1/6 = 80 on https)
    val numHttpPortPerHttpsPort = 2  // http has "" and "80", https just has ""

    assert(matchSamOnly.size    == allSampleUrlStrs.size / (numHostVariants * numInvalidPorts), "matchSamOnly size is wrong")
    assert(matchSamWwwOnly.size == allSampleUrlStrs.size / (numHostVariants * numInvalidPorts), "matchSamWwwOnly size is wrong")
    assert(matchSamAbOnly.size  == allSampleUrlStrs.size / (numHostVariants * numInvalidPorts), "matchSamAbOnly size is wrong")
    assert(matchSamCdeOnly.size == allSampleUrlStrs.size / (numHostVariants * numInvalidPorts), "matchSamCdeOnly size is wrong")
    assert(matchSamMulti.size   == numSubHosts * matchSamWwwOnly.size , "matchSamMulti size is wrong")
    assert(matchSamNone1.size == 0, "matchSamNone1 should be empty")

//    matchSamWwwHttp1 should not be empty  // Check how to add a clue to this.
    assert(matchSamWwwHttp1.size != 0, "matchSamWwwHttp1.size should not be 0")
    assert(matchSamWwwHttp1.size != matchSamWwwOnly.size, "matchSamWwwHttp1.size should not equal matchSamWwwOnly.size")
    assert(matchSamWwwHttp1.size == matchSamWwwHttp2.size, "matchSamWwwHttp1.size should equal matchSamWwwHttp2.size")

    assert(matchSamWwwHttps.size != 0, "matchSamWwwHttps.size should not be 0")
    assert(matchSamWwwHttp1.size == matchSamWwwHttps.size * numHttpPortPerHttpsPort, "matchSamWwwHttp1.size ratio to matchSamWwwHttps.size is wrong")

//    assert(matchSam.size == matchBig.size)
//    assert(matchSam.size == matchAll.size / 2)
//    assert(matchAll.size == allUrlStrs.size, "Matching both sites should find all sites")
//    assert((matchSam ++ matchBig).size == allUrlStrs.size, "Two halves should equal a whole")

  }

  def incExcUrlStrs(incAuths: Seq[String] = Nil, incPaths: Seq[String] = Nil, excAuths: Seq[String] = Nil, excPaths: Seq[String] = Nil): IncExcUrlStrings =
    IncExcUrlStrings(incAuths, incPaths, excAuths, excPaths)

  def testMatch(url: String, incExcStrsSeq: Seq[IncExcUrlStrings]): Boolean = {
    UrlMatcher.tryToLowerUrlFields(url) match {
      case None => throw new Exception(s"Failed to get UrlFields for `$url`")
      case Some(urlFields) => {
        val matchFun = toUrlMatcher(incExcStrsSeq)

        matchFun(urlFields)
      }
    }
  }

  def testMatch(url: String, incExcStrs: IncExcUrlStrings): Boolean =
    testMatch(url, List(incExcStrs))

  def testMatch(url: String, incAuth: String = "", incPath: String = "", excAuth: String = "", excPath: String = ""): Boolean =
    testMatch(url, incExcUrlStrs(List(incAuth), List(incPath), List(excAuth), List(excPath)))

  //this is similar to how we match rss feeds with no rules
  def defaultUrlMatcherTest(rawUrl: String): Boolean = {
    val defaultUrlMatcher = IncExcUrlStrings(List(""), List(""), List(""), List("")).toUrlMatcher.get

    UrlMatcher.tryToLowerUrlFields(rawUrl) match {
      // If we can't parse the article's URL, then we don't like it.
      case None => false

      // The article's URL has to pass at least one of the rules.
      case Some(urlFields) => defaultUrlMatcher(urlFields)
    }
  }

  //
  // Protocols
  //

  test(""" Explicit "" in protocol matches everything. """) {
    assert(true == testMatch("http://sample.com", incAuth = "://"))
    assert(true == testMatch("https://sample.com", incAuth = "://"))
    assert(true == testMatch("sample.com", incAuth = "://"))
  }

  test(""" Explicit "*" in protocol matches everything. """) {
    assert(true == testMatch("http://sample.com", incAuth = "*://"))
    assert(true == testMatch("https://sample.com", incAuth = "*://"))
    assert(true == testMatch("sample.com", incAuth = "*://"))
  }

  test(""" Missing protocol in pattern matches everything. """) {
    assert(true == testMatch("http://sample.com", incAuth = ""))
    assert(true == testMatch("https://sample.com", incAuth = ""))
    assert(true == testMatch("sample.com", incAuth = ""))
  }

  test(""" non-http protocols don't match http or https. """) {
    assert(false == testMatch("sample.com", incAuth = "spork://"))
    assert(false == testMatch("sample.com", incAuth = "gopher://"))
    assert(false == testMatch("sample.com", incAuth = "ftp://"))
  }

  test(""" Explicit protocol filters correctly filter against explicit protocols in haveURLs. """) {
    assert(true == testMatch("http://sample.com", incAuth = "http://"))
    assert(true == testMatch("http://sample.com", incAuth = "HTTP://"))
    assert(true == testMatch("HTTP://sample.com", incAuth = "http://"))

    assert(true  == testMatch("https://sample.com", incAuth="https://"))
    assert(true  == testMatch("https://sample.com", incAuth="HTTPS://"))
    assert(true  == testMatch("HTTPS://sample.com", incAuth="https://"))
  }

  test(""" Explicit protocol filters correctly filter against implicit protocols in haveURLs. """) {
    assert(true == testMatch("sample.com", incAuth = "http://"))
    assert(true == testMatch("sample.com", incAuth = "HTTP://"))
    assert(false == testMatch("sample.com", incAuth = "https://"))
    assert(false == testMatch("sample.com", incAuth = "HTTPS://"))
  }

  //
  // Hosts
  //

  test(""" Just matching "*.com" or "*.gov" works as expected. """) {
    assert(true  == testMatch(    "sample.com", incAuth="*.com"))
    assert(true  == testMatch("www.sample.com", incAuth="*.com"))
    assert(false == testMatch(    "sample.com", incAuth="*.gov"))
    assert(false == testMatch("www.sample.com", incAuth="*.gov"))
  }

  test(""" "xxxxx.com" does not imply "www.xxxxx.com". """) {
    assert(true  == testMatch(    "sample.com", incAuth="sample.com"))
    assert(false == testMatch("www.sample.com", incAuth="sample.com"))
  }

  test(""" "www.xxxxx.com" does not imply "xxxxx.com". """) {
    assert(false == testMatch(     "sample.com", incAuth="www.sample.com"))
    assert(true  == testMatch( "www.sample.com", incAuth="www.sample.com"))
    assert(true  == testMatch( "www.SAMPLE.com", incAuth="www.sample.com"))
    assert(true  == testMatch( "www.sample.com", incAuth="www.SAMPLE.com"))
  }

  test(""" "xxxxx.com" does not match YYYwww.xxxxx.com, YYYxxxxx.com, xxxxx.comYYY, or www.xxxxx.comYYY """) {
    assert(true  == testMatch(       "sample.com"   , incAuth="sample.com"))
    assert(false == testMatch(    "YYYsample.com"   , incAuth="sample.com"))
    assert(false == testMatch(       "sample.comYYY", incAuth="sample.com"))
    assert(false == testMatch(   "www.sample.com"   , incAuth="sample.com"))
    assert(false == testMatch("YYYwww.sample.com"   , incAuth="sample.com"))
    assert(false == testMatch(   "www.sample.comYYY", incAuth="sample.com"))
  }

  test(""" "*.xxxxx.com" does not match xxxxx.com """) {
    assert(false == testMatch(     "sample.com", incAuth=  "*.sample.com"))
    assert(true  == testMatch( "www.sample.com", incAuth=  "*.sample.com"))
    assert(true  == testMatch(  "ab.sample.com", incAuth=  "*.sample.com"))
    assert(true  == testMatch( "a.b.sample.com", incAuth=  "*.sample.com"))
  }

  test(""" Neither "xxxxx.com" nor "*.xxxxx.com" match YYYxxxxx.com """) {
    assert(false == testMatch("bigassample.com", incAuth=    "sample.com"))
    assert(false == testMatch("bigassample.com", incAuth=  "*.sample.com"))
  }

  test(""" "xxxxx.*" matches xxxxx.{com, gov}, but does not match www.xxxxx.{com, gov}, xxxxxY.com, Yxxxxx.com, or Y.xxxxx.com (if Y is not www) """) {
    assert(true  == testMatch(       "sample.gov"   , incAuth=  "sample.*"))
    assert(false == testMatch(   "www.sample.gov"   , incAuth=  "sample.*"))
    assert(false == testMatch(    "ab.sample.gov"   , incAuth=  "sample.*"))
    assert(false == testMatch( "a.www.sample.gov"   , incAuth=  "sample.*"))
    assert(false == testMatch(   "    sampleSEX.gov", incAuth=  "sample.*"))
    assert(false == testMatch(   " SEXsample.gov"   , incAuth=  "sample.*"))
  }

  test(""" "*.xxxxx.*" matches any host ending in .xxxxx.{com, gov, etc} but does not match xxxxx.{com, gov, etc} """) {
    assert(false == testMatch(       "sample.gov"    , incAuth=  "*.sample.*"))
    assert(true  == testMatch(   "www.sample.gov"    , incAuth=  "*.sample.*"))
    assert(true  == testMatch(    "ab.sample.gov"    , incAuth=  "*.sample.*"))
    assert(true  == testMatch( "a.www.sample.gov"    , incAuth=  "*.sample.*"))
    assert(false == testMatch(   "www.sample.SEX.gov", incAuth=  "*.sample.*"))
    assert(false == testMatch(   "www.sampleSEX.gov" , incAuth=  "*.sample.*"))
    assert(false == testMatch("www.SEXsample.gov"    , incAuth=  "*.sample.*"))
    assert(false == testMatch( "wwwSEXsample.gov"    , incAuth=  "*.sample.*"))
  }

  //
  // Ports
  //

  test(""" DefaultUrlMatcher matches urls as per their protocol """) {
    assert(true  == defaultUrlMatcherTest("sample.com"))
    assert(true  == defaultUrlMatcherTest("sample.com:80"))
    assert(true  == defaultUrlMatcherTest("http://sample.com"))
    assert(true  == defaultUrlMatcherTest("http://sample.com:80"))
    assert(true  == defaultUrlMatcherTest("https://sample.com"))
    assert(true  == defaultUrlMatcherTest("https://sample.com:443"))

    assert(false == defaultUrlMatcherTest("sample.com:8080"))
    assert(false == defaultUrlMatcherTest("http://sample.com:8080"))
    assert(false == defaultUrlMatcherTest("https://sample.com:8080"))
  }


  test(""" Missing ports in include patterns match default ports (80/443), express or implied. """) {
    assert(true  == testMatch(         "sample.com"     , incAuth=  "sample.com"))
    assert(true  == testMatch(  "http://sample.com"     , incAuth=  "sample.com"))
    assert(true  == testMatch( "https://sample.com"     , incAuth=  "sample.com"))
    assert(true  == testMatch(         "sample.com:80"  , incAuth=  "sample.com"))
    assert(true  == testMatch(  "http://sample.com:80"  , incAuth=  "sample.com"))
    assert(false == testMatch(         "sample.com:443" , incAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample.com:443" , incAuth=  "sample.com"))
    assert(true  == testMatch( "https://sample.com:443" , incAuth=  "sample.com"))
    assert(false == testMatch(         "sample.com:8080", incAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample.com:8080", incAuth=  "sample.com"))
    assert(false == testMatch( "https://sample.com:8080", incAuth=  "sample.com"))
  }

  test(""" Missing ports in exclude patterns match all ports for that domain, express or implied. """) {
    assert(false == testMatch(         "sample.com"     , excAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample.com"     , excAuth=  "sample.com"))
    assert(false == testMatch( "https://sample.com"     , excAuth=  "sample.com"))
    assert(false == testMatch(         "sample.com:80"  , excAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample.com:80"  , excAuth=  "sample.com"))
    assert(false == testMatch(         "sample.com:443" , excAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample.com:443" , excAuth=  "sample.com"))
    assert(false == testMatch( "https://sample.com:443" , excAuth=  "sample.com"))
    assert(false == testMatch(         "sample.com:8080", excAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample.com:8080", excAuth=  "sample.com"))
    assert(false == testMatch( "https://sample.com:8080", excAuth=  "sample.com"))
  }

  test(""" Missing ports in exclude patterns match default ports (80/443) for other domains, express or implied. """) {
    assert(true  == testMatch(         "sample2.com"     , excAuth=  "sample.com"))
    assert(true  == testMatch(  "http://sample2.com"     , excAuth=  "sample.com"))
    assert(true  == testMatch( "https://sample2.com"     , excAuth=  "sample.com"))
    assert(true  == testMatch(         "sample2.com:80"  , excAuth=  "sample.com"))
    assert(true  == testMatch(  "http://sample2.com:80"  , excAuth=  "sample.com"))
    assert(false == testMatch(         "sample2.com:443" , excAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample2.com:443" , excAuth=  "sample.com"))
    assert(true  == testMatch( "https://sample2.com:443" , excAuth=  "sample.com"))
    assert(false == testMatch(         "sample2.com:8080", excAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample2.com:8080", excAuth=  "sample.com"))
    assert(false == testMatch( "https://sample2.com:8080", excAuth=  "sample.com"))
  }

  test(""" "" ports in include patterns match default ports (80/443), express or implied. """) {
    assert(true  == testMatch(         "sample.com"     , incAuth=  "sample.com"))
    assert(true  == testMatch(  "http://sample.com"     , incAuth=  "sample.com"))
    assert(true  == testMatch( "https://sample.com"     , incAuth=  "sample.com"))
    assert(true  == testMatch(         "sample.com:80"  , incAuth=  "sample.com"))
    assert(true  == testMatch(  "http://sample.com:80"  , incAuth=  "sample.com"))
    assert(false == testMatch(         "sample.com:443" , incAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample.com:443" , incAuth=  "sample.com"))
    assert(true  == testMatch( "https://sample.com:443" , incAuth=  "sample.com"))
    assert(false == testMatch(         "sample.com:8080", incAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample.com:8080", incAuth=  "sample.com"))
    assert(false == testMatch( "https://sample.com:8080", incAuth=  "sample.com"))
  }

  test(""" "" ports in exclude patterns match all ports for that domain, express or implied. """) {
    assert(false == testMatch(         "sample.com"     , excAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample.com"     , excAuth=  "sample.com"))
    assert(false == testMatch( "https://sample.com"     , excAuth=  "sample.com"))
    assert(false == testMatch(         "sample.com:80"  , excAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample.com:80"  , excAuth=  "sample.com"))
    assert(false == testMatch(         "sample.com:443" , excAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample.com:443" , excAuth=  "sample.com"))
    assert(false == testMatch( "https://sample.com:443" , excAuth=  "sample.com"))
    assert(false == testMatch(         "sample.com:8080", excAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample.com:8080", excAuth=  "sample.com"))
    assert(false == testMatch( "https://sample.com:8080", excAuth=  "sample.com"))
  }

  test(""" "" ports in exclude patterns match default ports (80/443) for other domains, express or implied. """) {
    assert(true  == testMatch(         "sample2.com"     , excAuth=  "sample.com"))
    assert(true  == testMatch(  "http://sample2.com"     , excAuth=  "sample.com"))
    assert(true  == testMatch( "https://sample2.com"     , excAuth=  "sample.com"))
    assert(true  == testMatch(         "sample2.com:80"  , excAuth=  "sample.com"))
    assert(true  == testMatch(  "http://sample2.com:80"  , excAuth=  "sample.com"))
    assert(false == testMatch(         "sample2.com:443" , excAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample2.com:443" , excAuth=  "sample.com"))
    assert(true  == testMatch( "https://sample2.com:443" , excAuth=  "sample.com"))
    assert(false == testMatch(         "sample2.com:8080", excAuth=  "sample.com"))
    assert(false == testMatch(  "http://sample2.com:8080", excAuth=  "sample.com"))
    assert(false == testMatch( "https://sample2.com:8080", excAuth=  "sample.com"))
  }

  test(""" "*" ports in include patterns match any port, express or implied. """) {
    assert(true  == testMatch(         "sample.com"     , incAuth=  "sample.com:*"))
    assert(true  == testMatch(  "http://sample.com"     , incAuth=  "sample.com:*"))
    assert(true  == testMatch( "https://sample.com"     , incAuth=  "sample.com:*"))
    assert(true  == testMatch(         "sample.com:80"  , incAuth=  "sample.com:*"))
    assert(true  == testMatch(  "http://sample.com:80"  , incAuth=  "sample.com:*"))
    assert(true  == testMatch(         "sample.com:443" , incAuth=  "sample.com:*"))
    assert(true  == testMatch(  "http://sample.com:443" , incAuth=  "sample.com:*"))
    assert(true  == testMatch( "https://sample.com:443" , incAuth=  "sample.com:*"))
    assert(true  == testMatch(         "sample.com:8080", incAuth=  "sample.com:*"))
    assert(true  == testMatch(  "http://sample.com:8080", incAuth=  "sample.com:*"))
    assert(true  == testMatch( "https://sample.com:8080", incAuth=  "sample.com:*"))
  }

  test(""" "*" ports in exclude patterns match any port, express or implied. """) {
    assert(false == testMatch(         "sample.com"     , excAuth=  "sample.com:*"))
    assert(false == testMatch(  "http://sample.com"     , excAuth=  "sample.com:*"))
    assert(false == testMatch( "https://sample.com"     , excAuth=  "sample.com:*"))
    assert(false == testMatch(         "sample.com:80"  , excAuth=  "sample.com:*"))
    assert(false == testMatch(  "http://sample.com:80"  , excAuth=  "sample.com:*"))
    assert(false == testMatch(         "sample.com:443" , excAuth=  "sample.com:*"))
    assert(false == testMatch(  "http://sample.com:443" , excAuth=  "sample.com:*"))
    assert(false == testMatch( "https://sample.com:443" , excAuth=  "sample.com:*"))
    assert(false == testMatch(         "sample.com:8080", excAuth=  "sample.com:*"))
    assert(false == testMatch(  "http://sample.com:8080", excAuth=  "sample.com:*"))
    assert(false == testMatch( "https://sample.com:8080", excAuth=  "sample.com:*"))
  }

  test(""" "*" ports in exclude patterns match default ports (80/443), express or implied, for other domains. """) {
    assert(true  == testMatch(         "sample2.com"     , excAuth=  "sample.com:*"))
    assert(true  == testMatch(  "http://sample2.com"     , excAuth=  "sample.com:*"))
    assert(true  == testMatch( "https://sample2.com"     , excAuth=  "sample.com:*"))
    assert(true  == testMatch(         "sample2.com:80"  , excAuth=  "sample.com:*"))
    assert(true  == testMatch(  "http://sample2.com:80"  , excAuth=  "sample.com:*"))
    assert(false == testMatch(         "sample2.com:443" , excAuth=  "sample.com:*"))
    assert(false == testMatch(  "http://sample2.com:443" , excAuth=  "sample.com:*"))
    assert(true  == testMatch( "https://sample2.com:443" , excAuth=  "sample.com:*"))
    assert(false == testMatch(         "sample2.com:8080", excAuth=  "sample.com:*"))
    assert(false == testMatch(  "http://sample2.com:8080", excAuth=  "sample.com:*"))
    assert(false == testMatch( "https://sample2.com:8080", excAuth=  "sample.com:*"))
  }

  test(""" ":*" exclude authority excludes all domains and ports. """) {
    assert(false == testMatch(         "sample.com"     , excAuth=  ":*"))
    assert(false == testMatch(  "http://sample.com"     , excAuth=  ":*"))
    assert(false == testMatch( "https://sample.com"     , excAuth=  ":*"))
    assert(false == testMatch(         "sample.com:80"  , excAuth=  ":*"))
    assert(false == testMatch(  "http://sample.com:80"  , excAuth=  ":*"))
    assert(false == testMatch(         "sample.com:443" , excAuth=  ":*"))
    assert(false == testMatch(  "http://sample.com:443" , excAuth=  ":*"))
    assert(false == testMatch( "https://sample.com:443" , excAuth=  ":*"))
    assert(false == testMatch(         "sample.com:8080", excAuth=  ":*"))
    assert(false == testMatch(  "http://sample.com:8080", excAuth=  ":*"))
    assert(false == testMatch( "https://sample.com:8080", excAuth=  ":*"))
  }

  test(""" Explicit include ports in patterns match correct express or implied ports. """) {
    assert(true  == testMatch(         "sample.com"     , incAuth=  "sample.com:80"))
    assert(true  == testMatch(  "http://sample.com"     , incAuth=  "sample.com:80"))
    assert(true  == testMatch( "https://sample.com:80"  , incAuth=  "sample.com:80"))
    assert(true  == testMatch(  "http://sample.com:443" , incAuth=  "sample.com:443"))
    assert(true  == testMatch( "https://sample.com"     , incAuth=  "sample.com:443"))
    assert(true  == testMatch(         "sample.com:80"  , incAuth=  "sample.com:80"))
    assert(true  == testMatch(         "sample.com:443" , incAuth=  "sample.com:443"))
    assert(true  == testMatch(         "sample.com:8080", incAuth=  "sample.com:8080"))
    assert(true  == testMatch(  "http://sample.com:8080", incAuth=  "sample.com:8080"))
    assert(true  == testMatch( "https://sample.com:8080", incAuth=  "sample.com:8080"))

    assert(false == testMatch(         "sample.com"     , incAuth=  "sample.com:81"))
    assert(false == testMatch(  "http://sample.com"     , incAuth=  "sample.com:81"))
    assert(false == testMatch( "https://sample.com"     , incAuth=  "sample.com:444"))
    assert(false == testMatch(         "sample.com:80"  , incAuth=  "sample.com:81"))
    assert(false == testMatch(         "sample.com:443" , incAuth=  "sample.com:444"))
    assert(false == testMatch(         "sample.com:8080", incAuth=  "sample.com:8081"))
  }

  test(""" Explicit exclude ports in patterns match correct express or implied ports. """) {
    assert(false == testMatch(         "sample.com"     , excAuth=  "sample.com:80"))
    assert(false == testMatch(  "http://sample.com"     , excAuth=  "sample.com:80"))
    assert(false == testMatch( "https://sample.com:80"  , excAuth=  "sample.com:80"))
    assert(false == testMatch(  "http://sample.com:443" , excAuth=  "sample.com:443"))
    assert(false == testMatch( "https://sample.com"     , excAuth=  "sample.com:443"))
    assert(false == testMatch(         "sample.com:80"  , excAuth=  "sample.com:80"))
    assert(false == testMatch(         "sample.com:443" , excAuth=  "sample.com:443"))
    assert(false == testMatch(         "sample.com:8080", excAuth=  "sample.com:8080"))
    assert(false == testMatch( "https://sample.com:8080", excAuth=  "sample.com:8080"))

    assert(true  == testMatch(         "sample.com"     , excAuth=  "sample.com:81"))
    assert(true  == testMatch(  "http://sample.com"     , excAuth=  "sample.com:81"))
    assert(true  == testMatch( "https://sample.com"     , excAuth=  "sample.com:444"))
    assert(true  == testMatch(         "sample.com:80"  , excAuth=  "sample.com:81"))
    assert(false == testMatch(         "sample.com:443" , excAuth=  "sample.com:444"))
    assert(false == testMatch(         "sample.com:8080", excAuth=  "sample.com:8081"))
  }

  test(""" Port-only patterns work as expected. """) {
    assert(true  == testMatch(         "sample.com"     , incAuth=  ":80"))
    assert(true  == testMatch(  "http://sample.com"     , incAuth=  ":80"))
    assert(true  == testMatch( "https://sample.com"     , incAuth=  ":443"))
    assert(true  == testMatch(         "sample.com:80"  , incAuth=  ":80"))
    assert(true  == testMatch(         "sample.com:443" , incAuth=  ":443"))
    assert(true  == testMatch(         "sample.com:8080", incAuth=  ":8080"))

    assert(false == testMatch(         "sample.com"     , incAuth=  ":81"))
    assert(false == testMatch(  "http://sample.com"     , incAuth=  ":81"))
    assert(false == testMatch( "https://sample.com"     , incAuth=  ":444"))
    assert(false == testMatch(         "sample.com:80"  , incAuth=  ":81"))
    assert(false == testMatch(         "sample.com:443" , incAuth=  ":444"))
    assert(false == testMatch(         "sample.com:8080", incAuth=  ":8081"))
  }

  test(""" Port-only exclude patterns work as expected. """) {
    assert(false == testMatch(         "sample.com"     , excAuth=  ":80"))
    assert(false == testMatch(  "http://sample.com"     , excAuth=  ":80"))
    assert(false == testMatch( "https://sample.com"     , excAuth=  ":443"))
    assert(false == testMatch(         "sample.com:80"  , excAuth=  ":80"))
    assert(false == testMatch(         "sample.com:443" , excAuth=  ":443"))
    assert(false == testMatch(         "sample.com:8080", excAuth=  ":8080"))

    assert(true  == testMatch(         "sample.com"     , excAuth=  ":81"))
    assert(true  == testMatch(  "http://sample.com"     , excAuth=  ":81"))
    assert(true  == testMatch( "https://sample.com"     , excAuth=  ":444"))
    assert(true  == testMatch(         "sample.com:80"  , excAuth=  ":81"))
    assert(false == testMatch(         "sample.com:443" , excAuth=  ":444"))
    assert(false == testMatch(         "sample.com:8080", excAuth=  ":8081"))
  }

  //
  // Paths
  //

  test(""" "" or "*" paths match everything. """) {
    assert(true  == testMatch("sample1.com"                , incPath=  ""))
    assert(true  == testMatch("sample2.com/"               , incPath=  ""))
    assert(true  == testMatch("sample3.com/dir1"           , incPath=  ""))
    assert(true  == testMatch("sample4.com/dir1/"          , incPath=  ""))
    assert(true  == testMatch("sample5.com/dir1/art1.html" , incPath=  ""))
    assert(true  == testMatch("sample5.com/dir1/art2.html" , incPath=  ""))

    assert(true  == testMatch("sample1.com"                , incPath=  "*"))
    assert(true  == testMatch("sample2.com/"               , incPath=  "*"))
    assert(true  == testMatch("sample3.com/dir1"           , incPath=  "*"))
    assert(true  == testMatch("sample4.com/dir1/"          , incPath=  "*"))
    assert(true  == testMatch("sample5.com/dir1/art1.html" , incPath=  "*"))
  }

  test(""" "*XXX" paths match any path ending with "XXX" """) {
    assert(false == testMatch("sample1.com"                , incPath=  "*.html"))
    assert(false == testMatch("sample2.com/"               , incPath=  "*.html"))
    assert(false == testMatch("sample3.com/dir1"           , incPath=  "*.html"))
    assert(false == testMatch("sample4.com/dir1/"          , incPath=  "*.html"))
    assert(true  == testMatch("sample5.com/dir1/art1.html" , incPath=  "*.html"))
    assert(true  == testMatch("sample6.com/dir1/art2.html" , incPath=  "*.html"))
    assert(false == testMatch("sample7.com/dir2"           , incPath=  "*.html"))
    assert(false == testMatch("sample8.com/dir2/"          , incPath=  "*.html"))
    assert(true  == testMatch("sample9.com/dir2/art1.html" , incPath=  "*.html"))
    assert(true  == testMatch("sample0.com/dir2/art2.html" , incPath=  "*.html"))
  }

  test(""" "XXX*" paths match any path prefixed by "XXX" (but does not match "XXX" itself) """) {
    assert(false == testMatch("sample1.com"                , incPath=  "/dir1/*"))
    assert(false == testMatch("sample2.com/"               , incPath=  "/dir1/*"))
    assert(false == testMatch("sample3.com/dir1"           , incPath=  "/dir1/*"))
    assert(false == testMatch("sample4.com/dir1/"          , incPath=  "/dir1/*"))
    assert(true  == testMatch("sample5.com/dir1/art1.html" , incPath=  "/dir1/*"))
    assert(true  == testMatch("sample6.com/dir1/art2.html" , incPath=  "/dir1/*"))
    assert(false == testMatch("sample7.com/dir2"           , incPath=  "/dir1/*"))
    assert(false == testMatch("sample8.com/dir2/"          , incPath=  "/dir1/*"))
    assert(false == testMatch("sample9.com/dir2/art1.html" , incPath=  "/dir1/*"))
    assert(false == testMatch("sample0.com/dir2/art2.html" , incPath=  "/dir1/*"))

    assert(false == testMatch("sample1.com"                , incPath=  "/dir1/*"))
    assert(false == testMatch("sample2.com/"               , incPath=  "/dir1/*"))
    assert(false == testMatch("sample3.com/dir1"           , incPath=  "/dir1/*"))
    assert(false == testMatch("sample4.com/dir1/"          , incPath=  "/dir1/*"))
    assert(true  == testMatch("sample5.com/dir1/art1.html" , incPath=  "/dir1/*"))
    assert(true  == testMatch("sample6.com/dir1/art2.html" , incPath=  "/dir1/*"))
    assert(false == testMatch("sample7.com/dir2"           , incPath=  "/dir1/*"))
    assert(false == testMatch("sample8.com/dir2/"          , incPath=  "/dir1/*"))
    assert(false == testMatch("sample9.com/dir2/art1.html" , incPath=  "/dir1/*"))
    assert(false == testMatch("sample0.com/dir2/art2.html" , incPath=  "/dir1/*"))
  }

  test(""" "*XXX*" paths match any path containing XXX (including XXX itself) """) {
    assert(false == testMatch("sample1.com"                , incPath=  "*/art*"))
    assert(false == testMatch("sample2.com/"               , incPath=  "*/art*"))
    assert(false == testMatch("sample3.com/dir1"           , incPath=  "*/art*"))
    assert(false == testMatch("sample4.com/dir1/"          , incPath=  "*/art*"))
    assert(true  == testMatch("sample5.com/dir1/art1.html" , incPath=  "*/art*"))
    assert(true  == testMatch("sample6.com/dir1/art2.html" , incPath=  "*/art*"))
    assert(false == testMatch("sample7.com/dir2"           , incPath=  "*/art*"))
    assert(false == testMatch("sample8.com/dir2/"          , incPath=  "*/art*"))
    assert(true  == testMatch("sample9.com/dir2/art1.html" , incPath=  "*/art*"))
    assert(true  == testMatch("sample0.com/dir2/art2.html" , incPath=  "*/art*"))

    assert(false == testMatch("sample4.com/dir1/"          , incPath=  "*/dir1/*"))
    assert(true  == testMatch("sample5.com/dir1/art1.html" , incPath=  "*/dir1/*"))
  }

  test(""" "start*finish" matches anything starting with "start" and ending with "finish", including "startfinish". """) {
    assert(false == testMatch("sample1.com"                , incPath=  "/dir1/*art1.html"))
    assert(false == testMatch("sample2.com/"               , incPath=  "/dir1/*art1.html"))
    assert(false == testMatch("sample3.com/dir1"           , incPath=  "/dir1/*art1.html"))
    assert(false == testMatch("sample4.com/dir1/"          , incPath=  "/dir1/*art1.html"))
    assert(true  == testMatch("sample5.com/dir1/art1.html" , incPath=  "/dir1/*art1.html"))
    assert(false == testMatch("sample6.com/dir1/art2.html" , incPath=  "/dir1/*art1.html"))
    assert(false == testMatch("sample7.com/dir2"           , incPath=  "/dir1/*art1.html"))
    assert(false == testMatch("sample8.com/dir2/"          , incPath=  "/dir1/*art1.html"))
    assert(false == testMatch("sample9.com/dir2/art1.html" , incPath=  "/dir1/*art1.html"))
    assert(false == testMatch("sample0.com/dir2/art2.html" , incPath=  "/dir1/*art1.html"))

    assert(false == testMatch("sample1.com"                , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample2.com/"               , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample3.com/dir1"           , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample4.com/dir1/"          , incPath=  "/dir1/*.html"))
    assert(true  == testMatch("sample5.com/dir1/art1.html" , incPath=  "/dir1/*.html"))
    assert(true  == testMatch("sample6.com/dir1/art2.html" , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample7.com/dir2"           , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample8.com/dir2/"          , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample9.com/dir2/art1.html" , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample0.com/dir2/art2.html" , incPath=  "/dir1/*.html"))
  }

  test(""" "start*to*finish" matches anything starting with "start" optional characters, then "to", optional characters, and ending with "finish", including "starttofinish". """) {
    assert(false == testMatch("sample1.com"                , incPath=  "/dir1/*art*.html"))
    assert(false == testMatch("sample2.com/"               , incPath=  "/dir1/*art*.html"))
    assert(false == testMatch("sample3.com/dir1"           , incPath=  "/dir1/*art*.html"))
    assert(false == testMatch("sample4.com/dir1/"          , incPath=  "/dir1/*art*.html"))
    assert(true  == testMatch("sample5.com/dir1/art1.html" , incPath=  "/dir1/*art*.html"))
    assert(true  == testMatch("sample6.com/dir1/art2.html" , incPath=  "/dir1/*art*.html"))
    assert(false == testMatch("sample5.com/dir1/aNO1.html" , incPath=  "/dir1/*art*.html"))
    assert(false == testMatch("sample6.com/dir1/aNO2.html" , incPath=  "/dir1/*art*.html"))
    assert(false == testMatch("sample7.com/dir2"           , incPath=  "/dir1/*art*.html"))
    assert(false == testMatch("sample8.com/dir2/"          , incPath=  "/dir1/*art*.html"))
    assert(false == testMatch("sample9.com/dir2/art1.html" , incPath=  "/dir1/*art*.html"))
    assert(false == testMatch("sample0.com/dir2/art2.html" , incPath=  "/dir1/*art*.html"))

    assert(false == testMatch("sample1.com"                , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample2.com/"               , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample3.com/dir1"           , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample4.com/dir1/"          , incPath=  "/dir1/*.html"))
    assert(true  == testMatch("sample5.com/dir1/art1.html" , incPath=  "/dir1/*.html"))
    assert(true  == testMatch("sample6.com/dir1/art2.html" , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample7.com/dir2"           , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample8.com/dir2/"          , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample9.com/dir2/art1.html" , incPath=  "/dir1/*.html"))
    assert(false == testMatch("sample0.com/dir2/art2.html" , incPath=  "/dir1/*.html"))
  }

  test(""" "XXX" paths do an exact match. """) {
    assert(true  == testMatch("sample4.com/dir1/"          , incPath=  "/dir1/"))
    assert(false == testMatch("sample5.com/dir1/art1.html" , incPath=  "/dir1/"))
    assert(false == testMatch("sample6.com/dir1/art1.html" , incPath=  "art1.html"))
  }

  test("""The claims in the code comments should be true""") {

    // "*wanted*" matches anything with "wanted" in it, including "wanted" itself.
    assert(false == testMatch("sample3.com/wante"            , incPath=  "*/wanted*"))
    assert(true  == testMatch("sample3.com/wanted"           , incPath=  "*/wanted*"))
    assert(true  == testMatch("sample3.com/wanted/"          , incPath=  "*/wanted*"))

    assert(false == testMatch("sample3.com/pre/wante"        , incPath=  "*/wanted*"))
    assert(true  == testMatch("sample3.com/pre/wanted"       , incPath=  "*/wanted*"))
    assert(true  == testMatch("sample3.com/pre/wanted/"      , incPath=  "*/wanted*"))

    // "*wanted/*" matches anything with "wanted/" in it, NOT including "wanted/" itself.
    assert(false == testMatch("sample3.com/wanted"           , incPath=  "*/wanted/*"))
    assert(false == testMatch("sample3.com/wanted/"          , incPath=  "*/wanted/*"))   // Don't match self
    assert(true  == testMatch("sample3.com/wanted/bork"      , incPath=  "*/wanted/*"))

    assert(false == testMatch("sample3.com/pre/wanted"       , incPath=  "*/wanted/*"))
    assert(false == testMatch("sample3.com/pre/wanted/"      , incPath=  "*/wanted/*"))   // Don't match self
    assert(true  == testMatch("sample3.com/pre/wanted/bork"  , incPath=  "*/wanted/*"))

    // "*wanted*" matches anything starting with "wanted", including "wanted" itself.
    assert(false == testMatch("sample3.com/wante"            , incPath=  "/wanted*"))
    assert(true  == testMatch("sample3.com/wanted"           , incPath=  "/wanted*"))
    assert(true  == testMatch("sample3.com/wanted/"          , incPath=  "/wanted*"))

    assert(false == testMatch("sample3.com/pre/wante"        , incPath=  "/wanted*"))
    assert(false  == testMatch("sample3.com/pre/wanted"      , incPath=  "/wanted*"))
    assert(false  == testMatch("sample3.com/pre/wanted/"     , incPath=  "/wanted*"))

    // "*wanted/*" matches anything starting with "wanted/", NOT including "wanted/" itself.
    assert(false == testMatch("sample3.com/wanted"           , incPath=  "/wanted/*"))
    assert(false  == testMatch("sample3.com/wanted/"         , incPath=  "/wanted/*"))   // Don't match self
    assert(true  == testMatch("sample3.com/wanted/bork"      , incPath=  "/wanted/*"))

    assert(false == testMatch("sample3.com/pre/wanted"       , incPath=  "/wanted/*"))
    assert(false == testMatch("sample3.com/pre/wanted/"      , incPath=  "/wanted/*"))   // Don't match self
    assert(false  == testMatch("sample3.com/pre/wanted/bork" , incPath=  "/wanted/*"))

    // "*wanted" matches anything ending with "wanted", including "wanted" itself.
    assert(false == testMatch("sample3.com/wante"            , incPath=  "*wanted"))
    assert(true  == testMatch("sample3.com/wanted"           , incPath=  "*wanted"))
    assert(false  == testMatch("sample3.com/wanted/"         , incPath=  "*wanted"))

    assert(false == testMatch("sample3.com/wante/post"       , incPath=  "*wanted"))
    assert(false == testMatch("sample3.com/wanted/post"      , incPath=  "*wanted"))
    assert(false  == testMatch("sample3.com/wanted/post"     , incPath=  "*wanted"))

    // "*wanted/" matches anything ending with "wanted/", including "wanted/" itself.
    assert(false == testMatch("sample3.com/wante"            , incPath=  "*/wanted"))
    assert(true  == testMatch("sample3.com/wanted"           , incPath=  "*/wanted"))
    assert(false  == testMatch("sample3.com/wanted/"         , incPath=  "*/wanted"))

    assert(false == testMatch("sample3.com/wante/post"       , incPath=  "*/wanted"))
    assert(false == testMatch("sample3.com/wanted/post"      , incPath=  "*/wanted"))
    assert(false  == testMatch("sample3.com/wanted/post"     , incPath=  "*/wanted"))
  }

  //
  // Composing Authorities and Paths
  //
  test(""" Includes activate on any-auth AND any-path match, excludes activate on any-auth OR any-path match """) {
    assert(true  == testMatch("www.sample1.com", incExcUrlStrs(incAuths=List("www.sample1.com", "www.sample2.com"))))
    assert(true  == testMatch("www.sample2.com", incExcUrlStrs(incAuths=List("www.sample1.com", "www.sample2.com"))))
    assert(false == testMatch("www.sample3.com", incExcUrlStrs(incAuths=List("www.sample1.com", "www.sample2.com"))))

    assert(true  == testMatch("www.sample1.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch("www.sample2.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch("www.sample1.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch("www.sample2.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample1.com/God", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample2.com/God", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com/God", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
  }

  test(""" Includes activate on any-auth AND any-path match, but fails non-default ports """) {

    assert(true  == testMatch(     "www.sample1.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch(     "www.sample2.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample3.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch(     "www.sample1.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch(     "www.sample2.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample3.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample1.com/God", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample2.com/God", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample3.com/God", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))

    assert(false == testMatch("www.sample1.com:8080/cat", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample2.com:8080/cat", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com:8080/cat", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample1.com:8080/dog", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample2.com:8080/dog", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com:8080/dog", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample1.com:8080/God", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample2.com:8080/God", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com:8080/God", incExcUrlStrs(incAuths=List("www.sample1.*", "www.sample2.*"), incPaths=List("/cat", "/dog"))))

  }

  test(""" Includes activate on any-auth(w/ any-port) AND any-path match """) {

    assert(true  == testMatch(     "www.sample1.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch(     "www.sample2.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample3.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch(     "www.sample1.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch(     "www.sample2.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample3.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample1.com/God", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample2.com/God", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample3.com/God", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))

    assert(true  == testMatch("www.sample1.com:8080/cat", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch("www.sample2.com:8080/cat", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com:8080/cat", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch("www.sample1.com:8080/dog", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch("www.sample2.com:8080/dog", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com:8080/dog", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample1.com:8080/God", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample2.com:8080/God", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com:8080/God", incExcUrlStrs(incAuths=List("www.sample1.*:*", "www.sample2.*:*"), incPaths=List("/cat", "/dog"))))
  }

  test(""" Includes activate on correct port with any-auth(w/ specific-port) AND any-path match """) {

    assert(false == testMatch(     "www.sample1.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample2.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample3.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample1.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample2.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample3.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample1.com/God", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample2.com/God", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample3.com/God", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))

    assert(true  == testMatch("www.sample1.com:8080/cat", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch("www.sample2.com:8080/cat", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com:8080/cat", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch("www.sample1.com:8080/dog", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch("www.sample2.com:8080/dog", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com:8080/dog", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample1.com:8080/God", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample2.com:8080/God", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com:8080/God", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*:8080"), incPaths=List("/cat", "/dog"))))
  }

  test(""" Includes activate correct any-auth(one w/ specific-port) AND any-path match """) {

    assert(false == testMatch(     "www.sample1.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch(     "www.sample2.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample3.com/cat", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample1.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch(     "www.sample2.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample3.com/dog", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample1.com/God", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample2.com/God", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch(     "www.sample3.com/God", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))

    assert(true  == testMatch("www.sample1.com:8080/cat", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample2.com:8080/cat", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com:8080/cat", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(true  == testMatch("www.sample1.com:8080/dog", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample2.com:8080/dog", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com:8080/dog", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample1.com:8080/God", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample2.com:8080/God", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
    assert(false == testMatch("www.sample3.com:8080/God", incExcUrlStrs(incAuths=List("www.sample1.*:8080", "www.sample2.*"), incPaths=List("/cat", "/dog"))))
  }
  //
  // Includes vs. Excludes
  //
  // Add more
  test(""" Port only include pattern plus any-auth exclude pattern applied first inclusive then exclusive """) {
    assert(false == testMatch("www.sample1.com"     , incExcUrlStrs(incAuths=List("*:8080"), excAuths=List("www.sample2.*"))))
    assert(true  == testMatch("www.sample1.com:8080", incExcUrlStrs(incAuths=List("*:8080"), excAuths=List("www.sample2.*"))))
    assert(false == testMatch("www.sample2.com"     , incExcUrlStrs(incAuths=List("*:8080"), excAuths=List("www.sample2.*"))))
    assert(false == testMatch("www.sample2.com:8080", incExcUrlStrs(incAuths=List("*:8080"), excAuths=List("www.sample2.*"))))
  }

  test(""" Auth without port (matches protocol default port) plus exclude port 80 rejects http and non-standard ports """) {
    assert(false == testMatch(        "www.sample1.com"     , incExcUrlStrs(incAuths=List("www.sample1.*"), excAuths=List(":80"))))
    assert(false == testMatch(        "www.sample1.com:8080", incExcUrlStrs(incAuths=List("www.sample1.*"), excAuths=List(":80"))))
    assert(true  == testMatch("https://www.sample1.com"     , incExcUrlStrs(incAuths=List("www.sample1.*"), excAuths=List(":80"))))
    assert(false == testMatch("https://www.sample1.com:8080", incExcUrlStrs(incAuths=List("www.sample1.*"), excAuths=List(":80"))))
    assert(false == testMatch(        "www.sample2.com"     , incExcUrlStrs(incAuths=List("www.sample1.*"), excAuths=List(":80"))))
    assert(false == testMatch(        "www.sample2.com:8080", incExcUrlStrs(incAuths=List("www.sample1.*"), excAuths=List(":80"))))
    assert(false == testMatch("https://www.sample2.com"     , incExcUrlStrs(incAuths=List("www.sample1.*"), excAuths=List(":80"))))
    assert(false == testMatch("https://www.sample2.com:8080", incExcUrlStrs(incAuths=List("www.sample1.*"), excAuths=List(":80"))))
  }


  //
  // WebMD Tests:
  //
  test(""" WebMD Exclusion Patterns work correctly """) {
    assert(false == testMatch("http://www.webmd.com/depression/tc/credits-depression", incExcUrlStrs(excPaths=List("*/credits-*"))))
  }

  //
  // Moviefone Tests:
  //
  test("""Moviefone "Exclude Canada" works correctly""") {
    assert(false == testMatch(    "http://news.moviefone.ca/2014/12/30/where-is-the-interview-playing-in-canada/", excAuth="http://news.moviefone.ca/"))
    assert(true  == testMatch(    "http://news.moviefone.XX/2014/12/30/where-is-the-interview-playing-in-canada/", excAuth="http://news.moviefone.ca/"))

    assert(false == testMatch(    "http://news.moviefone.ca/2014/12/30/where-is-the-interview-playing-in-canada/", excAuth="http://news.moviefone.ca"))
    assert(true  == testMatch(    "http://news.moviefone.XX/2014/12/30/where-is-the-interview-playing-in-canada/", excAuth="http://news.moviefone.ca"))
  }
}

/*
Here is a sample generation of allSampleUrlStrs:

http://ab.sample.com
http://ab.sample.com/
http://ab.sample.com/art1.html
http://ab.sample.com/art2.html
http://ab.sample.com/dir1
http://ab.sample.com/dir1/
http://ab.sample.com/dir1/art1.html
http://ab.sample.com/dir1/art2.html
http://ab.sample.com/dir1/dira
http://ab.sample.com/dir1/dira/
http://ab.sample.com/dir1/dira/art1.html
http://ab.sample.com/dir1/dira/art2.html
http://ab.sample.com/dir1/dirb
http://ab.sample.com/dir1/dirb/
http://ab.sample.com/dir1/dirb/art1.html
http://ab.sample.com/dir1/dirb/art2.html
http://ab.sample.com/dir2
http://ab.sample.com/dir2/
http://ab.sample.com/dir2/art1.html
http://ab.sample.com/dir2/art2.html
http://ab.sample.com/dir2/dira
http://ab.sample.com/dir2/dira/
http://ab.sample.com/dir2/dira/art1.html
http://ab.sample.com/dir2/dira/art2.html
http://ab.sample.com/dir2/dirb
http://ab.sample.com/dir2/dirb/
http://ab.sample.com/dir2/dirb/art1.html
http://ab.sample.com/dir2/dirb/art2.html
http://ab.sample.com/dira
http://ab.sample.com/dira/
http://ab.sample.com/dira/art1.html
http://ab.sample.com/dira/art2.html
http://ab.sample.com/dirb
http://ab.sample.com/dirb/
http://ab.sample.com/dirb/art1.html
http://ab.sample.com/dirb/art2.html
http://ab.sample.com:80
http://ab.sample.com:80/
http://ab.sample.com:80/art1.html
http://ab.sample.com:80/art2.html
http://ab.sample.com:80/dir1
http://ab.sample.com:80/dir1/
http://ab.sample.com:80/dir1/art1.html
http://ab.sample.com:80/dir1/art2.html
http://ab.sample.com:80/dir1/dira
http://ab.sample.com:80/dir1/dira/
http://ab.sample.com:80/dir1/dira/art1.html
http://ab.sample.com:80/dir1/dira/art2.html
http://ab.sample.com:80/dir1/dirb
http://ab.sample.com:80/dir1/dirb/
http://ab.sample.com:80/dir1/dirb/art1.html
http://ab.sample.com:80/dir1/dirb/art2.html
http://ab.sample.com:80/dir2
http://ab.sample.com:80/dir2/
http://ab.sample.com:80/dir2/art1.html
http://ab.sample.com:80/dir2/art2.html
http://ab.sample.com:80/dir2/dira
http://ab.sample.com:80/dir2/dira/
http://ab.sample.com:80/dir2/dira/art1.html
http://ab.sample.com:80/dir2/dira/art2.html
http://ab.sample.com:80/dir2/dirb
http://ab.sample.com:80/dir2/dirb/
http://ab.sample.com:80/dir2/dirb/art1.html
http://ab.sample.com:80/dir2/dirb/art2.html
http://ab.sample.com:80/dira
http://ab.sample.com:80/dira/
http://ab.sample.com:80/dira/art1.html
http://ab.sample.com:80/dira/art2.html
http://ab.sample.com:80/dirb
http://ab.sample.com:80/dirb/
http://ab.sample.com:80/dirb/art1.html
http://ab.sample.com:80/dirb/art2.html
http://ab.sample.com:8080
http://ab.sample.com:8080/
http://ab.sample.com:8080/art1.html
http://ab.sample.com:8080/art2.html
http://ab.sample.com:8080/dir1
http://ab.sample.com:8080/dir1/
http://ab.sample.com:8080/dir1/art1.html
http://ab.sample.com:8080/dir1/art2.html
http://ab.sample.com:8080/dir1/dira
http://ab.sample.com:8080/dir1/dira/
http://ab.sample.com:8080/dir1/dira/art1.html
http://ab.sample.com:8080/dir1/dira/art2.html
http://ab.sample.com:8080/dir1/dirb
http://ab.sample.com:8080/dir1/dirb/
http://ab.sample.com:8080/dir1/dirb/art1.html
http://ab.sample.com:8080/dir1/dirb/art2.html
http://ab.sample.com:8080/dir2
http://ab.sample.com:8080/dir2/
http://ab.sample.com:8080/dir2/art1.html
http://ab.sample.com:8080/dir2/art2.html
http://ab.sample.com:8080/dir2/dira
http://ab.sample.com:8080/dir2/dira/
http://ab.sample.com:8080/dir2/dira/art1.html
http://ab.sample.com:8080/dir2/dira/art2.html
http://ab.sample.com:8080/dir2/dirb
http://ab.sample.com:8080/dir2/dirb/
http://ab.sample.com:8080/dir2/dirb/art1.html
http://ab.sample.com:8080/dir2/dirb/art2.html
http://ab.sample.com:8080/dira
http://ab.sample.com:8080/dira/
http://ab.sample.com:8080/dira/art1.html
http://ab.sample.com:8080/dira/art2.html
http://ab.sample.com:8080/dirb
http://ab.sample.com:8080/dirb/
http://ab.sample.com:8080/dirb/art1.html
http://ab.sample.com:8080/dirb/art2.html
http://cde.sample.com
http://cde.sample.com/
http://cde.sample.com/art1.html
http://cde.sample.com/art2.html
http://cde.sample.com/dir1
http://cde.sample.com/dir1/
http://cde.sample.com/dir1/art1.html
http://cde.sample.com/dir1/art2.html
http://cde.sample.com/dir1/dira
http://cde.sample.com/dir1/dira/
http://cde.sample.com/dir1/dira/art1.html
http://cde.sample.com/dir1/dira/art2.html
http://cde.sample.com/dir1/dirb
http://cde.sample.com/dir1/dirb/
http://cde.sample.com/dir1/dirb/art1.html
http://cde.sample.com/dir1/dirb/art2.html
http://cde.sample.com/dir2
http://cde.sample.com/dir2/
http://cde.sample.com/dir2/art1.html
http://cde.sample.com/dir2/art2.html
http://cde.sample.com/dir2/dira
http://cde.sample.com/dir2/dira/
http://cde.sample.com/dir2/dira/art1.html
http://cde.sample.com/dir2/dira/art2.html
http://cde.sample.com/dir2/dirb
http://cde.sample.com/dir2/dirb/
http://cde.sample.com/dir2/dirb/art1.html
http://cde.sample.com/dir2/dirb/art2.html
http://cde.sample.com/dira
http://cde.sample.com/dira/
http://cde.sample.com/dira/art1.html
http://cde.sample.com/dira/art2.html
http://cde.sample.com/dirb
http://cde.sample.com/dirb/
http://cde.sample.com/dirb/art1.html
http://cde.sample.com/dirb/art2.html
http://cde.sample.com:80
http://cde.sample.com:80/
http://cde.sample.com:80/art1.html
http://cde.sample.com:80/art2.html
http://cde.sample.com:80/dir1
http://cde.sample.com:80/dir1/
http://cde.sample.com:80/dir1/art1.html
http://cde.sample.com:80/dir1/art2.html
http://cde.sample.com:80/dir1/dira
http://cde.sample.com:80/dir1/dira/
http://cde.sample.com:80/dir1/dira/art1.html
http://cde.sample.com:80/dir1/dira/art2.html
http://cde.sample.com:80/dir1/dirb
http://cde.sample.com:80/dir1/dirb/
http://cde.sample.com:80/dir1/dirb/art1.html
http://cde.sample.com:80/dir1/dirb/art2.html
http://cde.sample.com:80/dir2
http://cde.sample.com:80/dir2/
http://cde.sample.com:80/dir2/art1.html
http://cde.sample.com:80/dir2/art2.html
http://cde.sample.com:80/dir2/dira
http://cde.sample.com:80/dir2/dira/
http://cde.sample.com:80/dir2/dira/art1.html
http://cde.sample.com:80/dir2/dira/art2.html
http://cde.sample.com:80/dir2/dirb
http://cde.sample.com:80/dir2/dirb/
http://cde.sample.com:80/dir2/dirb/art1.html
http://cde.sample.com:80/dir2/dirb/art2.html
http://cde.sample.com:80/dira
http://cde.sample.com:80/dira/
http://cde.sample.com:80/dira/art1.html
http://cde.sample.com:80/dira/art2.html
http://cde.sample.com:80/dirb
http://cde.sample.com:80/dirb/
http://cde.sample.com:80/dirb/art1.html
http://cde.sample.com:80/dirb/art2.html
http://cde.sample.com:8080
http://cde.sample.com:8080/
http://cde.sample.com:8080/art1.html
http://cde.sample.com:8080/art2.html
http://cde.sample.com:8080/dir1
http://cde.sample.com:8080/dir1/
http://cde.sample.com:8080/dir1/art1.html
http://cde.sample.com:8080/dir1/art2.html
http://cde.sample.com:8080/dir1/dira
http://cde.sample.com:8080/dir1/dira/
http://cde.sample.com:8080/dir1/dira/art1.html
http://cde.sample.com:8080/dir1/dira/art2.html
http://cde.sample.com:8080/dir1/dirb
http://cde.sample.com:8080/dir1/dirb/
http://cde.sample.com:8080/dir1/dirb/art1.html
http://cde.sample.com:8080/dir1/dirb/art2.html
http://cde.sample.com:8080/dir2
http://cde.sample.com:8080/dir2/
http://cde.sample.com:8080/dir2/art1.html
http://cde.sample.com:8080/dir2/art2.html
http://cde.sample.com:8080/dir2/dira
http://cde.sample.com:8080/dir2/dira/
http://cde.sample.com:8080/dir2/dira/art1.html
http://cde.sample.com:8080/dir2/dira/art2.html
http://cde.sample.com:8080/dir2/dirb
http://cde.sample.com:8080/dir2/dirb/
http://cde.sample.com:8080/dir2/dirb/art1.html
http://cde.sample.com:8080/dir2/dirb/art2.html
http://cde.sample.com:8080/dira
http://cde.sample.com:8080/dira/
http://cde.sample.com:8080/dira/art1.html
http://cde.sample.com:8080/dira/art2.html
http://cde.sample.com:8080/dirb
http://cde.sample.com:8080/dirb/
http://cde.sample.com:8080/dirb/art1.html
http://cde.sample.com:8080/dirb/art2.html
http://sample.com
http://sample.com/
http://sample.com/art1.html
http://sample.com/art2.html
http://sample.com/dir1
http://sample.com/dir1/
http://sample.com/dir1/art1.html
http://sample.com/dir1/art2.html
http://sample.com/dir1/dira
http://sample.com/dir1/dira/
http://sample.com/dir1/dira/art1.html
http://sample.com/dir1/dira/art2.html
http://sample.com/dir1/dirb
http://sample.com/dir1/dirb/
http://sample.com/dir1/dirb/art1.html
http://sample.com/dir1/dirb/art2.html
http://sample.com/dir2
http://sample.com/dir2/
http://sample.com/dir2/art1.html
http://sample.com/dir2/art2.html
http://sample.com/dir2/dira
http://sample.com/dir2/dira/
http://sample.com/dir2/dira/art1.html
http://sample.com/dir2/dira/art2.html
http://sample.com/dir2/dirb
http://sample.com/dir2/dirb/
http://sample.com/dir2/dirb/art1.html
http://sample.com/dir2/dirb/art2.html
http://sample.com/dira
http://sample.com/dira/
http://sample.com/dira/art1.html
http://sample.com/dira/art2.html
http://sample.com/dirb
http://sample.com/dirb/
http://sample.com/dirb/art1.html
http://sample.com/dirb/art2.html
http://sample.com:80
http://sample.com:80/
http://sample.com:80/art1.html
http://sample.com:80/art2.html
http://sample.com:80/dir1
http://sample.com:80/dir1/
http://sample.com:80/dir1/art1.html
http://sample.com:80/dir1/art2.html
http://sample.com:80/dir1/dira
http://sample.com:80/dir1/dira/
http://sample.com:80/dir1/dira/art1.html
http://sample.com:80/dir1/dira/art2.html
http://sample.com:80/dir1/dirb
http://sample.com:80/dir1/dirb/
http://sample.com:80/dir1/dirb/art1.html
http://sample.com:80/dir1/dirb/art2.html
http://sample.com:80/dir2
http://sample.com:80/dir2/
http://sample.com:80/dir2/art1.html
http://sample.com:80/dir2/art2.html
http://sample.com:80/dir2/dira
http://sample.com:80/dir2/dira/
http://sample.com:80/dir2/dira/art1.html
http://sample.com:80/dir2/dira/art2.html
http://sample.com:80/dir2/dirb
http://sample.com:80/dir2/dirb/
http://sample.com:80/dir2/dirb/art1.html
http://sample.com:80/dir2/dirb/art2.html
http://sample.com:80/dira
http://sample.com:80/dira/
http://sample.com:80/dira/art1.html
http://sample.com:80/dira/art2.html
http://sample.com:80/dirb
http://sample.com:80/dirb/
http://sample.com:80/dirb/art1.html
http://sample.com:80/dirb/art2.html
http://sample.com:8080
http://sample.com:8080/
http://sample.com:8080/art1.html
http://sample.com:8080/art2.html
http://sample.com:8080/dir1
http://sample.com:8080/dir1/
http://sample.com:8080/dir1/art1.html
http://sample.com:8080/dir1/art2.html
http://sample.com:8080/dir1/dira
http://sample.com:8080/dir1/dira/
http://sample.com:8080/dir1/dira/art1.html
http://sample.com:8080/dir1/dira/art2.html
http://sample.com:8080/dir1/dirb
http://sample.com:8080/dir1/dirb/
http://sample.com:8080/dir1/dirb/art1.html
http://sample.com:8080/dir1/dirb/art2.html
http://sample.com:8080/dir2
http://sample.com:8080/dir2/
http://sample.com:8080/dir2/art1.html
http://sample.com:8080/dir2/art2.html
http://sample.com:8080/dir2/dira
http://sample.com:8080/dir2/dira/
http://sample.com:8080/dir2/dira/art1.html
http://sample.com:8080/dir2/dira/art2.html
http://sample.com:8080/dir2/dirb
http://sample.com:8080/dir2/dirb/
http://sample.com:8080/dir2/dirb/art1.html
http://sample.com:8080/dir2/dirb/art2.html
http://sample.com:8080/dira
http://sample.com:8080/dira/
http://sample.com:8080/dira/art1.html
http://sample.com:8080/dira/art2.html
http://sample.com:8080/dirb
http://sample.com:8080/dirb/
http://sample.com:8080/dirb/art1.html
http://sample.com:8080/dirb/art2.html
http://www.sample.com
http://www.sample.com/
http://www.sample.com/art1.html
http://www.sample.com/art2.html
http://www.sample.com/dir1
http://www.sample.com/dir1/
http://www.sample.com/dir1/art1.html
http://www.sample.com/dir1/art2.html
http://www.sample.com/dir1/dira
http://www.sample.com/dir1/dira/
http://www.sample.com/dir1/dira/art1.html
http://www.sample.com/dir1/dira/art2.html
http://www.sample.com/dir1/dirb
http://www.sample.com/dir1/dirb/
http://www.sample.com/dir1/dirb/art1.html
http://www.sample.com/dir1/dirb/art2.html
http://www.sample.com/dir2
http://www.sample.com/dir2/
http://www.sample.com/dir2/art1.html
http://www.sample.com/dir2/art2.html
http://www.sample.com/dir2/dira
http://www.sample.com/dir2/dira/
http://www.sample.com/dir2/dira/art1.html
http://www.sample.com/dir2/dira/art2.html
http://www.sample.com/dir2/dirb
http://www.sample.com/dir2/dirb/
http://www.sample.com/dir2/dirb/art1.html
http://www.sample.com/dir2/dirb/art2.html
http://www.sample.com/dira
http://www.sample.com/dira/
http://www.sample.com/dira/art1.html
http://www.sample.com/dira/art2.html
http://www.sample.com/dirb
http://www.sample.com/dirb/
http://www.sample.com/dirb/art1.html
http://www.sample.com/dirb/art2.html
http://www.sample.com:80
http://www.sample.com:80/
http://www.sample.com:80/art1.html
http://www.sample.com:80/art2.html
http://www.sample.com:80/dir1
http://www.sample.com:80/dir1/
http://www.sample.com:80/dir1/art1.html
http://www.sample.com:80/dir1/art2.html
http://www.sample.com:80/dir1/dira
http://www.sample.com:80/dir1/dira/
http://www.sample.com:80/dir1/dira/art1.html
http://www.sample.com:80/dir1/dira/art2.html
http://www.sample.com:80/dir1/dirb
http://www.sample.com:80/dir1/dirb/
http://www.sample.com:80/dir1/dirb/art1.html
http://www.sample.com:80/dir1/dirb/art2.html
http://www.sample.com:80/dir2
http://www.sample.com:80/dir2/
http://www.sample.com:80/dir2/art1.html
http://www.sample.com:80/dir2/art2.html
http://www.sample.com:80/dir2/dira
http://www.sample.com:80/dir2/dira/
http://www.sample.com:80/dir2/dira/art1.html
http://www.sample.com:80/dir2/dira/art2.html
http://www.sample.com:80/dir2/dirb
http://www.sample.com:80/dir2/dirb/
http://www.sample.com:80/dir2/dirb/art1.html
http://www.sample.com:80/dir2/dirb/art2.html
http://www.sample.com:80/dira
http://www.sample.com:80/dira/
http://www.sample.com:80/dira/art1.html
http://www.sample.com:80/dira/art2.html
http://www.sample.com:80/dirb
http://www.sample.com:80/dirb/
http://www.sample.com:80/dirb/art1.html
http://www.sample.com:80/dirb/art2.html
http://www.sample.com:8080
http://www.sample.com:8080/
http://www.sample.com:8080/art1.html
http://www.sample.com:8080/art2.html
http://www.sample.com:8080/dir1
http://www.sample.com:8080/dir1/
http://www.sample.com:8080/dir1/art1.html
http://www.sample.com:8080/dir1/art2.html
http://www.sample.com:8080/dir1/dira
http://www.sample.com:8080/dir1/dira/
http://www.sample.com:8080/dir1/dira/art1.html
http://www.sample.com:8080/dir1/dira/art2.html
http://www.sample.com:8080/dir1/dirb
http://www.sample.com:8080/dir1/dirb/
http://www.sample.com:8080/dir1/dirb/art1.html
http://www.sample.com:8080/dir1/dirb/art2.html
http://www.sample.com:8080/dir2
http://www.sample.com:8080/dir2/
http://www.sample.com:8080/dir2/art1.html
http://www.sample.com:8080/dir2/art2.html
http://www.sample.com:8080/dir2/dira
http://www.sample.com:8080/dir2/dira/
http://www.sample.com:8080/dir2/dira/art1.html
http://www.sample.com:8080/dir2/dira/art2.html
http://www.sample.com:8080/dir2/dirb
http://www.sample.com:8080/dir2/dirb/
http://www.sample.com:8080/dir2/dirb/art1.html
http://www.sample.com:8080/dir2/dirb/art2.html
http://www.sample.com:8080/dira
http://www.sample.com:8080/dira/
http://www.sample.com:8080/dira/art1.html
http://www.sample.com:8080/dira/art2.html
http://www.sample.com:8080/dirb
http://www.sample.com:8080/dirb/
http://www.sample.com:8080/dirb/art1.html
http://www.sample.com:8080/dirb/art2.html
https://ab.sample.com
https://ab.sample.com/
https://ab.sample.com/art1.html
https://ab.sample.com/art2.html
https://ab.sample.com/dir1
https://ab.sample.com/dir1/
https://ab.sample.com/dir1/art1.html
https://ab.sample.com/dir1/art2.html
https://ab.sample.com/dir1/dira
https://ab.sample.com/dir1/dira/
https://ab.sample.com/dir1/dira/art1.html
https://ab.sample.com/dir1/dira/art2.html
https://ab.sample.com/dir1/dirb
https://ab.sample.com/dir1/dirb/
https://ab.sample.com/dir1/dirb/art1.html
https://ab.sample.com/dir1/dirb/art2.html
https://ab.sample.com/dir2
https://ab.sample.com/dir2/
https://ab.sample.com/dir2/art1.html
https://ab.sample.com/dir2/art2.html
https://ab.sample.com/dir2/dira
https://ab.sample.com/dir2/dira/
https://ab.sample.com/dir2/dira/art1.html
https://ab.sample.com/dir2/dira/art2.html
https://ab.sample.com/dir2/dirb
https://ab.sample.com/dir2/dirb/
https://ab.sample.com/dir2/dirb/art1.html
https://ab.sample.com/dir2/dirb/art2.html
https://ab.sample.com/dira
https://ab.sample.com/dira/
https://ab.sample.com/dira/art1.html
https://ab.sample.com/dira/art2.html
https://ab.sample.com/dirb
https://ab.sample.com/dirb/
https://ab.sample.com/dirb/art1.html
https://ab.sample.com/dirb/art2.html
https://ab.sample.com:80
https://ab.sample.com:80/
https://ab.sample.com:80/art1.html
https://ab.sample.com:80/art2.html
https://ab.sample.com:80/dir1
https://ab.sample.com:80/dir1/
https://ab.sample.com:80/dir1/art1.html
https://ab.sample.com:80/dir1/art2.html
https://ab.sample.com:80/dir1/dira
https://ab.sample.com:80/dir1/dira/
https://ab.sample.com:80/dir1/dira/art1.html
https://ab.sample.com:80/dir1/dira/art2.html
https://ab.sample.com:80/dir1/dirb
https://ab.sample.com:80/dir1/dirb/
https://ab.sample.com:80/dir1/dirb/art1.html
https://ab.sample.com:80/dir1/dirb/art2.html
https://ab.sample.com:80/dir2
https://ab.sample.com:80/dir2/
https://ab.sample.com:80/dir2/art1.html
https://ab.sample.com:80/dir2/art2.html
https://ab.sample.com:80/dir2/dira
https://ab.sample.com:80/dir2/dira/
https://ab.sample.com:80/dir2/dira/art1.html
https://ab.sample.com:80/dir2/dira/art2.html
https://ab.sample.com:80/dir2/dirb
https://ab.sample.com:80/dir2/dirb/
https://ab.sample.com:80/dir2/dirb/art1.html
https://ab.sample.com:80/dir2/dirb/art2.html
https://ab.sample.com:80/dira
https://ab.sample.com:80/dira/
https://ab.sample.com:80/dira/art1.html
https://ab.sample.com:80/dira/art2.html
https://ab.sample.com:80/dirb
https://ab.sample.com:80/dirb/
https://ab.sample.com:80/dirb/art1.html
https://ab.sample.com:80/dirb/art2.html
https://ab.sample.com:8080
https://ab.sample.com:8080/
https://ab.sample.com:8080/art1.html
https://ab.sample.com:8080/art2.html
https://ab.sample.com:8080/dir1
https://ab.sample.com:8080/dir1/
https://ab.sample.com:8080/dir1/art1.html
https://ab.sample.com:8080/dir1/art2.html
https://ab.sample.com:8080/dir1/dira
https://ab.sample.com:8080/dir1/dira/
https://ab.sample.com:8080/dir1/dira/art1.html
https://ab.sample.com:8080/dir1/dira/art2.html
https://ab.sample.com:8080/dir1/dirb
https://ab.sample.com:8080/dir1/dirb/
https://ab.sample.com:8080/dir1/dirb/art1.html
https://ab.sample.com:8080/dir1/dirb/art2.html
https://ab.sample.com:8080/dir2
https://ab.sample.com:8080/dir2/
https://ab.sample.com:8080/dir2/art1.html
https://ab.sample.com:8080/dir2/art2.html
https://ab.sample.com:8080/dir2/dira
https://ab.sample.com:8080/dir2/dira/
https://ab.sample.com:8080/dir2/dira/art1.html
https://ab.sample.com:8080/dir2/dira/art2.html
https://ab.sample.com:8080/dir2/dirb
https://ab.sample.com:8080/dir2/dirb/
https://ab.sample.com:8080/dir2/dirb/art1.html
https://ab.sample.com:8080/dir2/dirb/art2.html
https://ab.sample.com:8080/dira
https://ab.sample.com:8080/dira/
https://ab.sample.com:8080/dira/art1.html
https://ab.sample.com:8080/dira/art2.html
https://ab.sample.com:8080/dirb
https://ab.sample.com:8080/dirb/
https://ab.sample.com:8080/dirb/art1.html
https://ab.sample.com:8080/dirb/art2.html
https://cde.sample.com
https://cde.sample.com/
https://cde.sample.com/art1.html
https://cde.sample.com/art2.html
https://cde.sample.com/dir1
https://cde.sample.com/dir1/
https://cde.sample.com/dir1/art1.html
https://cde.sample.com/dir1/art2.html
https://cde.sample.com/dir1/dira
https://cde.sample.com/dir1/dira/
https://cde.sample.com/dir1/dira/art1.html
https://cde.sample.com/dir1/dira/art2.html
https://cde.sample.com/dir1/dirb
https://cde.sample.com/dir1/dirb/
https://cde.sample.com/dir1/dirb/art1.html
https://cde.sample.com/dir1/dirb/art2.html
https://cde.sample.com/dir2
https://cde.sample.com/dir2/
https://cde.sample.com/dir2/art1.html
https://cde.sample.com/dir2/art2.html
https://cde.sample.com/dir2/dira
https://cde.sample.com/dir2/dira/
https://cde.sample.com/dir2/dira/art1.html
https://cde.sample.com/dir2/dira/art2.html
https://cde.sample.com/dir2/dirb
https://cde.sample.com/dir2/dirb/
https://cde.sample.com/dir2/dirb/art1.html
https://cde.sample.com/dir2/dirb/art2.html
https://cde.sample.com/dira
https://cde.sample.com/dira/
https://cde.sample.com/dira/art1.html
https://cde.sample.com/dira/art2.html
https://cde.sample.com/dirb
https://cde.sample.com/dirb/
https://cde.sample.com/dirb/art1.html
https://cde.sample.com/dirb/art2.html
https://cde.sample.com:80
https://cde.sample.com:80/
https://cde.sample.com:80/art1.html
https://cde.sample.com:80/art2.html
https://cde.sample.com:80/dir1
https://cde.sample.com:80/dir1/
https://cde.sample.com:80/dir1/art1.html
https://cde.sample.com:80/dir1/art2.html
https://cde.sample.com:80/dir1/dira
https://cde.sample.com:80/dir1/dira/
https://cde.sample.com:80/dir1/dira/art1.html
https://cde.sample.com:80/dir1/dira/art2.html
https://cde.sample.com:80/dir1/dirb
https://cde.sample.com:80/dir1/dirb/
https://cde.sample.com:80/dir1/dirb/art1.html
https://cde.sample.com:80/dir1/dirb/art2.html
https://cde.sample.com:80/dir2
https://cde.sample.com:80/dir2/
https://cde.sample.com:80/dir2/art1.html
https://cde.sample.com:80/dir2/art2.html
https://cde.sample.com:80/dir2/dira
https://cde.sample.com:80/dir2/dira/
https://cde.sample.com:80/dir2/dira/art1.html
https://cde.sample.com:80/dir2/dira/art2.html
https://cde.sample.com:80/dir2/dirb
https://cde.sample.com:80/dir2/dirb/
https://cde.sample.com:80/dir2/dirb/art1.html
https://cde.sample.com:80/dir2/dirb/art2.html
https://cde.sample.com:80/dira
https://cde.sample.com:80/dira/
https://cde.sample.com:80/dira/art1.html
https://cde.sample.com:80/dira/art2.html
https://cde.sample.com:80/dirb
https://cde.sample.com:80/dirb/
https://cde.sample.com:80/dirb/art1.html
https://cde.sample.com:80/dirb/art2.html
https://cde.sample.com:8080
https://cde.sample.com:8080/
https://cde.sample.com:8080/art1.html
https://cde.sample.com:8080/art2.html
https://cde.sample.com:8080/dir1
https://cde.sample.com:8080/dir1/
https://cde.sample.com:8080/dir1/art1.html
https://cde.sample.com:8080/dir1/art2.html
https://cde.sample.com:8080/dir1/dira
https://cde.sample.com:8080/dir1/dira/
https://cde.sample.com:8080/dir1/dira/art1.html
https://cde.sample.com:8080/dir1/dira/art2.html
https://cde.sample.com:8080/dir1/dirb
https://cde.sample.com:8080/dir1/dirb/
https://cde.sample.com:8080/dir1/dirb/art1.html
https://cde.sample.com:8080/dir1/dirb/art2.html
https://cde.sample.com:8080/dir2
https://cde.sample.com:8080/dir2/
https://cde.sample.com:8080/dir2/art1.html
https://cde.sample.com:8080/dir2/art2.html
https://cde.sample.com:8080/dir2/dira
https://cde.sample.com:8080/dir2/dira/
https://cde.sample.com:8080/dir2/dira/art1.html
https://cde.sample.com:8080/dir2/dira/art2.html
https://cde.sample.com:8080/dir2/dirb
https://cde.sample.com:8080/dir2/dirb/
https://cde.sample.com:8080/dir2/dirb/art1.html
https://cde.sample.com:8080/dir2/dirb/art2.html
https://cde.sample.com:8080/dira
https://cde.sample.com:8080/dira/
https://cde.sample.com:8080/dira/art1.html
https://cde.sample.com:8080/dira/art2.html
https://cde.sample.com:8080/dirb
https://cde.sample.com:8080/dirb/
https://cde.sample.com:8080/dirb/art1.html
https://cde.sample.com:8080/dirb/art2.html
https://sample.com
https://sample.com/
https://sample.com/art1.html
https://sample.com/art2.html
https://sample.com/dir1
https://sample.com/dir1/
https://sample.com/dir1/art1.html
https://sample.com/dir1/art2.html
https://sample.com/dir1/dira
https://sample.com/dir1/dira/
https://sample.com/dir1/dira/art1.html
https://sample.com/dir1/dira/art2.html
https://sample.com/dir1/dirb
https://sample.com/dir1/dirb/
https://sample.com/dir1/dirb/art1.html
https://sample.com/dir1/dirb/art2.html
https://sample.com/dir2
https://sample.com/dir2/
https://sample.com/dir2/art1.html
https://sample.com/dir2/art2.html
https://sample.com/dir2/dira
https://sample.com/dir2/dira/
https://sample.com/dir2/dira/art1.html
https://sample.com/dir2/dira/art2.html
https://sample.com/dir2/dirb
https://sample.com/dir2/dirb/
https://sample.com/dir2/dirb/art1.html
https://sample.com/dir2/dirb/art2.html
https://sample.com/dira
https://sample.com/dira/
https://sample.com/dira/art1.html
https://sample.com/dira/art2.html
https://sample.com/dirb
https://sample.com/dirb/
https://sample.com/dirb/art1.html
https://sample.com/dirb/art2.html
https://sample.com:80
https://sample.com:80/
https://sample.com:80/art1.html
https://sample.com:80/art2.html
https://sample.com:80/dir1
https://sample.com:80/dir1/
https://sample.com:80/dir1/art1.html
https://sample.com:80/dir1/art2.html
https://sample.com:80/dir1/dira
https://sample.com:80/dir1/dira/
https://sample.com:80/dir1/dira/art1.html
https://sample.com:80/dir1/dira/art2.html
https://sample.com:80/dir1/dirb
https://sample.com:80/dir1/dirb/
https://sample.com:80/dir1/dirb/art1.html
https://sample.com:80/dir1/dirb/art2.html
https://sample.com:80/dir2
https://sample.com:80/dir2/
https://sample.com:80/dir2/art1.html
https://sample.com:80/dir2/art2.html
https://sample.com:80/dir2/dira
https://sample.com:80/dir2/dira/
https://sample.com:80/dir2/dira/art1.html
https://sample.com:80/dir2/dira/art2.html
https://sample.com:80/dir2/dirb
https://sample.com:80/dir2/dirb/
https://sample.com:80/dir2/dirb/art1.html
https://sample.com:80/dir2/dirb/art2.html
https://sample.com:80/dira
https://sample.com:80/dira/
https://sample.com:80/dira/art1.html
https://sample.com:80/dira/art2.html
https://sample.com:80/dirb
https://sample.com:80/dirb/
https://sample.com:80/dirb/art1.html
https://sample.com:80/dirb/art2.html
https://sample.com:8080
https://sample.com:8080/
https://sample.com:8080/art1.html
https://sample.com:8080/art2.html
https://sample.com:8080/dir1
https://sample.com:8080/dir1/
https://sample.com:8080/dir1/art1.html
https://sample.com:8080/dir1/art2.html
https://sample.com:8080/dir1/dira
https://sample.com:8080/dir1/dira/
https://sample.com:8080/dir1/dira/art1.html
https://sample.com:8080/dir1/dira/art2.html
https://sample.com:8080/dir1/dirb
https://sample.com:8080/dir1/dirb/
https://sample.com:8080/dir1/dirb/art1.html
https://sample.com:8080/dir1/dirb/art2.html
https://sample.com:8080/dir2
https://sample.com:8080/dir2/
https://sample.com:8080/dir2/art1.html
https://sample.com:8080/dir2/art2.html
https://sample.com:8080/dir2/dira
https://sample.com:8080/dir2/dira/
https://sample.com:8080/dir2/dira/art1.html
https://sample.com:8080/dir2/dira/art2.html
https://sample.com:8080/dir2/dirb
https://sample.com:8080/dir2/dirb/
https://sample.com:8080/dir2/dirb/art1.html
https://sample.com:8080/dir2/dirb/art2.html
https://sample.com:8080/dira
https://sample.com:8080/dira/
https://sample.com:8080/dira/art1.html
https://sample.com:8080/dira/art2.html
https://sample.com:8080/dirb
https://sample.com:8080/dirb/
https://sample.com:8080/dirb/art1.html
https://sample.com:8080/dirb/art2.html
https://www.sample.com
https://www.sample.com/
https://www.sample.com/art1.html
https://www.sample.com/art2.html
https://www.sample.com/dir1
https://www.sample.com/dir1/
https://www.sample.com/dir1/art1.html
https://www.sample.com/dir1/art2.html
https://www.sample.com/dir1/dira
https://www.sample.com/dir1/dira/
https://www.sample.com/dir1/dira/art1.html
https://www.sample.com/dir1/dira/art2.html
https://www.sample.com/dir1/dirb
https://www.sample.com/dir1/dirb/
https://www.sample.com/dir1/dirb/art1.html
https://www.sample.com/dir1/dirb/art2.html
https://www.sample.com/dir2
https://www.sample.com/dir2/
https://www.sample.com/dir2/art1.html
https://www.sample.com/dir2/art2.html
https://www.sample.com/dir2/dira
https://www.sample.com/dir2/dira/
https://www.sample.com/dir2/dira/art1.html
https://www.sample.com/dir2/dira/art2.html
https://www.sample.com/dir2/dirb
https://www.sample.com/dir2/dirb/
https://www.sample.com/dir2/dirb/art1.html
https://www.sample.com/dir2/dirb/art2.html
https://www.sample.com/dira
https://www.sample.com/dira/
https://www.sample.com/dira/art1.html
https://www.sample.com/dira/art2.html
https://www.sample.com/dirb
https://www.sample.com/dirb/
https://www.sample.com/dirb/art1.html
https://www.sample.com/dirb/art2.html
https://www.sample.com:80
https://www.sample.com:80/
https://www.sample.com:80/art1.html
https://www.sample.com:80/art2.html
https://www.sample.com:80/dir1
https://www.sample.com:80/dir1/
https://www.sample.com:80/dir1/art1.html
https://www.sample.com:80/dir1/art2.html
https://www.sample.com:80/dir1/dira
https://www.sample.com:80/dir1/dira/
https://www.sample.com:80/dir1/dira/art1.html
https://www.sample.com:80/dir1/dira/art2.html
https://www.sample.com:80/dir1/dirb
https://www.sample.com:80/dir1/dirb/
https://www.sample.com:80/dir1/dirb/art1.html
https://www.sample.com:80/dir1/dirb/art2.html
https://www.sample.com:80/dir2
https://www.sample.com:80/dir2/
https://www.sample.com:80/dir2/art1.html
https://www.sample.com:80/dir2/art2.html
https://www.sample.com:80/dir2/dira
https://www.sample.com:80/dir2/dira/
https://www.sample.com:80/dir2/dira/art1.html
https://www.sample.com:80/dir2/dira/art2.html
https://www.sample.com:80/dir2/dirb
https://www.sample.com:80/dir2/dirb/
https://www.sample.com:80/dir2/dirb/art1.html
https://www.sample.com:80/dir2/dirb/art2.html
https://www.sample.com:80/dira
https://www.sample.com:80/dira/
https://www.sample.com:80/dira/art1.html
https://www.sample.com:80/dira/art2.html
https://www.sample.com:80/dirb
https://www.sample.com:80/dirb/
https://www.sample.com:80/dirb/art1.html
https://www.sample.com:80/dirb/art2.html
https://www.sample.com:8080
https://www.sample.com:8080/
https://www.sample.com:8080/art1.html
https://www.sample.com:8080/art2.html
https://www.sample.com:8080/dir1
https://www.sample.com:8080/dir1/
https://www.sample.com:8080/dir1/art1.html
https://www.sample.com:8080/dir1/art2.html
https://www.sample.com:8080/dir1/dira
https://www.sample.com:8080/dir1/dira/
https://www.sample.com:8080/dir1/dira/art1.html
https://www.sample.com:8080/dir1/dira/art2.html
https://www.sample.com:8080/dir1/dirb
https://www.sample.com:8080/dir1/dirb/
https://www.sample.com:8080/dir1/dirb/art1.html
https://www.sample.com:8080/dir1/dirb/art2.html
https://www.sample.com:8080/dir2
https://www.sample.com:8080/dir2/
https://www.sample.com:8080/dir2/art1.html
https://www.sample.com:8080/dir2/art2.html
https://www.sample.com:8080/dir2/dira
https://www.sample.com:8080/dir2/dira/
https://www.sample.com:8080/dir2/dira/art1.html
https://www.sample.com:8080/dir2/dira/art2.html
https://www.sample.com:8080/dir2/dirb
https://www.sample.com:8080/dir2/dirb/
https://www.sample.com:8080/dir2/dirb/art1.html
https://www.sample.com:8080/dir2/dirb/art2.html
https://www.sample.com:8080/dira
https://www.sample.com:8080/dira/
https://www.sample.com:8080/dira/art1.html
https://www.sample.com:8080/dira/art2.html
https://www.sample.com:8080/dirb
https://www.sample.com:8080/dirb/
https://www.sample.com:8080/dirb/art1.html
https://www.sample.com:8080/dirb/art2.html
 */
