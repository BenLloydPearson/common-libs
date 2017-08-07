package com.gravity.utilities.web

/**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: 5/19/11
 * Time: 7:53 PM
 */

import java.util.Date

import com.gravity.utilities.time.GrvDateMidnight
import org.jsoup.nodes.Document
import org.scalatest.Matchers

import scala.collection._
import org.junit.Assert._
import com.gravity.goose.{Goose, Configuration, Article}
import com.gravity.utilities.grvstrings._
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import java.net.URL
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import org.apache.commons.lang3.StringEscapeUtils
import org.scalatest.junit.AssertionsForJUnit
import org.jsoup.Jsoup
import com.gravity.utilities.web.ContentUtils.Extractors

import scala.xml.Elem

object testSectionExtraction extends App with AssertionsForJUnit with Matchers {
  val url = "http://suite101.com/article/solutions_for_a_dogs_dry_skin_shedding-a66215"
  val html: Document = Jsoup.parse(new java.net.URL(url), 10000)
  html.head().append("""<meta property="section" content="Skin Care" />""")

  val config: Configuration = new Configuration
  config.setEnableImageFetching(false)
  config.setHtmlFetcher(GrvHtmlFetcher)
  Extractors.getExtractorOption(url).foreach(extractors => {
    config.setPublishDateExtractor(extractors.publishDateExt)
    config.setAdditionalDataExtractor(extractors.additionalDataExt)

    extractors.contentExtractorOption.foreach(ce => config.setContentExtractor(ce))
  })

  val article: Article = new Goose(config).extractContent(url, html.html())

  ContentUtilsIT.printResult(article)
}

object testWebMd extends App with AssertionsForJUnit with Matchers {
  ContentUtilsIT.crawlPrintAndAssert(
    Seq(
      "http://www.webmd.com/kidney-stones/news/20130515/sugary-sodas-fruit-punches-may-raise-kidney-stone-risk-study",
      "http://www.webmd.com/vitamins-supplements/ingredientmono-147-Bugleweed.aspx?activeIngredientId=147&activeIngredientName=Bugleweed",
      "http://www.webmd.com/baby/guide/pregnancy-am-i-pregnant"
    ),
    ignoreMissingPublishDates = true
  )
}

object testRantSports extends App with AssertionsForJUnit with Matchers {
  val urls: scala.Seq[String] = Seq(
    "http://www.rantsports.com/nfl/2013/05/21/10-nfl-rookies-who-will-disappoint-in-2013/"
  )

  ContentUtilsIT.crawlPrintAndAssert(urls, fetchImage = true, ignoreMissingPublishDates = true)
}

object testZekeBlog extends App with AssertionsForJUnit with Matchers {
  val urls: scala.Seq[String] = Seq(
    "http://victorblog.com/2013/03/05/html5-canvas-gradient-creator/",
    "http://victorblog.com/2013/01/17/solving-angularjs-empty-select-option-caused-by-jquery-ui-effect/",
    "http://victorblog.com/2012/12/31/old-fashioned-metronome-in-angularjs-html5-and-css3/",
    "http://victorblog.com/2012/12/20/make-angularjs-http-service-behave-like-jquery-ajax/"
  )

  ContentUtilsIT.crawlPrintAndAssert(urls, useUrlForPublishTime = true, fetchImage = true)
}

object testSomeScribdSites extends App with AssertionsForJUnit with Matchers {
  val urls: scala.Seq[String] = Seq(
    "http://www.scribd.com/doc/120233088/THE-TASK-SET",
    "http://www.scribd.com/doc/119454297/Vision-2025-Advancing-South-Carolina-s-Capacity-and-Expertise-in-Science-and-Technology",
    "http://www.scribd.com/doc/119983225/Not-Hollywood-by-Sherry-B-Ortner",
    "http://www.scribd.com/doc/115664645/The-Beautiful-Edible-Garden-Excerpt"
  )

  ContentUtilsIT.crawlPrintAndAssert(urls)
}

object testSomeWordPressSites extends App with AssertionsForJUnit with Matchers {
  val urls: scala.Seq[String] = Seq(
    "http://crayfisher.wordpress.com/2012/10/17/candy-crowley-worst-moderator-ever/",
    "http://johnnyvoid.wordpress.com/2012/10/17/unemployment-is-not-falling-dont-believe-the-hype/",
    "http://miami.cbslocal.com/2012/06/11/bath-salts-may-be-behind-naked-man-confronting-girl-in-park/",
    "http://surfwithamigas.com/2012/10/23/surfboard-reviews-the-rusty-dwart/"
  )

  ContentUtilsIT.crawlPrintAndAssert(urls, useUrlForPublishTime = true)
}

object testYahooOlympics extends App with AssertionsForJUnit with Matchers {
  val urls: scala.Seq[String] = Seq(
    "http://sports.yahoo.com/news/olympic-torch-becomes-cultural-happening-190838778--oly.html",
    "http://sports.yahoo.com/news/britons-cheer-torch-relay-halfway-mark-175403121--oly.html",
    "http://sports.yahoo.com/news/olympics--flojo-s-legacy-very-much-alive-24-years-after-her-triumphs-in-seoul.html",
    "http://sports.yahoo.com/blogs/olympics-fourth-place-medal/three-years-being-vegetative-state-teen-sets-two-163339202--oly.html"
  )

  ContentUtilsIT.crawlPrintAndAssert(urls)
}

object testWalyou extends App with AssertionsForJUnit with Matchers {
  val urls: scala.Seq[String] = Seq(
    "http://walyou.com/iron-man-sneakers/",
    "http://walyou.com/enterprise-real/",
    "http://walyou.com/xbox-360-controller-bullets/",
    "http://walyou.com/how-much-is-mark-zuckerberg-worth/",
    "http://walyou.com/sexy-geek-bathing-suits/"
  )

  ContentUtilsIT.crawlPrintAndAssert(urls)
}

object testSuite101 extends App with AssertionsForJUnit with Matchers {
  val urls: scala.Seq[String] = Seq(
    "http://suite101.com/article/solutions_for_a_dogs_dry_skin_shedding-a66215",
    "http://suite101.com/article/cat-hairball-causes-and-vomiting-in-cats-a60894",
    "http://suite101.com/article/increase-revenue-and-stimulate-business-growth-through-marketing-a406117",
    "http://suite101.com/article/coping-behaviours-used-to-respond-to-stress-in-the-21st-century-a397198"
  )

  ContentUtilsIT.crawlPrintAndAssert(urls)
}

object testDyingScene extends App with AssertionsForJUnit with Matchers {
  val url = "http://dyingscene.com/news/cover-art-of-the-year-winner-monstro-monstro/"
  val article: Article = ContentUtils.fetchAndExtractMetadata(url)
  ContentUtilsIT.printResult(article)
  assertNotNull("Publish Date should not be null!", article.publishDate)
}

object testMensFitness extends App with AssertionsForJUnit with Matchers {
  val urls: scala.Seq[String] = Seq(
    "http://www.mensfitness.com/training/build-muscle/mma-endurance-workout-iii",
    "http://www.mensfitness.com/training/build-muscle/10-ways-to-gain-muscle0",
    "http://www.mensfitness.com/training/build-muscle/mma-endurance-workout-i",
    "http://www.mensfitness.com/nutrition/supplements/natural-t-booster",
    "http://www.mensfitness.com/polls/whats-your-strongest-body-part",
    "http://www.mensfitness.com/training/build-muscle/what-do-you-think-is-the-average-arm-measurement-for-guys-who-lift",
    "http://www.mensfitness.com/women/galleries/fit-for-her-corinne-morrill",
    "http://www.mensfitness.com/women/dating-advice/dont-ask-dont-tell",
    "http://www.mensfitness.com/poll/56480/results",
    "http://www.mensfitness.com/poll/56415/results",
    "http://www.mensfitness.com/nutrition/what-to-eat/eat-your-way-to-a-six-pack",
    "http://www.mensfitness.com/nutrition/what-to-eat/the-best-fruit-for-men",
    "http://www.mensfitness.com/leisure/sports/greatest-mma-fighters-without-a-major-title",
    "http://www.mensfitness.com/bench",
    "http://www.mensfitness.com/stuff/reviews/shoe-guide-2011"
  )

  for (url <- urls) {
    val article = ContentUtils.fetchAndExtractMetadata(url)
    ContentUtilsIT.printResult(article)
    assertNotNull("Publish Date should not be null!", article.publishDate)
  }
}

object testSFGate extends App with AssertionsForJUnit with Matchers {
  val urls: scala.Seq[String] = Seq(
    "http://blog.sfgate.com/cityexposed/2012/05/06/monkey-business/",
    "http://blog.sfgate.com/49ers/2012/03/29/how-did-randy-moss-look-well-harbaugh-was-47-of-48/",
    "http://blog.sfgate.com/gettowork/2012/03/29/should-potential-employers-have-access-to-your-social-network/",
    "http://blog.sfgate.com/hottopics/2012/03/28/best-year-of-your-life-is-33-survey-says/",
    "http://blog.sfgate.com/hottopics/2012/03/28/man-saws-off-his-foot-to-avoid-work/",
    "http://blog.sfgate.com/hottopics/2012/03/28/ron-burgundy-announces-%e2%80%98anchorman%e2%80%99-sequel-on-conan/",
    "http://blog.sfgate.com/hottopics/2012/03/29/bohemian-rhapsody%e2%80%99-the-version-you-have-to-hear/",
    "http://blog.sfgate.com/hottopics/2012/03/29/now-you-can-be-buried-in-a-bacon-coffin/",
    "http://blog.sfgate.com/nov05election/2012/03/28/george-miller-confident-after-final-day-of-health-care-arguments/",
    "http://blog.sfgate.com/nov05election/2012/03/28/rick-santorum-arrives-in-ca-tomorrow-so-wheres-the-cash-coming-from/",
    "http://blog.sfgate.com/nov05election/2012/03/28/shocker-ca-state-assemblyman-nathan-fletcher-abandons-gop-to-get-things-done-as-independent/",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/28/DDJ61NQQ96.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/28/MN6C1NR2DK.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/28/MNLA1NPK9N.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/28/MNMV1NR2MA.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/29/BAIM1NRFH0.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/29/BAS31NRKL3.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/29/DD3A1NPGC7.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/g/a/2012/03/27/hearstmaghealth6643161.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/g/a/2012/03/28/bloomberg_articlesM1LQY76TTDS001-M1M3T.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/g/a/2012/03/29/trayvon_martin_cartoon.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/25/national/w075936D39.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/27/financial/f190139D39.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/27/international/i013238D82.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/27/national/a111537D49.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/27/national/a205753D40.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a030027D59.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a101904D10.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a102350D23.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a133419D89.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a135545D02.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a170553D86.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a191515D02.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/w153930D38.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/sports/s214224D10.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/state/n060722D18.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/29/national/a052937D11.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/29/national/a065243D04.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/29/national/a085840D34.DTL",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/19/BA0T1NN13J.DTL",
    "http://insidescoopsf.sfgate.com/blog/2012/03/19/james-beard-awards-2012-nominees-announced-bay-area-fares-well-again/",
    "http://blog.sfgate.com/bronstein/2011/08/16/bart-spokesperson-no-right-to-free-speech-inside-bart/",
    "http://blog.sfgate.com/framerate/2012/03/21/super-hooper/",
    "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/21/MNM11NNR7G.DTL",
    "http://insidescoopsf.sfgate.com/blog/2012/03/21/report-cellar-door-is-being-renamed-le-cigare-volant/",
    "http://blog.sfgate.com/bronstein/2011/08/16/bart-spokesperson-no-right-to-free-speech-inside-bart/"
  )

  for (url <- urls) {
    val article = ContentUtils.fetchAndExtractMetadata(url)
    ContentUtilsIT.printResult(article)
    assertNotNull("Publish Date should not be null!", article.publishDate)
  }
}

object testVoices extends App with AssertionsForJUnit with Matchers {
  def assertions(url: String) {
    assertTrue("The following URL should have a PublishDateExtractor!\n\t" + url, ContentUtils.hasPublishDateExtractor(url))
    println("Fetching article for URL: " + url)
    val article = ContentUtils.fetchAndExtractMetadata(url)
    ContentUtilsIT.printResult(article)
    assertNotSame("Title must not be: voices.yahoo.com!", "voices.yahoo.com", article.title)
    assertNotNull("Publish Date should not be null!", article.publishDate)
  }

  val feedUrl = "http://voices.yahoo.com/rss/recent_15.xml"

  println("Retrieving URLs to verify from feedUrl: " + feedUrl)

  val rss: Elem = scala.xml.XML.load(new URL(feedUrl))

  for {
    item <- rss \\ "item"
    url = (item \ "link").text.drop(1)
  } {
    assertions(url)
  }

  assertions("http://voices.yahoo.com/article/9330101/lovess-demise-10882501.html")
}

object testKnownEscapes extends App with AssertionsForJUnit with Matchers {
  val funkyChars = "‘attacks’ & other <things>Don’t love something that can’t love you back</things>"
  val cleaned: String = StringEscapeUtils.escapeHtml4(funkyChars)

  println("input: " + funkyChars)
  println("output: " + cleaned)
}

object testBoston extends App with AssertionsForJUnit with Matchers {
  val urls: List[String] = List(
    "http://www.boston.com/Boston/whitecoatnotes/2011/12/clipboard-the-cigarette-debate/MZrRNyXBGVQHY8L5gUwUeI/index.html",
    "http://www.boston.com/news/nation/articles/2011/12/06/bodybuilder_in_custody_after_calif_couple_beaten/",
    "http://www.boston.com/lifestyle/health/childinmind/2011/12/why_defiant_behavior_pushes_pa.html",
    "http://boston.com/community/blogs/less_is_more/2011/12/landrys_social_security_propos.html",
    "http://www.boston.com/community/moms/blogs/24_hour_workday/2011/11/babysitting-for-the-holidays.html",
    "http://www.boston.com/sports/football/patriots/articles/2012/01/08/gronk_the_great_helps_power_the_patriots/",
    "http://www.boston.com/Boston/culturedesk/2012/02/colbert-attacks-mckibben-demands-apology/FzrglQhaqHi7XEoEbI8TbL/index.html", // properly escaped right/left quotes
    "http://www.boston.com/realestate/news/blogs/renow/2012/02/dont_love_somet.html", // NOT escaped right quotes used as apostrophes
    "http://www.boston.com/news/health/blog/healthylifestyles/2010/01/newlywed_weight_gain.html",
    "http://www.boston.com/sports/baseball/redsox/articles/2012/02/27/wait_and_see_approach_for_valentine_in_clubhouse/"
  )

  for (url <- urls) {
    assertTrue("The following URL should have a PublishDateExtractor!\n\t" + url, ContentUtils.hasPublishDateExtractor(url))
    println("Fetching article for URL: " + url)
    val article = ContentUtils.fetchAndExtractMetadata(url)
    ContentUtilsIT.printResult(article)
    assertNotNull("Publish Date should not be null!", article.publishDate)
    assertFalse("Title should not contain Boston.com!", article.title.contains("Boston.com"))
    assertFalse("Title should not contain The Boston Globe!", article.title.contains("The Boston Globe"))
    assertFalse("Title should not contain Boston Red Sox!", article.title.contains("Boston Red Sox"))
  }
}

object testBostonAllRss extends App with AssertionsForJUnit with Matchers {
  val urls: mutable.Set[String] = mutable.Set[String]()
  val feeds: List[String] = scala.io.Source.fromInputStream(getClass.getResourceAsStream("boston_feed_list_unwound.txt")).getLines().toList

  println("Will attempt to pull unique article URLs from %d RSS feeds...".format(feeds.size))

  var goodFeeds = 0

  def getRss(feed: String): Option[Elem] = {
    try {
      println("Attempting to load RSS for: " + feed)
      val rss = scala.xml.XML.load(new URL(feed))
      goodFeeds += 1
      Some(rss)
    } catch {
      case _: Exception => {
        println("...Failed")
        None
      }
    }
  }

  for {
    feed <- feeds
    rss <- getRss(feed)
    item <- rss \\ "item"
    url = (item \ "origLink").text
    if (ArticleWhitelist.isValidPartnerArticle(url))
  } {
    urls += url
  }

  println("Successfully collected %d unique whitelisted article URLs from %d feeds!".format(urls.size, goodFeeds))
  println("Will now attempt to crawl and extract data for all %d urls...".format(urls.size))

  val failures: mutable.Buffer[(String, String)] = mutable.Buffer[(String, String)]()
  var testedUrls = 0

  def verifyURL(url: String): Boolean = {
    testedUrls += 1

    if (ContentUtils.hasPublishDateExtractor(url)) {
      println("Fetching article for URL: " + url)
      val article = ContentUtils.fetchAndExtractMetadata(url)
      ContentUtilsIT.printResult(article)

      if (article.publishDate == null) {
        failures += "Unable to parse Publish Date!" -> url
        return false
      }
    } else {
      failures += "No PublishDateExtractor!" -> url
      return false
    }

    true
  }

  var failedUrls = 0

  for (url <- urls) {
    if (!verifyURL(url)) failedUrls += 1
  }


  if (!failures.isEmpty) {
    println()
    println()
    printf("The following %d urls failed (out of %d total):%n", failedUrls, testedUrls)

    val sfs = failures.sortBy(_._2)

    var i = 0

    for ((f, u) <- sfs) {
      i += 1
      printf("#%d: %s%n\t%s%n%n", i, f, u)
    }

    fail(failures.size + " Failures Occurred!")
  }
}

object testBostonLive extends App with AssertionsForJUnit with Matchers {
  val siteMap: Elem = scala.xml.XML.load(new URL("http://www.boston.com/news/google-news-sitemap.xml"))

  val failures: mutable.Buffer[(String, String)] = mutable.Buffer[(String, String)]()
  var testedUrls = 0

  def verifyURL(url: String): Boolean = {
    testedUrls += 1

    if (ContentUtils.hasPublishDateExtractor(url)) {
      println("Fetching article for URL: " + url)
      val article = ContentUtils.fetchAndExtractMetadata(url)
      ContentUtilsIT.printResult(article)

      if (article.publishDate == null) {
        failures += "Unable to parse Publish Date!" -> url
        return false
      }
    } else {
      failures += "No PublishDateExtractor!" -> url
      return false
    }

    true
  }

  var failedUrls = 0

  for {
    node <- siteMap
    urlNode <- node \\ "url" \\ "loc"
    rawUrl = urlNode.text
    if (!rawUrl.contains("/gallery/"))
  } {
    val url = if (!rawUrl.startsWith("http://")) {
      println("invalid url, so i'm fixing it!")
      val good = "http://" + rawUrl.stripPrefix("http:/")
      printf("\tWAS: %s%n\tNOW: %s%n", rawUrl, good)
      good
    } else rawUrl

    if (!verifyURL(url)) failedUrls += 1
  }

  val pleasePassUrl = "http://articles.boston.com/2012-02-02/lifestyle/31018031_1_boston-beer-ban-sales-shaun-clancy"

  if (!verifyURL(pleasePassUrl)) failedUrls += 1

  if (!failures.isEmpty) {
    println()
    println()
    printf("The following %d urls failed (out of %d total):%n", failedUrls, testedUrls)

    val sfs = failures.sortBy(_._2)

    var i = 0

    for ((f, u) <- sfs) {
      i += 1
      printf("#%d: %s%n\t%s%n%n", i, f, u)
    }

    fail(failures.size + " Failures Occurred!")
  }
}

object testWesh extends App with AssertionsForJUnit with Matchers {
  val url = "http://www.wesh.com/news/29736011/detail.html"

  assertTrue("The following URL should have a PublishDateExtractor!\n\t" + url, ContentUtils.hasPublishDateExtractor(url))
  println("Fetching article for URL: " + url)
  val article: Article = ContentUtils.fetchAndExtractMetadata(url)
  ContentUtilsIT.printResult(article)
}

object testYahooNews extends App with AssertionsForJUnit with Matchers {
  val urls: scala.Seq[String] = Seq(
    "http://news.yahoo.com/santorum-fires-gun-woman-shouts-pretend-obama-160942541--abc-news.html;_ylt=AvlGwZdAiIR3RyDeaEMqwQ2s0NUE;_ylu=X3oDMTNsYWZnamhuBG1pdANUb3BTdG9yeSBGUARwa2cDMDgwM2ViMzctY2Y1Ni0zNjM2LThkMDItMmE5YTQwYWIxZWM2BHBvcwMxBHNlYwN0b3Bfc3RvcnkEdmVyAzE3NTEzOTIwLTc1MDktMTFlMS1iZGZmLWJlM2NkODM1OWFkNA--;_ylg=X3oDMTFrM25vcXFyBGludGwDdXMEbGFuZwNlbi11cwRwc3RhaWQDBHBzdGNhdAMEcHQDc2VjdGlvbnMEdGVzdAM-;_ylv=3",
    "http://news.yahoo.com/blogs/ticket/santorum-suggests-electing-obama-better-romney-presidency-211531760.html"
  )

  for (url <- urls) {
    assertTrue("The following URL should have a PublishDateExtractor!\n\t" + url, ContentUtils.hasPublishDateExtractor(url))
    println("Fetching article for URL: " + url)
    val article = ContentUtils.fetchAndExtractMetadata(url)
    ContentUtilsIT.printResult(article)
    assertNotNull("Publish Date should NOT be NULL!", article.publishDate)
  }
}

object testYahooNewsAttribution extends App with AssertionsForJUnit with Matchers {
  val urlsAndShouldHaveAttribution: scala.Seq[(String, Boolean)] = Seq(
    "http://news.yahoo.com/microsoft-profit-falls-slightly-200949676--sector.html" -> true,
    "http://news.yahoo.com/dick-clark-tv-years-eve-icon-dies-82-030250094.html;_ylt=AqkJNl7X1KltyFyTPSbvpoSs0NUE;_ylu=X3oDMTNhbTI1MDRlBG1pdAMEcGtnAzkzMmU4NWUyLTVhZWYtM2JjNC05MjJjLWMxZjZkMTBiODNiOQRwb3MDMwRzZWMDbG5fQVBfZ2FsBHZlcgNjZTQ0NWRmMi04OWQyLTExZTEtYmZiNy03MjNjYzg4MTQyMTY-;_ylv=3" -> true,
    "http://news.yahoo.com/blogs/ticket/ted-nugent-meet-secret-over-obama-comments-180823765.html" -> false,
    "http://news.yahoo.com/feds-charge-7-internet-ad-fraud-case-165522124.html;_ylt=AsscMlQqNo53.SNedeGoPNCs0NUE;_ylu=X3oDMTNpYWNjZ2kyBG1pdAMEcGtnAzc1NGRiMDRjLWQ4NGYtM2VhYi1iY2U5LTljZDQ3YTk2MzI5NwRwb3MDMQRzZWMDbG5fTGF0ZXN0TmV3c19nYWwEdmVyA2I0OTUzYjUwLTBiMDAtMTFlMS04YmY3LThiMTNmOWJjNDY1MA--;_ylv=3" -> true,
    "http://news.yahoo.com/blogs/technology-blog/just-show-create-friends-list-facebook-174736732.html" -> false,
    //      "http://news.yahoo.com/thousands-defy-norway-mass-killer-breivik-song-102440602.html" -> true, <--- this article has no publish date in the HTML period.
    "http://news.yahoo.com/zynga-reports-1q-net-loss-higher-revenue-202218357--finance.html" -> true,
    "http://news.yahoo.com/murdoch-hacking-scandal-changed-entire-company-165150725--finance.html" -> true,
    "http://news.yahoo.com/hubbub-over-content-rights-greets-google-drive-040418608--finance.html" -> true
  )

  for ((url, shouldHaveAttribution) <- urlsAndShouldHaveAttribution) {
    println("Fetching article for URL: " + url)
    val article = ContentUtils.fetchAndExtractMetadata(url)
    ContentUtilsIT.printResult(article)
    assertNotNull("Publish Date should NOT be NULL!", article.publishDate)
    assertFalse("Title should NOT be EMPTY!", article.title.isEmpty)
    val haveHaveNot = if (shouldHaveAttribution) "HAVE" else "NOT have"
    assertEquals("Article should %s attribution.logo".format(haveHaveNot), shouldHaveAttribution, article.additionalData.keySet.contains("attribution.logo"))
  }
}

object testTechCrunch extends App with AssertionsForJUnit with Matchers {
  val url = "http://techcrunch.com/2011/11/03/meet-verizons-new-htc-rezound/"
  val expectedTags: Set[String] = Set("htc", "Verizon", "vigor", "LTE", "Rezound")

  val tcArticle: Article = ContentUtils.fetchAndExtractMetadata(url)
  ContentUtilsIT.printResult(tcArticle)
  assertEquals("Tags should equal: " + expectedTags.mkString(", ") + "!", expectedTags, tcArticle.tags)
  val expectedPublishDate: Date = new GrvDateMidnight(2011, 11, 3).toDate
  assertEquals("Publish date should be 2011-11-03!", expectedPublishDate, tcArticle.publishDate)
}

object testTimeTooManyTagsReduction extends App with AssertionsForJUnit with Matchers {
  val wpUrl = "http://newsfeed.time.com/2011/10/11/japan-to-give-away-10000-free-flights-to-tourists-in-2012/"
  val nonWPurl = "http://www.time.com/time/world/article/0,8599,2097306,00.html"

  val expectedWPtags: Set[String] = Set("earthquake", "free airfare", "free flights", "Fukushima-Daiichi", "japan", "japan tourism agency", "Tourism", "travel")

  val wpArticle: Article = ContentUtils.fetchAndExtractMetadata(wpUrl)
  ContentUtilsIT.printResult(wpArticle)
  assertEquals(expectedWPtags, wpArticle.tags)

  val nonWParticle: Article = ContentUtils.fetchAndExtractMetadata(nonWPurl)
  ContentUtilsIT.printResult(nonWParticle)
}

object testCnnMoneyArticles extends App with AssertionsForJUnit with Matchers {
  val urls: List[String] = List("http://money.cnn.com/2011/09/09/news/economy/obama_jobs_pay_for/index.htm",
    "http://tech.fortune.cnn.com/2011/09/09/gamestop-ceo-talks-ios-devices/")

  for (url <- urls) {
    assertTrue("The following URL should have a PublishDateExtractor!\n\t" + url, ContentUtils.hasPublishDateExtractor(url))
    println("Fetching article for URL: " + url)
    val article = ContentUtils.fetchAndExtractMetadata(url)
    ContentUtilsIT.printResult(article)
  }
}

object testTimeArticles extends App with AssertionsForJUnit with Matchers {
  val urls: List[String] =
    "http://www.time.com/time/nation/article/0,8599,2090658,00.html" ::
      "http://swampland.time.com/2011/08/04/congress-reaches-deal-to-end-faa-shutdown/" ::
      "http://www.time.com/time/magazine/article/0,9171,758031,00.html" ::
      "http://www.time.com/time/magazine/article/0,9171,788783,00.html" ::
      Nil

  for (url <- urls) withClue (url) {
    val article = ContentUtils.fetchAndExtractMetadata(url)
    article.title should not be (null)
    article.publishDate should not be (null)
  }
}

object testExtractingPublishDatesForTime extends App with AssertionsForJUnit with Matchers {
  val url = "http://swampland.time.com/2011/08/04/congress-reaches-deal-to-end-faa-shutdown/"
  val parts: Array[String] = tokenize(url, "/")
  println("URL: " + url)
  println("parts.size: " + parts.size)
  parts.zipWithIndex.foreach {case (part: String, i: Int) => printf("part(%d): %s%n", i, part)}

  val day: Int = parts(parts.size - 2).tryToInt match {
    case Some(dd) => dd
    case None => {
      println("failed to parse day portion to an Int. Day porrtion string = " + parts(parts.size - 2))
      0
    }
  }

  val month: Int = parts(parts.size - 3).tryToInt match {
    case Some(mm) => mm
    case None => {
      println("failed to parse month portion to an Int. Day porrtion string = " + parts(parts.size - 3))
      0
    }
  }

  val year: Int = parts(parts.size - 4).tryToInt match {
    case Some(yyyy) => yyyy
    case None => {
      println("failed to parse year portion to an Int. Day porrtion string = " + parts(parts.size - 4))
      0
    }
  }

  val publishDate: DateTime = new DateTime(year, month, day, 0, 0, 0, 0)

  println("Publish date extracted: " + publishDate.toString(DateTimeFormat.longDateTime()))
}

object testGettingScribdArticle extends App with AssertionsForJUnit with Matchers {
  val result: Article = ContentUtils.fetchAndExtractMetadata("http://www.scribd.com/doc/35766151/Trial-by-Jury")
  ContentUtilsIT.printResult(result)
}

object testGettingWsjArticle extends App with AssertionsForJUnit with Matchers {
  val result: Article = ContentUtils.fetchAndExtractMetadata("http://online.wsj.com/article/SB10001424052748704904604576333511794234064.html?mod=WSJ_hpp_sections_news")
  ContentUtilsIT.printResult(result)
}

object testGettingBuzznetArticle extends App with AssertionsForJUnit with Matchers {
  val urls: scala.Seq[String] = Seq(
    "http://wevegotyoucovered.buzznet.com/user/journal/17251757/buzznet-exclusive-kristen-stewart-talks/",
    "http://pattygopez.buzznet.com/user/journal/16972909/demi-lovato-talks-being-sober/",
    "http://pattygopez.buzznet.com/user/journal/8126591/bristol-palin-admits-having-plastic/",
    "http://craigertiger.buzznet.com/user/audio/free-download-our-new-remix-202781/",
    "http://henrycoachella.buzznet.com/user/polls/191601/poll-which-indie-hipster-hype/",
    "http://amehkristine-stunningnewpicsofchr.buzznet.com/user/photos/"
  )

  ContentUtilsIT.crawlPrintAndAssert(urls)
}

object verifyAuthorExtraction extends App with AssertionsForJUnit with Matchers {
  val result: Article = ContentUtils.fetchAndExtractMetadata("http://billkaulitzoneandonly.buzznet.com/user/journal/8271301/why-should-care-chapter-1/")
  ContentUtilsIT.printResult(result)
  result.additionalData.get("author") match {
    case Some(authorName) => assertEquals("Author name should be as expected!", "BillyMartinsRiotGirl", authorName)
    case None => fail("Author name should be found!")
  }

  result.additionalData.get("gender") match {
    case Some(authorGender) => assertEquals("Author gender should be as expected!", "F", authorGender)
    case None => fail("Author gender should be found!")
  }
}

object testFunkyDateIssue extends App with AssertionsForJUnit with Matchers {
  val result: Article = ContentUtils.fetchAndExtractMetadata("http://www.scribd.com/doc/29019824/4-Explosives-Foreplay-Techniques-to-Spice-Up-Your-Boring-Sex-Life")

  ContentUtilsIT.printResult(result)
}

object testCatsterBlogDate extends App with AssertionsForJUnit with Matchers {
  val result: Article = ContentUtils.fetchAndExtractMetadata("http://blogs.catster.com/cat_tip_of_the_day/2011/06/03/cat-owners-use-treats-wisely/")

  ContentUtilsIT.printResult(result)
}

object testDogsterBlogDate extends App with AssertionsForJUnit with Matchers {
  val result: Article = ContentUtils.fetchAndExtractMetadata("http://blogs.dogster.com/living-with-dogs/guardians-of-rescue-has-a-new-dog-mascot/2011/08/")

  assertNotNull("Dogster Result should not be NULL! You probably need to use a more recent article URL.", result)

  ContentUtilsIT.printResult(result)
}

object testXojane extends App with AssertionsForJUnit with Matchers {
  //    val result = ContentUtils.fetchAndExtractMetadata("http://www.xojane.com/beauty/dye-your-hair-red-at-home")
  val result: Article = ContentUtils.fetchAndExtractMetadata("http://www.xojane.com/fashion/ask-laia-are-leather-pants-faux-pas")

  assertNotNull("xoJane Result should not be NULL! You probably need to use a more recent article URL.", result)

  ContentUtilsIT.printResult(result)
}

object ContentUtilsIT {
  def printResult(result: Article) {
    println("Results for url: " + result.canonicalLink)
    println("\ttitle: " + result.title)
    println("\tpublishedDate: " + result.publishDate)
    println("\timage: " + result.topImage.imageSrc)
    result.additionalData.get("author") match {
      case Some(author) => println("\tauthor: " + author)
      case None => println("\tno author found")
    }
    result.additionalData.get("authorLink") match {
      case Some(authorLink) => println("\tauthorLink: " + authorLink)
      case None => println("\tno authorLink found")
    }
    result.additionalData.get("gender") match {
      case Some(gender) => println("\tgender: " + gender)
      case None => println("\tno gender found")
    }
    result.additionalData.get("attribution.name") match {
      case Some(name) => println("\tattribution.name: " + name)
      case None => println("\tno attribution.name found")
    }
    result.additionalData.get("attribution.logo") match {
      case Some(logo) => println("\tattribution.logo: " + logo)
      case None => println("\tno attribution.logo found")
    }
    result.additionalData.get("attribution.site") match {
      case Some(site) => println("\tattribution.site: " + site)
      case None => println("\tno attribution.site found")
    }
    result.additionalData.get("section") match {
      case Some(section) => println("\tsection: " + section)
      case None => println("\tno section found")
    }
    if (result.tags != null && result.tags.size > 0) {
      println()
      println("\tCaptured TAGs:")
      result.tags.toList.zipWithIndex.foreach({
        case (tag, i) => printf("\t\t#%d: %s%n", i + 1, tag)
      })
    }
    println("\tSummary: " + ContentUtils.extractSummary(result))
    println("-------------------------------------------------------------")
    println()
  }

  def crawlPrintAndAssert(urls: Seq[String], useUrlForPublishTime: Boolean = false, fetchImage: Boolean = false, ignoreMissingPublishDates: Boolean = false) {
    for (url <- urls) {
      val pname = ArticleWhitelist.getPartnerName(url).getOrElse("NO PARTNER NAME FOUND")
      val article = if (useUrlForPublishTime) {
        ContentUtils.fetchAndExtractMetadataWithUrlBasedPublishTime(url, fetchImage)
      } else {
        ContentUtils.fetchAndExtractMetadata(url, fetchImage)
      }
      println("Partner Name: " + pname)
      printResult(article)
      if (!ignoreMissingPublishDates) assertNotNull("Publish Date should not be null!", article.publishDate)
    }
  }
}