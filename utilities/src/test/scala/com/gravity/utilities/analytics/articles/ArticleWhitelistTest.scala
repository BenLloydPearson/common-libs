package com.gravity.utilities.analytics.articles

import org.junit.Assert._
import com.gravity.utilities.analytics.articles.ArticleWhitelist.Partners
import java.net.URL
import org.junit.{Assert, Ignore, Test}
import com.gravity.utilities.grvstrings.GrvString
import com.gravity.utilities.grvstrings

/**
 * Created by Jim Plush
 * User: jim
 * Date: 5/9/11
 */

case class UrlCandidate(url: String, isExpectedValid: Boolean = true, validatePartnerName: Boolean = true)

class ArticleWhitelistTest {
  def assertUrlCandidates(candidates: Seq[UrlCandidate], partner: String, siteGuidOverride: Option[String] = None) {
    for (uc <- candidates) {
      assertUrl(uc.url, partner, uc.isExpectedValid, uc.validatePartnerName, siteGuidOverride)
    }
  }

  def assertUrl(url: String, partner: String, isExpectedValid: Boolean = true, validatePartnerName: Boolean = true, siteGuidOverride: Option[String] = None) {
    val negation = if (isExpectedValid) "" else "NOT "

    if (validatePartnerName) {
      ArticleWhitelist.getPartnerName(url) match {
        case Some(name) => assertEquals("URL Should be for partner: " + partner, partner, name)
        case None => fail(s"Partner name should be '$partner' for url '$url'!")
      }
    }

    assertSame(s"\n\t$url\n\t\t`--->should ${negation}be a valid article URL!\n\n", isExpectedValid, ArticleWhitelist.urlCanBeCrawled(url, siteGuidOverride = siteGuidOverride))
    println(s"\n$url\n\t`---> was ${negation}valid as expected")
  }

  def assertUrls(partner: String, urlsWithExpected: Seq[(String, Boolean)], siteGuidOverride: Option[String] = None) {
    for ((url, expectedValid) <- urlsWithExpected) {
      assertUrl(url, partner, expectedValid, siteGuidOverride = siteGuidOverride)
    }
  }

  @Test def testMsn(): Unit = {
    val expGood: Seq[String] = Seq(
      "http://www.msn.com/en-us/autos/enthusiasts/treasure-hunting-at-hershey-the-worlds-largest-old-car-swap-meet/ar-BBmkSQu",
      "http://www.msn.com/en-us/entertainment/celebrity/celeb-kids-dressed-up-for-halloween/ss-AAfn1yd",
      "http://www.msn.com/en-us/foodanddrink/wine/heres-how-to-chill-any-wine-in-7-minutes-flat/ar-AAfhH0m",
      "http://www.msn.com/en-us/health/medical/5-unexpected-reasons-your-head-is-killing-you/ar-BBmxpKC",
      "http://www.msn.com/en-us/lifestyle/marriage/15-celebrities-who-married-outside-of-hollywood/ss-AAf8O9N",
      "http://www.msn.com/en-us/money/markets/the-case-for-year-round-daylight-saving-time-just-got-billions-of-dollars-stronger/ar-BBmA9mq",
      "http://www.msn.com/en-us/movies/news/the-biggest-cinematic-letdowns-of-2015-so-far/ar-BBmryKh",
      "http://www.msn.com/en-us/music/news/8-things-you-didnt-know-about-taylor-swifts-1989/ar-AAfwu0R",
      "http://www.msn.com/en-us/news/us/emt-out-of-work-for-helping-choking-girl/vi-BBmznaa",
      "http://www.msn.com/en-us/sports/nfl/report-manziel-won%E2%80%99t-be-disciplined-over-off-field-incident/ar-BBmzPhA",
      "http://www.msn.com/en-us/tv/news/stephen-colbert-challenges-donald-trump-to-donate-dollar1-million-to-charity/ar-BBmzg5z",
      "http://www.msn.com/en-us/video/_log/orca-tosses-seal-almost-100-feet-into-the-air/vi-BBmuVJH",
      "http://www.msn.com/en-us/video/watch/great-white-shark-chomps-on-boat/vp-BBmUeHj"
    )

    val expBad: Seq[String] = Seq(
      // Whole-section excludes
      "http://www.msn.com/en-us/travel/tripideas/the-13-most-beautiful-haunted-destinations-around-the-world/ss-BBmi3H1",
      "http://www.msn.com/en-us/weather/topstories/did-5-feet-of-snow-just-fall-on-florida/ar-BBmzfFm",

      // Search results
      "http://www.msn.com/en-us/weather/weathersearch?q=90045&form=PRWKWB&mkt=en-us&refig=b08ea4af0c114961be62f44accc6dc2c",
      "http://www.msn.com/en-us/autos/searchresults?q=tesla&form=PRAUSB&mkt=en-us&refig=fe82d5035dc04fb5a3284994b56bc06c&pq=tesla&sc=8-5&sp=-1&qs=n&sk=",
      "http://www.msn.com/en-us/entertainment/search?q=George+Takei&form=PRENSB&mkt=en-us&refig=d934fd04e57143f5a3de590376422464&pq=george+takei&sc=8-12&sp=-1&qs=n&sk=",
      "http://www.msn.com/en-us/foodanddrink/searchresults?q=zinfandel&form=PRFDSB&mkt=en-us&refig=1050f0f0064c4cbca15443c88ff7c591&pq=zinfandel&sc=8-9&sp=-1&qs=n&sk=",
      "http://www.msn.com/en-us/money/searchresults?q=apple&form=PRFISB&mkt=en-us",
      "http://www.msn.com/en-us/money/stockdetails/fi-126.1.AAPL.NAS?symbol=AAPL&form=PRFISB",
      "http://www.msn.com/en-us/money/watchlist?pfr=1",
      "http://www.msn.com/en-us/health/search?q=exercise&form=PRHFSB&mkt=en-us&refig=c6e078f9456140d0b2c240d14944eeab",
      "http://www.msn.com/en-us/movies/search?q=hudsucker+proxy&form=PRENSB&mkt=en-us&refig=50eaf9e921444ce1b69532042a67d1b7&pq=hudsucker&sc=8-9&sp=1&qs=LC&sk=",
      "http://www.msn.com/en-us/music/search?q=%22five+for+fighting%22&form=PRENSB&mkt=en-us&refig=61926f6be48c4242993194602b867e17&pq=%22five+for+fighting%22&sc=8-19&sp=-1&qs=n&sk=",
      "http://www.msn.com/en-us/news/trending/topicsearch?q=tuberculosis+deaths&form=PRNTTH&mkt=en-us&refig=0c17e3eef4304afcb9b3c5a09f83eeee&pq=&sc=8-0&sp=3&qs=PN&sk=PN2",
      "http://www.msn.com/en-us/travel/searchresults?q=singapore&form=PRTRSB&mkt=en-us&refig=6d8f4db289f24646b4b85341006fdeb3",
      "http://www.msn.com/en-us/tv/search?q=les+revenants&form=PRENSB&mkt=en-us&refig=75e569419084444aaecd6edf4cbb8c8b&pq=les+rev&sc=8-7&sp=1&qs=LS&sk=",
      "http://www.msn.com/en-us/video/searchresults?q=funny+internet+cats&form=PRVISB&mkt=en-us&refig=151ce35b16714243b841fcfb54ef85f5&pq=funny+internet+cat&sc=4-18&sp=-1&qs=n&sk=",
      "http://www.msn.com/en-us/sports/searchresults?q=bork&form=PRSPSB&mkt=en-us&refig=d1d12bbff94e4be7ae7cb6dc9620ba2e",

      // Section Landing Pages
      "http://www.msn.com/",
      "http://www.msn.com/en-us",
      "http://www.msn.com/en-us/autos",
      "http://www.msn.com/en-us/entertainment",
      "http://www.msn.com/en-us/foodanddrink",
      "http://www.msn.com/en-us/health",
      "http://www.msn.com/en-us/lifestyle",
      "http://www.msn.com/en-us/money",
      "http://www.msn.com/en-us/movies",
      "http://www.msn.com/en-us/music",
      "http://www.msn.com/en-us/news",
      "http://www.msn.com/en-us/sports",
      "http://www.msn.com/en-us/travel",
      "http://www.msn.com/en-us/tv",
      "http://www.msn.com/en-us/video/_log",
      "http://www.msn.com/en-us/weather"
    )

    val urlsAndExpectations = expGood.map(_ -> true) ++ expBad.map(_ -> false)

    assertUrls(Partners.MSN, urlsAndExpectations)
  }

  @Test def testTheGuardian(): Unit = {
    val urlsAndExpectations = Seq(
      "http://www.theguardian.com/football/2014/feb/05/david-beckham-superstar-mates-miami-new-mls-team?utm_source=foo" -> true,
      "http://www.theguardian.com/football/who-scored-blog/2014/feb/05/premier-league-liga-bundesliga-chelsea-manchester-city" -> true,
      "http://www.theguardian.com/news/reality-check/2014/jan/31/sex-guardian-readers-confess-all" -> true,
      "http://www.theguardian.com/uk/sport" -> false,
      "http://www.theguardian.com/us" -> false,
      "http://www.theguardian.com/world/gun-control" -> false,
      "http://www.theguardian.com/world/usa" -> false,
      "http://www.theguardian.com/" -> false
    )

    assertUrls(Partners.THE_GUARDIAN, urlsAndExpectations)
  }

  @Test def testHealthCentral(): Unit = {
    val urlsAndExpectations = Seq(
      "http://www.healthcentral.com/depression/cf/slideshows/12-ways-to-boost-self-control?ic=help" -> true,
      "http://www.healthcentral.com/chronic-pain/cf/slideshows/10-common-chronic-pain-myths?ic=help" -> true,
      "http://www.healthcentral.com/adhd/cf/slideshows/10-tips-for-keeping-a-house-clean-despite-adhd?ic=8800" -> true,
      "http://www.healthcentral.com/adhd/cf/slideshows" -> false,
      "http://www.healthcentral.com/clinical-trials/cf/teen-depression?ic=recch" -> false,
      "http://www.healthcentral.com/heart-disease/c/647186/164347/atrial-fibrillation?ic=help" -> false,
      "http://www.healthcentral.com/heart-disease/" -> false,
      "http://www.healthcentral.com/dailydose/cf/2014/01/17/moving_more_sitting_less_improves_quality_of_life/comments" -> false,
      "http://www.healthcentral.com/" -> false
    )

    assertUrls(Partners.HEALTH_CENTRAL, urlsAndExpectations)
  }

  @Test def testDrugs(): Unit = {
    val urlsAndExpectations = Seq(
      "http://www.drugs.com/answers/medications-side-effects-1172215.html" -> true,
      "http://www.drugs.com/answers/" -> false,

      "http://www.drugs.com/article/weight-gain.html" -> true,
      "http://www.drugs.com/article/" -> false,

      "http://www.drugs.com/cg/potassium-content-of-foods-list.html" -> true,
      "http://www.drugs.com/cg/" -> false,

      "http://www.drugs.com/condition/hypertension.html" -> true,
      "http://www.drugs.com/condition/" -> false,

      "http://www.drugs.com/dosage/ibuprofen.html" -> true,
      "http://www.drugs.com/dosage/" -> false,

      "http://www.drugs.com/drug-interactions/metformin.html" -> true,
      "http://www.drugs.com/drug-interactions/" -> false,

      "http://www.drugs.com/health-guide/hemorrhagic-stroke.html" -> true,
      "http://www.drugs.com/health-guide/" -> false,

      "http://www.drugs.com/mtm/multivitamin.html" -> true,
      "http://www.drugs.com/mtm/" -> false,

      "http://www.drugs.com/news/city-living-tied-more-anxiety-mood-disorders-32119.html" -> true,
      "http://www.drugs.com/news/" -> false,

      "http://www.drugs.com/pregnancy/ibuprofen.html" -> true,
      "http://www.drugs.com/pregnancy/" -> false,

      "http://www.drugs.com/price-guide/amlodipine" -> true,
      "http://www.drugs.com/price-guide/" -> false,

      "http://www.drugs.com/sfx/metformin-side-effects.html" -> true,
      "http://www.drugs.com/sfx/" -> false,

      "http://www.drugs.com/" -> false
    )

    assertUrls(Partners.DRUGS, urlsAndExpectations)
  }

  @Test def testEspn(): Unit = {
    val urlsAndExpectations = Seq(
      "http://espn.go.com/los-angeles/mlb/story/_/id/9609775/yasiel-puig-pulled-los-angeles-dodgers-game-apparent-disciplinary-reason" -> true,
      "http://espn.go.com/new-york/nfl/story/_/id/9608017/joe-namath-rex-ryan-new-york-jets-no-rhyme-reasoning-that" -> true,
      "http://insider.espn.go.com/fantasy/football/story/_/id/9585757/fantasy-football-measuring-rb-scoring-chances" -> true,
      "http://espn.go.com/blog/boston/red-sox/post/_/id/30546/bogaerts-gets-first-start-at-fenway" -> true,
      "http://scores.espn.go.com/mlb/recap?gameId=330828120" -> true,
      "http://espn.go.com/boston/" -> false
    )

    assertUrls(Partners.ESPN, urlsAndExpectations)
  }

  @Test def testIsWhitelistCheckAvailableForSite(): Unit = {
    assertTrue("GadgetReview Should have a Whitelist Check!", ArticleWhitelist.isWhitelistCheckAvailableForSite(ArticleWhitelist.siteGuid(_.GADGETREVIEW)))
    assertFalse("Empty string should NOT have a Whitelist Check!", ArticleWhitelist.isWhitelistCheckAvailableForSite(""))
  }

  @Test def testWebMD(): Unit = {
    val urls = Seq(
      "http://www.webmd.com/heartburn-gerd/default.htm" -> false,
      "http://www.webmd.com/heartburn-gerd/news/20130523/chronic-heartburn-may-raise-odds-for-throat-cancer-study" -> true
    )

    assertUrls(Partners.WEBMD, urls)
  }

  @Test def testGadgetReview(): Unit = {
    val urls = Seq(
      "http://www.gadgetreview.com/2013/07/dodges-tomahawk-motorcycle-is-no-joke-with-500-horsepower.html" -> true,
      "http://www.gadgetreview.com/smartphones/nokia-lumia-928-windows-phone-review" -> true,
      "http://www.gadgetreview.com/tag/dodge" -> false,
      "http://www.gadgetreview.com/categories/autos" -> false,
      "http://www.gadgetreview.com/page/2" -> false,
      "http://www.gadgetreview.com/" -> false,
      "http://www.gadgetreview.com" -> false
    )

    assertUrls(Partners.GADGETREVIEW, urls)
  }

  @Test def testUrlShouldBeCrawled(): Unit = {
    val urls = Seq(
      "http://aol.sportingnews.com/nba/story/2012-11-25/kevin-mchale-daughter-dies-alexandra-sasha-houston-rockets" -> true,
      "http://aol.sportingnews.com/nfl/story/2012-11-22/nfls-dirtiest-player-ndamukong-suh-kicks-texans-qb-matt-schaub-in-groin-area" -> true,
      "http://aol.sportingnews.com/nba/story/2012-11-28/andray-blatche-brooklyn-nets-washington-wizards-interview" -> true,
      "http://aol.sportingnews.com/soccer/story/2012-11-27/david-beckham-landon-donovan-retirement-la-galaxy-houston-dynamo-mls-cup" -> true,
      "http://aol.sportingnews.com/nfl/stories" -> false,
      "http://brabungian.com/nfl/stories" -> true,
      "http://brabungian.com/" -> false,
      "http://brabungian.com" -> false,
      "http://www.vidtomp3.com/middle.php?server=srv29&hash=4peTcGpk5KWmqWtx4pSWbXFnnmVsbHG0vMzHrKid2GU%253D" -> true,
      "http://testme.com?query=hi" -> true,
      "http--I'm not even a URL O_o" -> false
    )

    urls.foreach{case (url, shouldBeCrawled)=>
      Assert.assertEquals("URL: " + url + " should be craweld: " + shouldBeCrawled, shouldBeCrawled, ArticleWhitelist.urlCanBeCrawled(url))
    }
  }

  @Test def testEnglish(): Unit = {
    val url = "http://nl.scribd.com/doc/54204613/108/Bingo"
    val smallDog = ArticleWhitelist.isScribdArticle(url)
    val stillSubmit = (!smallDog) || ArticleWhitelist.isEnglishScribdArticle(url)
    println(smallDog)
    println(stillSubmit)
  }

  @Test def testWetPaint(): Unit = {
    val urls = Seq("http://www.wetpaint.com/kourtney-and-kim-take-new-york/articles/kim-kardashians-ex-nick-cannon-couldnt-get-over-her-sex-tape-report" -> true)
    assertUrls(Partners.WETPAINT, urls)
  }

  @Test def testSportingNews(): Unit = {
    val urls = Seq(
      "http://aol.sportingnews.com/nba/story/2012-11-25/kevin-mchale-daughter-dies-alexandra-sasha-houston-rockets" -> true,
      "http://aol.sportingnews.com/nfl/story/2012-11-22/nfls-dirtiest-player-ndamukong-suh-kicks-texans-qb-matt-schaub-in-groin-area" -> true,
      "http://aol.sportingnews.com/nba/story/2012-11-28/andray-blatche-brooklyn-nets-washington-wizards-interview" -> true,
      "http://aol.sportingnews.com/soccer/story/2012-11-27/david-beckham-landon-donovan-retirement-la-galaxy-houston-dynamo-mls-cup" -> true,
      "http://aol.sportingnews.com/nfl/stories" -> false
    )

    assertUrls(Partners.SPORTING_NEWS, urls)
  }

  @Test def testCbsSportsPartnerName(): Unit = {
    val url = "http://fantasynews.cbssports.com/fantasyfootballtoday/KZc8vqQNf674"
    ArticleWhitelist.getPartnerName(url) match {
      case Some(name) => assertEquals(Partners.CBS_SPORTS, name)
      case None => fail("No partner name found for URL: " + url)
    }
  }

  @Test def testYahooOlympics(): Unit = {
    val urls = Seq(
      UrlCandidate("http://sports.yahoo.com/news/olympic-torch-becomes-cultural-happening-190838778--oly.html", true),
      UrlCandidate("http://sports.yahoo.com/news/britons-cheer-torch-relay-halfway-mark-175403121--oly.html", true),
      UrlCandidate("http://sports.yahoo.com/news/olympics--flojo-s-legacy-very-much-alive-24-years-after-her-triumphs-in-seoul.html", true),
      UrlCandidate("http://sports.yahoo.com/blogs/olympics-fourth-place-medal/three-years-being-vegetative-state-teen-sets-two-163339202--oly.html", true),
      UrlCandidate("http://sports.yahoo.com/photos/olympics-20-hottest-olympic-athletes-slideshow/", true),
      UrlCandidate("http://sports.yahoo.com/photos/olympics-20-hottest-olympic-athletes-slideshow/ryan-lochte-photo-1343484901.html", true),
      UrlCandidate("http://sports.yahoo.com/news/john-cena-make-a-wish-300-requests-pro-wrestling-wwe.html", false, false),
      UrlCandidate("http://sports.yahoo.com/blogs/olympics-fourth-place-medal/", false),
      UrlCandidate("http://sports.yahoo.com/olympics/", false)
    )

    assertUrlCandidates(urls, Partners.YAHOO_OLYMPICS, Partners.partnerToSiteGuid.get(Partners.YAHOO_OLYMPICS))
  }

  @Test def testWalyou(): Unit = {
    val urls = Seq(
      "http://walyou.com/iron-man-sneakers/" -> true,
      "http://walyou.com/enterprise-real/" -> true,
      "http://walyou.com/xbox-360-controller-bullets/" -> true,
      "http://walyou.com/how-much-is-mark-zuckerberg-worth/" -> true,
      "http://walyou.com/sexy-geek-bathing-suits/" -> true,
      "http://walyou.com" -> false,
      "http://walyou.com/" -> false,
      "http://walyou.com/page/2/" -> false,
      "http://walyou.com/category/chris-bissell/" -> false,
      "http://walyou.com/tag/kitten-wrangler/" -> false,
      "http://walyou.com/video/" -> false
    )

    assertUrls(Partners.WALYOU, urls)
  }

  @Test def testSuite101(): Unit = {
    val urls = Seq(
      "http://suite101.com/article/solutions_for_a_dogs_dry_skin_shedding-a66215" -> true,
      "http://suite101.com/article/cat-hairball-causes-and-vomiting-in-cats-a60894" -> true,
      "http://suite101.com/article/increase-revenue-and-stimulate-business-growth-through-marketing-a406117" -> true,
      "http://suite101.com/article/coping-behaviours-used-to-respond-to-stress-in-the-21st-century-a397198" -> true,
      "http://suite101.com/mia-carter/articles" -> false,
      "http://suite101.com/mia-carter/articles/2" -> false,
      "http://community.suite101.com/about" -> false,
      "http://community.suite101.com/support/new-suite101" -> false,
      "http://suite101.com/" -> false,
      "http://suite101.com/businessandfinance" -> false,
      "http://community.suite101.com/join" -> false
    )

    assertUrls(Partners.SUITE_101, urls)
  }

  @Test def testDyingScene(): Unit = {
    val urls = Seq(
      "http://dyingscene.com/news/track-by-track-the-used-%e2%80%93-%e2%80%9cvulnerable%e2%80%9d/" -> true,
      "http://dyingscene.com/news/confirmed-chuck-ragan-mike-herrera-mxpx-and-more-added-to-warped-tour-2012-acoustic-basement/" -> true,
      "http://dyingscene.com/news/green-day-to-release-three-album-trilogy-beginning-late-this-year/" -> true,
      "http://dyingscene.com/news/interview-dying-scene-founder-on-running-the-webs-most-badass-punk-news-site/" -> true,
      "http://dyingscene.com/news/the-misfits-to-reissue-walk-among-us-for-record-store-day/" -> true,
      "http://dyingscene.com/page/2/" -> false,
      "http://dyingscene.com/shows/global-parasite-at-wolverhampton-punks-picnic-5272012/" -> false,
      "http://dyingscene.com/labels/financial-records/" -> false,
      "http://dyingscene.com/" -> false,
      "http://dyingscene.com/advertise-with-us" -> false,
      "http://dyingscene.com/?p=210411" -> false,
      "http://dyingscene.com/bands/thesheds/" -> false
    )

    assertUrls(Partners.DYING_SCENE, urls)
  }

  @Test def testMensFitness(): Unit = {
    val urls = Seq(
      "http://www.mensfitness.com/training/build-muscle/mma-endurance-workout-iii" -> true,
      "http://www.mensfitness.com/training/build-muscle/10-ways-to-gain-muscle0" -> true,
      "http://www.mensfitness.com/training/build-muscle/mma-endurance-workout-i" -> true,
      "http://www.mensfitness.com/nutrition/supplements/natural-t-booster" -> true,
      "http://www.mensfitness.com/polls/whats-your-strongest-body-part" -> true,
      "http://www.mensfitness.com/training/build-muscle/what-do-you-think-is-the-average-arm-measurement-for-guys-who-lift" -> true,
      "http://www.mensfitness.com/women/galleries/fit-for-her-corinne-morrill" -> true,
      "http://www.mensfitness.com/women/dating-advice/dont-ask-dont-tell" -> true,
      "http://www.mensfitness.com/poll/56480/results" -> true,
      "http://www.mensfitness.com/poll/56415/results" -> true,
      "http://www.mensfitness.com/nutrition/what-to-eat/eat-your-way-to-a-six-pack" -> true,
      "http://www.mensfitness.com/nutrition/what-to-eat/the-best-fruit-for-men" -> true,
      "http://www.mensfitness.com/leisure/sports/greatest-mma-fighters-without-a-major-title" -> true,
      "http://www.mensfitness.com/bench" -> true,
      "http://www.mensfitness.com/stuff/reviews/shoe-guide-2011" -> true,
      "http://www.mensfitness.com/" -> false,
      "http://www.mensfitness.com/terms" -> false,
      "http://www.mensfitness.com/privacy" -> false,
      "http://www.mensfitness.com/search/apachesolr_search/sweeten%20her%20mood" -> false,
      "http://www.mensfitness.com/contact" -> false
    )

    for ((url, expectInvalid) <- urls) {
      assertUrl(url, Partners.MENSFITNESS, expectInvalid)
    }
  }

  @Test def testSFGate(): Unit = {
    val urls = Seq(
      "http://blog.sfgate.com/49ers/2012/03/28/report-49ers-agree-to-terms-with-rb-brandon-jacobs/" -> true,
      "http://blog.sfgate.com/49ers/2012/03/29/how-did-randy-moss-look-well-harbaugh-was-47-of-48/" -> true,
      "http://blog.sfgate.com/gettowork/2012/03/29/should-potential-employers-have-access-to-your-social-network/" -> true,
      "http://blog.sfgate.com/hottopics/2012/03/28/best-year-of-your-life-is-33-survey-says/" -> true,
      "http://blog.sfgate.com/hottopics/2012/03/28/man-saws-off-his-foot-to-avoid-work/" -> true,
      "http://blog.sfgate.com/hottopics/2012/03/28/ron-burgundy-announces-%e2%80%98anchorman%e2%80%99-sequel-on-conan/" -> true,
      "http://blog.sfgate.com/hottopics/2012/03/29/bohemian-rhapsody%e2%80%99-the-version-you-have-to-hear/" -> true,
      "http://blog.sfgate.com/hottopics/2012/03/29/now-you-can-be-buried-in-a-bacon-coffin/" -> true,
      "http://blog.sfgate.com/nov05election/2012/03/28/george-miller-confident-after-final-day-of-health-care-arguments/" -> true,
      "http://blog.sfgate.com/nov05election/2012/03/28/rick-santorum-arrives-in-ca-tomorrow-so-wheres-the-cash-coming-from/" -> true,
      "http://blog.sfgate.com/nov05election/2012/03/28/shocker-ca-state-assemblyman-nathan-fletcher-abandons-gop-to-get-things-done-as-independent/" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/28/DDJ61NQQ96.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/28/MN6C1NR2DK.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/28/MNLA1NPK9N.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/28/MNMV1NR2MA.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/29/BAIM1NRFH0.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/29/BAS31NRKL3.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/29/DD3A1NPGC7.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/g/a/2012/03/27/hearstmaghealth6643161.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/g/a/2012/03/28/bloomberg_articlesM1LQY76TTDS001-M1M3T.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/g/a/2012/03/29/trayvon_martin_cartoon.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/25/national/w075936D39.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/27/financial/f190139D39.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/27/international/i013238D82.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/27/national/a111537D49.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/27/national/a205753D40.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a030027D59.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a101904D10.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a102350D23.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a133419D89.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a135545D02.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a170553D86.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/a191515D02.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/national/w153930D38.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/sports/s214224D10.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/28/state/n060722D18.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/29/national/a052937D11.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/29/national/a065243D04.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/n/a/2012/03/29/national/a085840D34.DTL" -> true,
      "http://www.sfgate.com/cgi-bin/article/comments/view?f=/c/a/2012/03/27/DDFU1NQCIA.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/article/comments/view?f=/c/a/2012/03/28/BAS31NRKL3.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/article/comments/view?f=/n/a/2012/03/26/national/a142150D15.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/article/comments/view?f=/n/a/2012/03/28/national/a030027D59.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/object/article?f=/c/a/2008/07/09/FDTA11HHDM.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/object/article?f=/c/a/2011/10/09/FD561KQ43H.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/object/article?f=/c/a/2012/02/05/LVBU1MVK01.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/object/article?f=/c/a/2012/03/28/BURQ1NQH50.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/object/article?f=/g/a/2010/09/23/fall_cooking_recipes_.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/object/article?f=/g/a/2011/04/05/top_100_restaurants_2011_photos.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/object/article?f=/g/a/2011/04/22/12_egg_recipes.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/object/article?f=/g/a/2011/05/19/20_Italian_recipes.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/object/article?f=/g/a/2011/09/22/weeknight_dinner_recipes.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/object/article?f=/g/a/2012/03/28/mexico_mix_cabo_pulmo.DTL" -> false,
      "http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/19/BA0T1NN13J.DTL" -> true,
      "http://insidescoopsf.sfgate.com/blog/2012/03/19/james-beard-awards-2012-nominees-announced-bay-area-fares-well-again/" -> true,
      "http://blog.sfgate.com/bronstein/2011/08/16/bart-spokesperson-no-right-to-free-speech-inside-bart/" -> true,
      "http://blog.sfgate.com/bronstein/" -> false,
      "http://blog.sfgate.com/framerate/2012/03/21/super-hooper/" -> true
    )

//    println("Expected To Be VALID:")
//    for {
//      (url, ev) <- urls
//      if (ev)
//    } println(url)
//
//    println()
//
//    println("Expected NOT To Be VALID:")
//    for {
//      (url, ev) <- urls
//      if (!ev)
//    } println(url)
//
//    println()

    for ((url, expectValid) <- urls) {
      assertUrl(url, Partners.SFGATE, expectValid)
    }
  }

  @Ignore @Test def testVoices(): Unit = {
    val feedUrl = "http://voices.yahoo.com/rss/recent_15.xml"

    println("Retrieving URLs to verify from feedUrl: " + feedUrl)

    val rss = scala.xml.XML.load(new URL(feedUrl))

    for {
      item <- rss \\ "item"
      url = (item \ "link").text.drop(1)
    } {
      assertUrl(url, Partners.VOICES)

    }
  }

  @Test def testVoicesRootUrl(): Unit = {
    val urls = Seq("http://voices.yahoo.com/","http://voices.yahoo.com")
    for (url <- urls) {
      assertFalse("Site root URL should NOT pass our whitelist!", ArticleWhitelist.isValidPartnerArticle(url))
    }
  }

  @Test def testBoston(): Unit = {
    val urls = Seq("http://www.boston.com/business/markets/articles/2012/07/27/us_futures_up_on_hope_for_unified_european_action/" -> true,
    "http://www.boston.com/ae/restaurants/boston-best-burgers/1iHkgP3ONuwRnQf4K8Z4gI/gallery.html" -> true,
    "http://www.boston.com/business/technology/video/?bctid=1753676054001&pconnect_name=677824" -> true,
    "http://calendar.boston.com/boston_ma/events/show/252579405-oar" -> true,
    "http://articles.boston.com/something/more/here" -> false,
    "http://www.boston.com/sports/other_sports/olympics/extras/olympics_blog/2012/07/day_1_morning_r.html" -> true)

    assertUrls(Partners.BOSTON, urls)
  }

  @Test def testTechcrunch(): Unit = {
    val urls = Seq("http://techcrunch.com/2013/07/05/roost-laptop-stand/" -> true,
      "http://techcrunch.com/2013/06/27/facebook-related-hashtags/" -> true,
      "http://techcrunch.com/2013/07/05/google-glass-could-soon-get-device-locking-music-player-and-boutique-app-store-firmware-reveals/" -> true,
      "http://techcrunch.com/2013/06/27/google-looking-for-groups-to-provide-the-next-hikers-to-don-trekker-street-view-backpack/" -> true,
      "http://techcrunch.com/2013/07/04/apple-exploring-earbud-tech-that-can-compensate-for-specific-fit-in-each-users-ear/" -> true)

    assertUrls(Partners.TECHCRUNCH, urls)
  }

  @Ignore @Test def testBostonFromRss(): Unit = {
    val siteMap = scala.xml.XML.load(new URL("http://www.boston.com/news/google-news-sitemap.xml"))

    def verifyURL(url: String, shouldBeValid: Boolean = true) {
      ArticleWhitelist.getPartnerName(url) match {
        case Some(name) => assertEquals("URL Should be for partner: 'boston'", Partners.BOSTON, name)
        case None => fail("Partner name should be 'boston', but instead was not found for URL: " + url)
      }

      assertEquals(url + " should be a valid article URL!", shouldBeValid, ArticleWhitelist.isValidPartnerArticle(url))
      val msg = if (shouldBeValid) "was valid as expected" else "was NOT valid as expected"
      println(url + " " + msg)
    }

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

      verifyURL(url)
    }

    val pleaseFailUrl = "http://www.boston.com/sports/football/patriots/"
    verifyURL(pleaseFailUrl, false)

    val pleasePassUrl = "http://articles.boston.com/2012-02-02/lifestyle/31018031_1_boston-beer-ban-sales-shaun-clancy"
    verifyURL(pleasePassUrl)

    val galleryOkUrl = "http://www.boston.com/sports/photogallery/boston_captains/"
    verifyURL(galleryOkUrl)

    val photoGalleryOkUrl = "http://www.boston.com/lifestyle/fashion/gallery/hilarious_avant_garde_fashion_paris_fall_2012/"
    verifyURL(photoGalleryOkUrl)
  }

  @Test def testWesh(): Unit = {
    val urls = Seq(
      "http://www.wesh.com/news/29736268/detail.html?fb_ref=detail_recommendations",
      "http://www.wesh.com/casey-anthony-extended-coverage/29738558/detail.html"
    )

    val badUrls = Seq(
      "http://www.wesh.com/slideshow/slideshows/26090597/detail.html",
      "http://www.wesh.com/video/29736213/detail.html"
    )

    for (url <- urls) {
      val failMsg = "URL: '%s' should match partner: '%s'".format(url, Partners.WESH)
      ArticleWhitelist.getPartnerName(url) match {
        case Some(pname) => assertEquals(failMsg, Partners.WESH, pname)
        case None => fail(failMsg)
      }

      assertTrue("URL: '%s' should be a valid partner article!".format(url), ArticleWhitelist.isValidPartnerArticle(url))
    }

    for (url <- badUrls) {
      val failMsg = "URL: '%s' should match partner: '%s'".format(url, Partners.WESH)
      ArticleWhitelist.getPartnerName(url) match {
        case Some(pname) => assertEquals(failMsg, Partners.WESH, pname)
        case None => fail(failMsg)
      }

      assertFalse("Bad URL: '%s' should NOT be a valid partner article!".format(url), ArticleWhitelist.isValidPartnerArticle(url))
    }
  }

  @Test def testYahoo(): Unit = {

    val urls = Seq(
      UrlCandidate("http://news.yahoo.com/feds-charge-7-internet-ad-fraud-case-165522124.html;_ylt=AsscMlQqNo53.SNedeGoPNCs0NUE;_ylu=X3oDMTNpYWNjZ2kyBG1pdAMEcGtnAzc1NGRiMDRjLWQ4NGYtM2VhYi1iY2U5LTljZDQ3YTk2MzI5NwRwb3MDMQRzZWMDbG5fTGF0ZXN0TmV3c19nYWwEdmVyA2I0OTUzYjUwLTBiMDAtMTFlMS04YmY3LThiMTNmOWJjNDY1MA--;_ylv=3", true),
      UrlCandidate("http://news.yahoo.com/blogs/technology-blog/just-show-create-friends-list-facebook-174736732.html", true),
      UrlCandidate("http://news.yahoo.com/thousands-defy-norway-mass-killer-breivik-song-102440602.html", true),
      UrlCandidate("http://news.yahoo.com/zynga-reports-1q-net-loss-higher-revenue-202218357--finance.html", true),
      UrlCandidate("http://news.yahoo.com/murdoch-hacking-scandal-changed-entire-company-165150725--finance.html", true),
      UrlCandidate("http://news.yahoo.com/hubbub-over-content-rights-greets-google-drive-040418608--finance.html", true),
      UrlCandidate("http://news.yahoo.com/video/pittsburghkdka-15751084/facebook-founder-recruits-at-carnegie-mellon-university-27198168.html", false),
      UrlCandidate("http://news.yahoo.com/photos/penn-state-s-joe-paterno-retiring-1320853217-slideshow/", false),
      UrlCandidate("http://news.yahoo.com/most-popular/2.html", false),
      UrlCandidate("http://news.yahoo.com/most-popular/2.html;_yt=blah;_ylu=who;_ynot=cares", false),
      UrlCandidate("http://news.yahoo.com/local/napa-ca-12797350.html", false),
      UrlCandidate("http://news.yahoo.com/", false),
      UrlCandidate("http://sports.yahoo.com/news/fresh-protests-brazil-despite-government-concessions-215945490.html", true, false),
      UrlCandidate("http://uk.news.yahoo.com/schools-warn-reforms-chaos-053754468.html",true, false)
    )

    assertUrlCandidates(urls, Partners.YAHOO_NEWS, Partners.partnerToSiteGuid.get(Partners.YAHOO_NEWS))
  }

  @Test def testYahooNewsWhitelist(): Unit = {
    val urls = Seq(
      "http://uk.news.yahoo.com/schools-warn-reforms-chaos-053754468.html"
    )

    urls.foreach{url=>
      import grvstrings._
      println("AUTHORITY: " + url.tryToURL.get.getAuthority)
      Assert.assertTrue(ArticleWhitelist.isValidPartnerArticle(url))
    }
  }

  @Test def testWsjUrlsForAllElse(): Unit = {
    val urls = List(
      "http://online.wsj.com/article/SB10001424053111903532804576566503132048630.html?mod=WSJ_Election_MIDDLEThirdStories" -> true,
      "http://blogs.wsj.com/speakeasy/2011/09/14/scarlett-johansson-nude-photos-part-of-fbi-probe-of-digital-theft/" -> true,
      "http://online.wsj.com/mdc/public/page/marketsdata.html" -> false
    )

    assertUrls(Partners.WSJ, urls)
  }

  @Test def testCnnMoneyPartnerName(): Unit = {
    val urls = List("http://money.cnn.com/2011/09/09/news/economy/obama_jobs_pay_for/index.htm",
      "http://money.cnn.com/video/technology/2011/12/02/an_soundcloud_profile.cnnmoney/",
      "http://tech.fortune.cnn.com/2011/12/05/chart-of-the-day-ipad-iphone-dominate-couch-commerce",
      "http://postcards.blogs.fortune.cnn.com/2011/12/05/google-marissa-mayer/",
      "http://money.cnn.com/2011/12/05/news/international/oil_iran/index.htm",
      "http://money.cnn.com/video/news/2011/12/02/n_engineers_quit.cnnmoney/",
      "http://tech.fortune.cnn.com/2011/12/02/google-amazon-prime",
      "http://tech.fortune.cnn.com/2011/09/09/gamestop-ceo-talks-ios-devices/")

    for (url <- urls) {
      ArticleWhitelist.getPartnerName(url) match {
        case Some(name) => {
          assertEquals(ArticleWhitelist.Partners.CNN_MONEY, name)
          printf("YAY! url: '%s' matched CNN Money: '%s'%n", url, ArticleWhitelist.Partners.CNN_MONEY)
        }
        case None => fail("Failed to get a partner name for the following URL: " + url)
      }
    }
  }

  @Test def testCnnMoneyIsValid(): Unit = {
    val urls = Seq("http://money.cnn.com/2011/09/09/news/economy/obama_jobs_pay_for/index.htm" -> true,
      "http://money.cnn.com/video/technology/2011/12/02/an_soundcloud_profile.cnnmoney/" -> true,
      "http://postcards.blogs.fortune.cnn.com/2011/12/05/google-marissa-mayer/" -> true,
      "http://tech.fortune.cnn.com/2011/12/05/chart-of-the-day-ipad-iphone-dominate-couch-commerce" -> true,
      "http://buzz.money.cnn.com/2012/11/02/linkedin-stock-facebook-earnings/" -> true,
      "http://tech.fortune.cnn.com/2011/09/09/gamestop-ceo-talks-ios-devices/" -> true,
      "http://money.cnn.com/search/index.html?sortBy=date&primaryType=mixed&search=Search&query=" -> false,
      "http://money.cnn.com/services/bridge/contact.us.html" -> false,
      "https://subscription.money.com/storefront/subscribe-to-money/site/mo-hotenthwave1111.html?link=1003748" -> false
    )

    assertUrls(ArticleWhitelist.Partners.CNN_MONEY, urls)
  }

  @Test def testTimeWordpressArticle(): Unit = {
    val url1 = "http://swampland.time.com/2011/08/04/congress-reaches-deal-to-end-faa-shutdown/"
    val url2 = "http://newsfeed.time.com/2010/12/01/top-yahoo-searches-of-2010-bieber-has-nothing-on-miley-but-the-oil-spill-trumped-them-all/"

    assertEquals("Partner name should be time!", Partners.TIME, ArticleWhitelist.getPartnerName(url1).get)
    assertTrue("isTimeWordpressArticle should be TRUE for this URL: " + url1, ArticleWhitelist.isTimeWordpressArticle(url1))
    assertTrue("isValidPartnerArticle should be TRUE for this URL: " + url1, ArticleWhitelist.isValidPartnerArticle(url1))

    assertEquals("Partner name should be time for this URL: " + url2, Partners.TIME, ArticleWhitelist.getPartnerName(url2).get)
    assertTrue("isTimeWordpressArticle should be TRUE for this URL: " + url2, ArticleWhitelist.isTimeWordpressArticle(url2))
    assertTrue("isValidPartnerArticle should be TRUE for this URL: " + url2, ArticleWhitelist.isValidPartnerArticle(url2))
  }

  @Test def testTimeOtherArticle(): Unit = {
    val urls =
      // "http://www.time.com/time/nation/article/0,8599,2090658,00.html" ::
      "http://www.time.com/time/nation/article/0,8599,2090658,00.html" ::
      "http://www.time.com/time/travel/cityguide/article/0,31489,1838100,00.html?foo=bar&baz" ::
      "http://www.time.com/time/specials/packages/article/0,28804,2026474_2026675,00.html" ::
      Nil

    for (url <- urls) {
      assertEquals("Partner name should be time!", Partners.TIME, ArticleWhitelist.getPartnerName(url).get)
      assertTrue("isTimeOtherArticle should be TRUE for this URL: " + url, ArticleWhitelist.isTimeOtherArticle(url))
    }
  }

  @Test def testScribdUrls(): Unit = {
    val url = "http://www.scribd.com/doc/43895865/Criminal-Complaint-Against-Expert-Networks-Executive-Don-Chu"
    val url2 = "http://www.scribd.com/press"
    val url3 = "http://www.scribd.com/search?query=doc"
    val url4 = "http://www.scribd.com/doc/43684363/New-Reviews-Category-Share-your-thoughts-with-the-world"
    val url5 = "http://www.ScribD.com/doc/999998/Testing-capital-letters"

    if (ArticleWhitelist.getPartnerName(url).get == "scribd") {
      assertTrue(ArticleWhitelist.isScribdArticle(url))
    } else fail("should be scribd")

    if (ArticleWhitelist.getPartnerName(url2).get == "scribd") {
      assertFalse(ArticleWhitelist.isScribdArticle(url2))
    } else fail("should be scribd")

    if (ArticleWhitelist.getPartnerName(url3).get == "scribd") {
      assertFalse(ArticleWhitelist.isScribdArticle(url3))
    } else fail("should be scribd")

    if (ArticleWhitelist.getPartnerName(url4).get == "scribd") {
      assertTrue(ArticleWhitelist.isScribdArticle(url4))
    } else fail("should be scribd")

    if (ArticleWhitelist.getPartnerName(url5).get == "scribd") {
      assertTrue(ArticleWhitelist.isScribdArticle(url5))
    } else fail("should be scribd")
  }

  @Test def testPartnerURL(): Unit = {
    val url = "http://clarissavengeance.buzznet.com/user/journal/8120531/bristol-palin-reality-show/"
    val url2 = "http://pattygopez.buzznet.com/user/journal/8126591/bristol-palin-admits-having-plastic/"
    val url3 = "http://www.scribd.com/doc/43801484/Mr-President-Start-Playing-Politics"
    val badurl = "http://neatorama/user/photos/"


    assertTrue(ArticleWhitelist.getPartnerName(url).get == "buzznet")
    assertTrue(ArticleWhitelist.getPartnerName(url2).get == "buzznet")
    assertTrue(ArticleWhitelist.getPartnerName(url3).get == "scribd")



    ArticleWhitelist.getPartnerName(badurl) match {
      case Some(result) => fail("Should have returned a None")
      case None =>
    }
  }


  // notice how dogster is before catster, that is not accidental :)
  @Test def testDogsterUrls(): Unit = {

    val good1 = "http://www.dogster.com/puppies/How-to-Keep-Your-Pet-Safe-with-Microchipping-and-Tagging-70"
    val good2 = "http://www.dogster.com/dog-health-care/dog-obesity"
    val good3 = "http://www.dogster.com/dog-food/how-to-choose-a-healthy-dog-food"
    val good4 = "http://www.dogster.com/puppies/puppy-socialization"
    val good5 = "http://www.dogster.com/dog-adoption/costs-to-adopt-a-dog"
    val good6 = "http://www.dogster.com/dog-travel/dog-car-travel"
    val good7 = "http://www.dogster.com/dogs-101/dog-boarding"
    val good8 = "http://dogblog.dogster.com/2011/05/18/tornado-dog-crawls-home-with-two-shattered-legs"
    val good9 = "http://dogblog.dogster.com/2011/05/13/special-photo-essay-dogs-of-war/"

    val bad1 = "http://www.dogster.com/group/Fancypants_cafe_where_everybody_knows_your_name-7838"
    val bad2 = "http://www.dogster.com/forums/Dog_Health"
    val bad3 = "http://www.dogster.com/diary/dcentral.php"

    val goodURLS = good1 :: good2 :: good3 :: good4 :: good5 :: good6 :: good7 :: good8 :: good9 :: Nil

    val badURLS = bad1 :: bad2 :: bad3 :: Nil

    for (url <- goodURLS) {
      assertTrue("should be dogster url: " + url, ArticleWhitelist.isValidPartnerArticle(url))
    }

    for (url <- badURLS) {
      assertFalse("should not be a dogster url: " + url, ArticleWhitelist.isValidPartnerArticle(url))
    }
  }


  @Test def testCatsterUrls(): Unit = {


    val good1 = "http://www.catster.com/cat-food/cat-food-wet-or-dry"
    val good2 = "http://www.catster.com/cat-health-care/cleaning-cat-ears"
    val good4 = "http://www.catster.com/cat-adoption/why-adopt-a-cat"
    val good5 = "http://blogs.catster.com/kitty-news-network/2010/12/17/holiday-miracle-cat-left-to-die-making-full-recovery/"
    val good6 = "http://www.catster.com/cat-health-care/cleaning-cat-ears"
    val good7 = "http://www.catster.com/kittens/How-to-Give-Your-Cat-a-Massage-127"
    val good8 = "http://www.catster.com/cat-behavior/cat-socialization"
    val good9 = "http://www.catster.com/green-cats/how-to-reduce-your-cats-carbon-pawprint"
    val good10 = "http://www.catster.com/cats-101/lost-cat"

    val bad1 = "http://www.catster.com/group/Fancypants_cafe_where_everybody_knows_your_name-7838"
    val bad2 = "http://www.catster.com/forums/Cat_Health/thread/705473"
    val bad3 = "http://www.catster.com/answers/question/what_are_some_simple_ways_to_green_your_cats_lifestyle-48071"
    val bad4 = "http://www.catster.com/register/"
    val bad5 = "http://www.catster.com/games/cat-breed-photo-game/"
    val bad6 = "http://www.catster.com/polls/What_is_your_cats_favorite_thing_to_steal-265"
    val bad7 = "http://www.catster.com/photoviewer/gallery/Bag"
    val bad8 = "http://www.catster.com/cats/1155722"
    val bad9 = "http://www.catster.com/family/1004964"


    val goodURLS = good1 :: good2 :: good4 :: good5 :: good6 :: good7 :: good8 :: good9 :: good10 :: Nil

    val badURLS = bad1 :: bad2 :: bad3 :: bad4 :: bad5 :: bad6 :: bad7 :: bad8 :: bad9 :: Nil

    for (url <- goodURLS) {
      assertTrue("should be catster url: " + url, ArticleWhitelist.isValidPartnerArticle(url))
    }

    for (url <- badURLS) {
      assertFalse("should not be a catster url: " + url, ArticleWhitelist.isValidPartnerArticle(url))
    }
  }

  @Test def testXoJane(): Unit = {
    val goodUrls = List("http://www.xojane.com/fashion/leather-paper-bag",
      "http://www.xojane.com/janes-phone/image/no-problemo",
      "http://www.xojane.com/new-agey/can-trinfinity8-fix-your-soul-math",
      "http://www.xojane.com/new-agey/ask-liz-what-does-my-naked-dream-mean",
      "http://www.xojane.com/beauty/pretty-past-get-penelope-trees-look",
      "http://www.xojane.com/sports/being-ski-bunny-isnt-always-fun-and-games",
      "http://www.xojane.com/entertainment/which-one-these-childhood-favorites-should-i-watch-first",
      "http://www.xojane.com/beauty/sally-hansen-lip-shimmer-plumping-balm",
      "http://www.xojane.com/fashion/what-do-i-wear",
      "http://www.xojane.com/janes-phone/image/im-so-excited-see-tori-spelling")

    for (url <- goodUrls) assertTrue("Should be valid xojane article: " + url, ArticleWhitelist.isValidPartnerArticle(url))
  }
}