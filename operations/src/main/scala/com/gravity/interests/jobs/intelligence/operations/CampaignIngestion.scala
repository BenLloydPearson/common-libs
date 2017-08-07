/**
 * Support for ingesting to campaigns from beacons.
 */

package com.gravity.interests.jobs.intelligence.operations

import com.gravity.algorithms.model.{FeatureSettings, SwitchResult}
import com.gravity.interests.jobs.intelligence._
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.analytics.{IncExcUrlStrings, UrlFields, UrlMatcher}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.web.extraction.HuffPoLiveArticleFetcher
import com.gravity.utilities.{DerivedStampedValue, Settings2}

import scala.collection._
import scalaz._


// Question: Hm, maybe move the optUrlMatcher and matchesUrl() into CampaignIngestionRule?
// Or perhaps change ingestRules to be Seq[CampaignIngestionRule], but then CampaignIngestionRules may have no meaning.
// Maybe make it so that we can ask the question, "matches for beacon ingestion?" or "matches for Rss Filtering?"
// Could filter the Seq[CampaignIngestionRules] based on predicate, then map to Seq[IncExcUrlStr]

case class CampaignRuleMatcher(campKey: CampaignKey,
                               campName: String,
                               incExcStrSeq: Seq[IncExcUrlStrings]) {

  lazy val optUrlMatcher = UrlMatcher.toOptUrlMatcher(incExcStrSeq)

  def matchesURL(url: UrlFields): Boolean = {
    optUrlMatcher match {
      case None => false
      case Some(urlMatcher) => urlMatcher(url)
    }
  }
}

object CampaignIngestion {
 import com.gravity.logging.Logging._
  val counterCategory : String = "Beacon Ingestion"
  import com.gravity.utilities.Counters._

  val huffPoSiteGuid = ArticleWhitelist.siteGuid(_.HUFFINGTON_POST)
  val huffPoSiteKey  = SiteKey(huffPoSiteGuid)
  val huffPoLiveEjectedCampaignKey = CampaignKey(huffPoSiteKey, 8068944056006697001L)

  // Our Content Group:
  //   contentGroupID: 930 == campaignKey: siteId:-7003838957478867962_campaignId:7013770867425671284
  //   https://dashboard.gravity.com/admin/content-group/edit?id=930&sg=95ec266b244de718b80c652a08af06fa ("HuffPost Live (from Beacon)")
  //   http://admintool/gravity-interests-web-0.5-SNAPSHOT-prod/admin/campaign/edit?campaignKey=siteId:-7003838957478867962_campaignId:7013770867425671284
  val huffPoLiveCampaignKey = CampaignKey(huffPoSiteKey, 7013770867425671284L)

  // A clone of the above Content Group at:
  //   "HuffPost LIVE (from beacon)  - HPL EOP"
  //   https://dashboard.gravity.com/admin/content-group/edit?id=2194&sg=95ec266b244de718b80c652a08af06fa&sort=-publish_timestamp
  //   http://admintool/gravity-interests-web-0.5-SNAPSHOT-prod/admin/campaign/edit?campaignKey=siteId:-7003838957478867962_campaignId:8847672577618494668
  val huffPoLiveCampaign2Key = CampaignKey(huffPoSiteKey, 8847672577618494668L)

  // Note: All verticals are passed in after being .toLowerCase()'d, so be sure to set all map keys in lower case to match
  val huffPoLiveVerticalToCampaignKeyMap = Map(
    "entertainment" -> CampaignKey(huffPoSiteKey, 8715640192532417532L)      // CG: 1030
    ,"business" -> CampaignKey(huffPoSiteKey, -5049430713270036376L)         // CG: 1026
    ,"sports" -> CampaignKey(huffPoSiteKey, 4320705657620111071L)            // CG: 1027
    ,"travel" -> CampaignKey(huffPoSiteKey, 3771077325040842654L)            // CG: 1028
    ,"religion" -> CampaignKey(huffPoSiteKey, 7516182131198344969L)          // CG: 1029
    ,"weddings" -> CampaignKey(huffPoSiteKey, 4379139444028517485L)          // CG: 1533
    ,"huff/post50" -> CampaignKey(huffPoSiteKey, -3513023434743967158L)      // CG: 1534
    ,"college" -> CampaignKey(huffPoSiteKey, -4670436691724852546L)          // CG: 1535
    ,"teen" -> CampaignKey(huffPoSiteKey, 5430008079405489537L)              // CG: 1536
    ,"technology" -> CampaignKey(huffPoSiteKey, 2900382734414263925L)        // CG: 1537
    ,"science" -> CampaignKey(huffPoSiteKey, -2510298821550621784L)          // CG: 1538
    ,"latino voices" -> CampaignKey(huffPoSiteKey, -1675202274242526244L)    // CG: 1539
    ,"gay voices" -> CampaignKey(huffPoSiteKey, -2496925185600887078L)       // CG: 1540
    ,"world" -> CampaignKey(huffPoSiteKey, -2119924465082390710L)            // CG: 1621
    ,"chicago" -> CampaignKey(huffPoSiteKey, 231234767513929901L)            // CG: 1577
    ,"dc" -> CampaignKey(huffPoSiteKey, -5720587445842731416L)               // CG: 1578
    ,"denver" -> CampaignKey(huffPoSiteKey, 2060808449317467858L)            // CG: 1579
    ,"detroit" -> CampaignKey(huffPoSiteKey, 7243079174238631044L)           // CG: 1580
    ,"los angeles" -> CampaignKey(huffPoSiteKey, -1294235911294825227L)      // CG: 1581
    ,"miami" -> CampaignKey(huffPoSiteKey, -6552627258024087342L)            // CG: 1582
    ,"new york" -> CampaignKey(huffPoSiteKey, -6418130244305924666L)         // CG: 1583
    ,"san francisco" -> CampaignKey(huffPoSiteKey, -7668047233416237622L)    // CG: 1584
    ,"healthy living" -> CampaignKey(huffPoSiteKey, -3800550460562379304L)   // CG: 1564
    ,"women" -> CampaignKey(huffPoSiteKey, 2504504193462675722L)             // CG: 1623
    ,"arts" -> CampaignKey(huffPoSiteKey, -847484537036271548L)              // CG: 1560
    ,"culture" -> CampaignKey(huffPoSiteKey, 3455293613501301676L)           // CG: 1541
    ,"impact" -> CampaignKey(huffPoSiteKey, 4573387097922721241L)            // CG: 1556
    ,"black voices" -> CampaignKey(huffPoSiteKey, -1105526280065444975L)     // CG: 1557
    ,"celebrity" -> CampaignKey(huffPoSiteKey, 2095415876157636813L)         // CG: 1558
    ,"comedy" -> CampaignKey(huffPoSiteKey, -3170496847581046314L)           // CG: 1559
    ,"books" -> CampaignKey(huffPoSiteKey, 4019604849499106115L)             // CG: 1561
    ,"home" -> CampaignKey(huffPoSiteKey, -263180852887774267L)              // CG: 1562
    ,"good news" -> CampaignKey(huffPoSiteKey, -8257767290618354440L)        // CG: 1563
    ,"green" -> CampaignKey(huffPoSiteKey, 1035633008266415711L)             // CG: 1565
    ,"crime" -> CampaignKey(huffPoSiteKey, -8577804186030072934L)            // CG: 1566
    ,"taste" -> CampaignKey(huffPoSiteKey, -6160409708345478905L)            // CG: 1567
    ,"weird" -> CampaignKey(huffPoSiteKey, 103131833117925071L)              // CG: 1568
    ,"tv" -> CampaignKey(huffPoSiteKey, 5265268900041974994L)                // CG: 1569
    ,"hawaii" -> CampaignKey(huffPoSiteKey, 2328217435066090406L)            // CG: 1570
    ,"politics" -> CampaignKey(huffPoSiteKey, -1737515414846845553L)         // CG: 1571
    ,"small business" -> CampaignKey(huffPoSiteKey, 5092456402839665411L)    // CG: 1572
    ,"gps for the soul" -> CampaignKey(huffPoSiteKey, -4903349503503194303L) // CG: 1573
    ,"media" -> CampaignKey(huffPoSiteKey, 3031169761785847004L)             // CG: 1574
    ,"divorce" -> CampaignKey(huffPoSiteKey, -7771794237083603298L)          // CG: 1575
    ,"style" -> CampaignKey(huffPoSiteKey, 6337145543516196852L)             // CG: 1576
    ,"money" -> CampaignKey(huffPoSiteKey, 162928700811792698L)              // CG: 1585
    ,"education" -> CampaignKey(huffPoSiteKey, 774535490020491695L)          // CG: 1586
    ,"technology" -> CampaignKey(huffPoSiteKey, 2872299084720502938L)        // CG: 1624
  )

  val huffPoLiveUncleanFlags = Set("nsfw", "gory")

  /**
   * If CampaignIngestion has an opinion about the article type of the (url, siteGuid) combo,
   * returns Some(ArticleTypes.Type), e.g. ArticleTypes.content or ArticleTypes.video.  Otherwise returns None.
   */
  def articleTypeOpinion(rawUrl: String, siteGuid: String): Option[ArticleTypes.Type] = {
    if (isUrlBeaconIngestableByHuffPoLive(rawUrl, siteGuid))
      Option(ArticleTypes.video)
    else if (isUrlBeaconIngestableBySite(rawUrl, siteGuid))
      Option(ArticleTypes.content)
    else
      None
  }

  def isUrlBeaconIngestableBySite(rawUrl: String, siteGuid: String) =
    isUrlBeaconIngestableByHuffPoLive(rawUrl, siteGuid) || matchingBeaconIngestionCampaignRuleMatchers(rawUrl, siteGuid).nonEmpty

  private def isUrlBeaconIngestableByHuffPoLive(rawUrl: String, siteGuid: String) =
    siteGuid == huffPoSiteGuid && HuffPoLiveArticleFetcher.isHuffPoLiveUrl(rawUrl)

  private def matchingBeaconIngestionCampaignRuleMatchers(rawUrl: String, siteGuid: String): Seq[CampaignRuleMatcher] = {
    for {
      campRuleMatcher  <- activeBeaconIngestionRuleMatchersForSite(siteGuid).toStream
      urlFields <- UrlMatcher.tryToLowerUrlFields(rawUrl).toList
      if campRuleMatcher.matchesURL(urlFields)
    } yield {
      campRuleMatcher
    }
  }

  val defaultUrlMatcher = IncExcUrlStrings(List(""), List(""), List(""), List("")).toUrlMatcher.get
  /**
   * Returns true if the article at rawUrl is not filtered out by include/exclude rules for Campaign ck.
   */
  def isRssArticleOkForCampaign(ck: CampaignKey, rawUrl: String) = {

    lazy val activeRssFilters = activeRssFilterRuleMatchersForCampaign(ck)

    UrlMatcher.tryToLowerUrlFields(rawUrl) match {
      // If we can't parse the article's URL, then we don't like it.
      case None => false

      case Some(urlFields) if activeRssFilters.nonEmpty =>
        // The article's URL has to pass at least one of the rules.
        activeRssFilters.exists(_.matchesURL(urlFields))

      case Some(urlFields) =>
        // There is no explicit rule so use the defaultUrlMatcher
        defaultUrlMatcher(urlFields)
    }
  }

  // Given a sequence of CampaignRows, returns a possibly-empty sequence of active CampaignRuleMatcher.
  def campRuleMatchersFromCampRows(campRows: Seq[CampaignRow], ciRuleFilter: ((CampaignIngestionRule) => Boolean)) = {
    for {
      cr <- campRows
      if isActiveForIngestion(cr.status)
      incExcStrsSeq = cr.campIngestRules.campIngestRuleSeq.filter(ciRuleFilter).map(_.incExcStrs)
      if incExcStrsSeq.nonEmpty
    } yield {
      CampaignRuleMatcher(cr.campaignKey, cr.nameOrNotSet, incExcStrsSeq)
    }
  }

  // A regularly-updated cached map of SiteKey -> List[CampRuleMatcher], backed by CampaignService.allCampaignMetaObj
  val activeBeaconRuleMatchers = DerivedStampedValue(CampaignService.stampedAcmoIfNewer) {acmo =>
    trace(s"Updating skToCampRuleMatchers for stamp ${acmo.stamp}")

    // Take a Map[SiteKey, Seq[CampaignRow]]...
    val skToCampRowsMap = acmo.bySiteKey

    // ...convert to Map[SiteKey, Seq[CampaignRuleMatcher]]...
    val skToRuleMatchersMap = skToCampRowsMap.mapValues(campRows => campRuleMatchersFromCampRows(campRows, _.useForBeaconIngest))

    // ...and toss out the ones with empty Seq[CampaignRuleMatcher] values.
    skToRuleMatchersMap.filter(_._2.nonEmpty)
  }

  // A regularly-updated cached map of CampaignKey -> List[CampRuleMatcher], backed by CampaignService.allCampaignMetaObj
  val activeRssFilterRuleMatchers = DerivedStampedValue(CampaignService.stampedAcmoIfNewer) {acmo =>
    trace(s"Updating ckToCampRuleMatchers for stamp ${acmo.stamp}")

    // Take a Map[CampaignKey, CampaignRow]...
    val ckToCampRowMap = acmo.allCampaignMeta

    // ...convert to Map[CampaignKey, Seq[CampaignRuleMatcher]]...
    val ckToRuleMatchersMap = ckToCampRowMap.mapValues(campRow => campRuleMatchersFromCampRows(List(campRow), _.useForRssFiltering))

    // ...and toss out the ones with empty Seq[CampaignRuleMatcher] values.
    ckToRuleMatchersMap.filter(_._2.nonEmpty)
  }

  private def activeBeaconIngestionRuleMatchersForSite(siteGuid: String): Seq[CampaignRuleMatcher] = {
    countPerSecond(counterCategory, "Calls to activeBeaconIngestionRuleMatchersForSite")

    activeBeaconRuleMatchers.value.getOrElse(SiteKey(siteGuid), Nil)
  }

  private def activeRssFilterRuleMatchersForCampaign(ck: CampaignKey): Seq[CampaignRuleMatcher] = {
    countPerSecond(counterCategory, "Calls to activeRssFilterRuleMatchersForCampaign")

    activeRssFilterRuleMatchers.value.getOrElse(ck, Nil)
  }

  private def isContentGroupIngestionDisabled(ck: CampaignKey) = {
    val switchResult = FeatureSettings.getScopedSwitch(FeatureSettings.disableContentGroupIngestion, Seq(ck.toScopedKey, ck.siteKey.toScopedKey, EverythingKey.toScopedKey))
    switchResult match {
      case SwitchResult(true,_,Some(scopedKey)) =>
        countPerSecond(counterCategory, "Ingestion skipped total")
        countPerSecond(counterCategory, s"Ingestion skipped for scopedKey ${scopedKey}")

      case SwitchResult(true,_,None) =>
        countPerSecond(counterCategory, "Ingestion skipped total")
        countPerSecond(counterCategory, s"Ingestion skipped for scopedKey None. What?!?! Default Changed?")

      case _ =>
    }

    switchResult.value
  }

  def addBeaconIngestedArticleToMatchingCampaigns(siteGuid: String, articleRow: ArticleRow, gooseArticle: com.gravity.goose.Article): Unit = {
    if (isUrlBeaconIngestableByHuffPoLive(articleRow.url, siteGuid)) {
      handleHuffPoLiveArticle(articleRow.articleKey, gooseArticle)
    }
    else {
      // Add the article to the site's campaigns that like the article's url
      for {
        campRuleMatcher <- matchingBeaconIngestionCampaignRuleMatchers(articleRow.url, siteGuid)
        ck = campRuleMatcher.campKey
        if !isContentGroupIngestionDisabled(ck)
      } {

        CampaignService.addArticleKey(ck,
                                      articleRow.articleKey,
                                      Settings2.INTEREST_SERVICE_USER_ID,
                                      CampaignArticleSettings.getDefault(isActive = true)) match {
          case Success(_) => countPerSecond(counterCategory, s"Articles SUCCESSFULLY beacon-ingested to ${campRuleMatcher.campName}")

          case Failure(fails) =>
            warn(fails, s"Failed to add article `${articleRow.url}` to ${campRuleMatcher.campName}")
            countPerSecond(counterCategory, s"Articles FAILED to be beacon-ingested to ${campRuleMatcher.campName}")
        }
      }
    }
  }

  private def isHuffPoLiveArticleClean(gooseArticle: com.gravity.goose.Article): Boolean = {
    gooseArticle.additionalData.get("content_flags").fold(true)(flagsString => {
      val flags = flagsString.split(",")

      flags.isEmpty || !flags.exists(huffPoLiveUncleanFlags.contains)
    })
  }

  // Return true if the Campaign should be ingested, based on the given CampaignStatus.
  def isActiveForIngestion(campaignStatus: CampaignStatus.Type): Boolean =
    campaignStatus == CampaignStatus.active || campaignStatus == CampaignStatus.pending

  // Return true if the Campaign with the given CampaignKey should be ingested, based on cached CampaignStatus.
  def isActiveForIngestion(campaignKey: CampaignKey): Boolean =
    CampaignService.campaignMeta(campaignKey).map(cr => isActiveForIngestion(cr.status)).getOrElse(false)

  private def handleHuffPoLiveArticle(articleKey: ArticleKey, gooseArticle: com.gravity.goose.Article): Unit = {
    val isClean = isHuffPoLiveArticleClean(gooseArticle)

    def addToCampaign(campaignKey: CampaignKey, messageSuffix: String): Unit = {
      if (isActiveForIngestion(campaignKey)) {
        ifTrace(trace("Adding article (id: {0}, url: {1}) to campaign: {2}" + messageSuffix, articleKey.articleId, gooseArticle.canonicalLink, campaignKey))
        CampaignService.addArticleKey(campaignKey, articleKey, Settings2.INTEREST_SERVICE_USER_ID, CampaignArticleSettings.getDefault(isActive = isClean)).valueOr {
          case fails: NonEmptyList[FailureResult] =>
            warn(fails, s"Failed to add HuffPoLive article (id: ${articleKey.articleId}, url: ${gooseArticle.canonicalLink}) to campaign (${campaignKey.toString})$messageSuffix")
        }
      }
    }

    // First add this to the main beacon campaign (and any clones).
    if (HuffPoLiveArticleFetcher.isHuffPoLiveHighlightUrl(gooseArticle.canonicalLink)) {
      addToCampaign(huffPoLiveCampaignKey, "")
      addToCampaign(huffPoLiveCampaign2Key, "")

      // Now look for the comma separated verticals in the gooseArticle.additionalData and add it to matching campaigns by vertical
      for {
        verticalsString <- gooseArticle.additionalData.get("verticals")
        verticals = verticalsString.splitBetter(",")
        vertical <- verticals
        campaignKey <- huffPoLiveVerticalToCampaignKeyMap.get(vertical)
      } {
        addToCampaign(campaignKey, " for vertical: " + vertical)
      }
    } else if (HuffPoLiveArticleFetcher.isHuffPoLiveUrl(gooseArticle.canonicalLink)) {
      addToCampaign(huffPoLiveEjectedCampaignKey, " for EJECTED")
    }
  }
}

