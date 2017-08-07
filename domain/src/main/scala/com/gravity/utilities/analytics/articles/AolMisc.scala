package com.gravity.utilities.analytics.articles

import javax.servlet.http.HttpServletRequest

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.web.http.GravityHttpServletRequest
import com.gravity.valueclasses.ValueClassesForDomain.SitePlacementId



// TODO-FIXGMS-2-REMOVE-AOLCOM-HARDCODED-SPIDS: This is used by AolDlStaticApiWidget.usesNarrowBandImage;
// it certainly could be replaced by an AlgoSetting if we want to generalize our support for dial-in users.  :)
object HardcodedAolNarrowBandPlacementId {
  val aolDlNarrowbandSitePlacementId: SitePlacementId = SitePlacementId(3525L)
}

// TODO-FIXGMS-2-REMOVE-AOLCOM-HARDCODED-SPIDS: These are used in 4 scripts (which see).
object HardcodedAolGmsPlacementsIds {
  val aolDlMainPlacementId: SitePlacementId = SitePlacementId(1967L)
  val aolDlMainBonanzaPlacementId: SitePlacementId = SitePlacementId(5690L)
  val aolDlBroadbandSitePlacementId: SitePlacementId = SitePlacementId(3526L)
  val sportsChannelDlSitePlacementId: SitePlacementId = SitePlacementId(3477L)
  val lifestyleChannelDlSitePlacementId: SitePlacementId = SitePlacementId(3479L)
  val financeChannelDlSitePlacementId: SitePlacementId = SitePlacementId(3478L)
  val entertainmentChannelDlSitePlacementId: SitePlacementId = SitePlacementId(3476L)
  val newsChannelDlSitePlacementId: SitePlacementId = SitePlacementId(3600L)
  val entertainmentChannelHnavSpId: SitePlacementId = SitePlacementId(3515L)
  val newsChannelHnavSpId: SitePlacementId = SitePlacementId(3513L)
  val lifestyleChannelHnavSpId: SitePlacementId = SitePlacementId(3516L)
  val sportsChannelHnavSpId: SitePlacementId = SitePlacementId(3514L)
  val financeChannelHnavSpId: SitePlacementId = SitePlacementId(3517L)
}

object AolMisc {
  val plidMax: Int = 2000000000

  val aolSiteGuid: String = ArticleWhitelist.siteGuid(_.AOL)
  val aolSiteKey: SiteKey = SiteKey(aolSiteGuid)
  val aolMailSiteGuid: String = ArticleWhitelist.siteGuid(_.AOL_MAIL)
  val aolMailSiteKey: SiteKey = SiteKey(aolMailSiteGuid)
  val dynamicLeadDLUGCampaignKey: CampaignKey = CampaignKey(aolSiteGuid, 7875655411765477568L)
  val dlugOneScopeKey: (Option[ScopedKey], String) = ArtGrvMap.toOneScopeKey(dynamicLeadDLUGCampaignKey, emptyString)

  val dlFeedCampaignKey: CampaignKey = CampaignKey(AolMisc.aolSiteGuid, 453593677230007996L)
  val promotionOneScopeKey: (Option[ScopedKey], String) = ArtGrvMap.toOneScopeKey(dlFeedCampaignKey, emptyString)


//  val usesSameContentAsDlMain: Set[SitePlacementId] = Set(
//    aolDlNarrowbandSitePlacementId,
//    aolDlBroadbandSitePlacementId,
//    aolDlMainBonanzaPlacementId
//  )

  private val aolMailSandboxUrl = "http://alpo-todaypage.mail.aol.com/"
  private val aolMailSandboxSecureUrl = "https://alpo-todaypage.mail.aol.com/"
  private val adsWrapperJsNonSecureUrl = "http://o.aolcdn.com/ads/adsWrapper.js"
  private val adsWrapperJsSecureUrl = "https://s.aolcdn.com/ads/adsWrapper.js"
  private val localhostUrl = "http://localhost:8080/"
  private val designDemoExternalUrl = "http://69.194.34.115/"

  def getAolPlId(articleKey: ArticleKey): Int = getAolPlId(articleKey.articleId)

  def getAolPlId(articleId: Long): Int = (articleId % plidMax).toInt

//  val placementIdsPoweredByDlug: Set[SitePlacementId] = {
//    Set(
//      HardcodedAolNarrowBandPlacementId.aolDlNarrowbandSitePlacementId,
//      HardcodedAolGmsPlacementsIds.aolDlMainPlacementId,
//      HardcodedAolGmsPlacementsIds.aolDlMainBonanzaPlacementId,
//      HardcodedAolGmsPlacementsIds.aolDlBroadbandSitePlacementId,
//      HardcodedAolGmsPlacementsIds.sportsChannelDlSitePlacementId,
//      HardcodedAolGmsPlacementsIds.lifestyleChannelDlSitePlacementId,
//      HardcodedAolGmsPlacementsIds.financeChannelDlSitePlacementId,
//      HardcodedAolGmsPlacementsIds.entertainmentChannelDlSitePlacementId,
//      HardcodedAolGmsPlacementsIds.newsChannelDlSitePlacementId,
//      HardcodedAolGmsPlacementsIds.entertainmentChannelHnavSpId,
//      HardcodedAolGmsPlacementsIds.newsChannelHnavSpId,
//      HardcodedAolGmsPlacementsIds.lifestyleChannelHnavSpId,
//      HardcodedAolGmsPlacementsIds.sportsChannelHnavSpId,
//      HardcodedAolGmsPlacementsIds.financeChannelHnavSpId
//    )
//  }

  def adsWrapperJsUrl(forceSecureUrl: Boolean = false)(implicit req: HttpServletRequest): String =
    if(forceSecureUrl || req.isSecureWithHostnameCheck) adsWrapperJsSecureUrl else adsWrapperJsNonSecureUrl

  def isAolMailSandbox(url: String): Boolean = url.startsWith(aolMailSandboxUrl) || url.startsWith(aolMailSandboxSecureUrl) ||
    url.startsWith(localhostUrl) || url.startsWith(designDemoExternalUrl)

  val aolNetworkDomainSuffixes: Set[String] = Set(
    "aol.com",
    "mapquest.com",
    "atwola.com",
    "ru4.com",
    "adsonar.com",
    "aol.it",
    "aol.co.uk",
    "aol.ca",
    "aim.com",
    "huffingtonpost.co.uk",
    "huffingtonpost.ca",
    "games.com",
    "dailyfinance.com",
    "stylelist.com",
    "patch.com",
    "aoltv.com",
    "engadget.com",
    "autoblog.com",
    "noisecreep.com",
    "theboot.com",
    "spinner.com",
    "mydaily.com",
    "mydaily.co.uk",
    "cambio.com",
    "moviefone.com",
    "mandatory.com",
    "pawnation.com",
    "theboombox.com",
    "techcrunch.com",
    "gadling.com",
    "adtech.de",
    "makers.com",
    "247wallst.com",
    "aolcdn.com",
    "aolradio.com",
    "aolartists.com",
    "parentdish.co.uk",
    "walletpop.ca",
    "aolradioblog.com",
    "aolheroes.com",
    "shortcuts.com",
    "joystiq.com",
    "tuaw.com",
    "homesessive.com",
    "kitchendaily.com",
    "purpleclover.com",
    "huffingtonpost.com",
    "huffpost.com",
    "wow.com",
    "stylemepretty.com",
    "tested.com",
    "crunchbase.com",
    "netscape.com",
    "compuserve.com",
    "aolsearch.com",
    "moviefone.ca",
    "altomail.com",
    "luxist.com",
    "mapquest.co.uk",
    "mapquest.ca",
    "stylelist.ca",
    "parentdish.ca",
    "gathr.com",
    "tourtracker.com",
    "gdgt.com"
  )

  val aolNetworkProtocols: Set[String] = Set(
    "aol"
  )

  val aolNetworkDomains: Set[String] = Set(
    "huff.to",
    "aol.sportingnews.com",
    "webmail.cs.com",
    "aolradio.slacker.com",
    "aol.careerbuilder.com",
    "aol.king.com"
  )
}
