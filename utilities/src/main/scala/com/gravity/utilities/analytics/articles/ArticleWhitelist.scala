package com.gravity.utilities.analytics.articles

import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.ScalaMagic
import org.apache.commons.lang.time.StopWatch
import org.apache.commons.lang3.StringUtils

import scala.util.matching.Regex

/**
 * Created by Jim Plush
 * User: jim
 * Date: 5/9/11
 * This object will enable the crawlers to only crawl urls that we think are actually articles. Since there is currently
 * no way to verify that we're only crawling valid articles and not section or search pages we need to write whitelists
 * for the larger partners. The long tail will have the plugin available that will push us publishes so we'd be ok there.
 */


object ArticleWhitelist {
 import com.gravity.logging.Logging._

  trait Partners {
    val BUZZNET = "buzznet"
    val SCRIBD = "scribd"
    val DOGSTER = "dogster"
    val CATSTER = "catster"
    val XOJANE = "xojane"
    val WSJ = "wsj"
    val YAHOO_NEWS = "yahoo news"
    val TIME = "time"
    val CNN_MONEY = "cnn money"
    val TECHCRUNCH = "techcrunch"
    val WESH = "wesh"
    val BOSTON = "boston"
    val VOICES = "voices"
    val SFGATE = "sfgate"
    val SFGATE_OLD = "sfgate-old-guid"
    //We changed the guid schema for Meebo
    val MENSFITNESS = "mensfitness"
    val MENSFITNESS_OLD = "mensfitness-old-guid"
    //We changed the guid scheme for Meebo
    val DYING_SCENE = "dyingscene"
    val METAL_RIOT = "metalriot"
    val SUITE_101 = "suite101"
    val WALYOU = "walyou"
    val YAHOO_OLYMPICS = "yahoo olympics"
    val UNITTEST = "cats"
    val FB_GRAPH_ROLLUP = "fb graph rollup"
    //Site for aggregating facebook api calls
    val HIGHLIGHTER_PLUGIN = "clickstream plugin"
    //Site for pulling in clickstream for highlighter
    val WORDPRESS = "wordpress"
    val GILTCITY = "giltcity"
    val CAFEMOM = "cafemom"
    val OPENX = "openx"
    val OPENX_CAFEMOM = "openx cafemom"
    val OPENX_CBS_SPORTS = "openx cbs spots"
    val CBS_SPORTS = "cbssports"
    val PROBOARDS = "proboards"
    val SPORTING_NEWS = "sportingnews"
    val SPORTING_NEWS_PARTNERS = "sportingnews partners"
    val WETPAINT = "wetpaint"
    val OPENX_RTB2 = "openx rtb"
    val DAILYMOTION = "dailymotion"
    val OPENX_VIDEO = "openx video"
    val OPENX_TEXT = "openx text"
    val WORDPRESS_EOP = "wordpress eop"
    val SPORTSILLUSTRATED = "sports illustrated"
    val BUZZFEED = "buzzfeed"
    val WEBMD = "webmd"
    val COSMOPOLITAN = "cosmopolitan"
    val GADGETREVIEW = "gadgetreview"
    val ESPN = "espn"
    val BIGLEADSPORTS = "big lead sports"
    val IVILLAGE = "ivillage"
    val CONDUIT = "conduit"
    val CROSSREADER = "cross reader"
    val GOGOMIX = "gogomix"
    val HEALTH_CENTRAL = "Health Central"
    val DRUGS = "drugs"
    val MSN = "msn"
    val ENGADGET = "engadget"
    val STYLELIST = "styleList"
    val DAILY_FINANCE = "dailyfinance"
    val HUFFINGTON_POST = "huffingtonpost"
    val HUFF_PO_DO_NOT_USE = "huffpo"
    val AOL = "aol"
    val THE_GUARDIAN = "theguardian"
    val AOL_MAIL = "aol mail"
    val AOL_ON = "aol on"
    val AOL_JOBS = "aol jobs"
    val AOL_REAL_ESTATE = "aol real estate"
    val AOL_AUTOS = "aol autos"
    val AOL_ENTERTAINMENT = "aol entertainment"
    val AOL_CA = "aol.ca"
    val AOL_DE = "aol.de"
    val AOL_FR = "aol.fr"
    val AOL_CO_UK = "aol.co.uk"

    val AUTOBLOG = "autoblog"
    val CAMBIO = "cambio"
    val KITCHEN_DAILY = "kitchen daily"
    val MAKERS = "makers"
    val JOYSTIQ = "joystiq"
    val TUAW = "tuaw"
    val FOREX_TEST_CAMPAIGN = "forex test campaign"
    val AOL_APPS = "aol apps"
    val ENGADGET_APPS = "engadget apps"
    val BILLBOARD = "billboard"
    val LOCAL_SYR = "localsyr"
    val WHERE_TRAVELLER = "wheretraveller"
    val HUFFPO_INTL = "huffpo intl"
    val THOUGHTLEADR = "thoughtleadr"
    val WASHINGTON_POST = "washington post"
    val FORBES = "forbes"
    val GRAVITY = "gravity"
    val NAPA_VALLEY_REGISTER = "napa valley register"
    val LEE_DOT_NET = "lee.net"
    val THR = "the hollywood reporter"
    val ADWEEK = "ad week"
    val CNN = "CNN"
    val DISQUS = "disqus"
    val DISQUS_PREMIUM = "disqus premium"
    val ADVERTISINGDOTCOM = "advertising.com"
    val IMEDIA = "iMedia"
    val TL_MEDIA = "tlMedia"
    val THE_ORBIT = "TheOrbit"
    val TMZ = "TMZ"
    val MOVIE_PHONE ="MovieFone.com (AOL)"
    val HUFF_PO_UK = "HuffingtonPost.co.uk (AOL UK)"
    val DISCOVERY_TV_NETWORKS = "DISCOVERY_TV_NETWORKS"
    val WOW = "Wow.com"
    val TECHCRUNCH_JP = "TechCrunch Japan (AOL)"

    val partnerToSiteGuid: Map[String, String] = Map(
      BUZZNET -> "108c039d3e9ff7d2f2bfe97389d4b4e8",
      SCRIBD -> "6e1ea1b081dc6743bbe3537728eca43d",
      DOGSTER -> "1c26a68fa3ab111b2b9fd39a832f459c",
      CATSTER -> "415b0927a7149da3171f4b88d612c656",
      XOJANE -> "2e71af6656d743de812a1ccef55f5db9",
      WSJ -> "18f3b45caef80572d421f68a979bd6dc",
      YAHOO_NEWS -> "494d6e560c95708877ba0eff211c02f3",
      TIME -> "e8fd47ffdc2b5a28d10f29d4dfdf8fb9",
      CNN_MONEY -> "b8ebc160de219b92cdb6904014ace282",
      TECHCRUNCH -> "fca4fa8af1286d8a77f26033fdeed202",
      WESH -> "0ee8e68089d819f85148fabe9de6f086",
      BOSTON -> "fe6d5479cc3242c2b7ef253225b4bfcc",
      VOICES -> "aa08660c4e79c71c48ea63f792a91327",
      SFGATE -> "meebo:hearstnewspapers:sfgate",
      SFGATE_OLD -> "772494943d17d6e3ad4833572e0f8dac",
      MENSFITNESS -> "meebo:americanmedia:mensfitness",
      MENSFITNESS_OLD -> "4d3c329706f6257c8a061f576635c28b",
      DYING_SCENE -> "706f25ce0f206dbd235e76a0edcd7c6c",
      SUITE_101 -> "12dd5078e59272a0e8e578abc5b8be96",
      WALYOU -> "b067b281228cbb7c93a2647ef4baf33d",
      UNITTEST -> "CATS11b081dc6743bbe3537728eca43d",
      FB_GRAPH_ROLLUP -> "ROLL5dc1b4c320b9a7f03fcfe12536ab",
      HIGHLIGHTER_PLUGIN -> "SPEC6ef2c5a210b9a7f03fcfe12537cd",
      METAL_RIOT -> "6435192c4c33fe2ec8322998f81fa055",
      YAHOO_OLYMPICS -> "46dba8c3d3fbe461beee920616e62229",
      WORDPRESS -> "268fc15296103bd0d639409cab651e35",
      GILTCITY -> "ff5078b06505373ab76399f5a77eb928",
      CAFEMOM -> "7ac5dc74baf3038e699794863bce4bf0",
      OPENX -> "82424a738c2b5fc6f1c39a66eb3ae581",
      OPENX_CAFEMOM -> "02d6f39f6a26ec56e4cafd446dbe7735",
      OPENX_CBS_SPORTS -> "f2e8025bb6df1136feb62dec4bdd89de",
      CBS_SPORTS -> "9c92d95fcf3da8cfcf3c009f2b3adcd5",
      PROBOARDS -> "a1f9015c15922698596d7c5bdd1561c2",
      SPORTING_NEWS -> "8e0359415bbe0d02961ea6c2c24fedd6",
      SPORTING_NEWS_PARTNERS -> "46ecb09adc92a29bc5142a779997297b",
      WETPAINT -> "934e04f62a151d57b4d8b3817a88c166",
      OPENX_RTB2 -> "360a519c5dc946f7447ad5384cf63dc2",
      DAILYMOTION -> "864aadfb058ce19d234444f495fe611b",
      OPENX_VIDEO -> "a161eda9db72ba318ff6d1286b057d95",
      OPENX_TEXT -> "edfe79075f0b6b5bbf897b98593ab6c6",
      WORDPRESS_EOP -> "361837bc83c4c2cba7b350dff56a564f",
      SPORTSILLUSTRATED -> "219c171532b2ee79090f757bd7f69c90",
      BUZZFEED -> "23651cdb658ea0d4203178a157359bf2",
      WEBMD -> "c859141b2d89f4eff4ddf2a00eaa9615",
      COSMOPOLITAN -> "9ca8bd5f86bd96d19fa084512355efb1",
      GADGETREVIEW -> "a3a7be6fd4e8ef5b8b2e7ecb37e662ca",
      ESPN -> "9a41401e9b7c945344e001ee7f23031e",
      BIGLEADSPORTS -> "0c4f37a8f0fe180111bf8f2ca77e8680",
      CONDUIT -> "126d2f36eea861994eb57d1af6af5020",
      IVILLAGE -> "c66b7f2bb5e03672f964892f63957fb0",
      CROSSREADER -> "3b9634247462cb906d4d8d5faec75ec5",
      GOGOMIX -> "2c3bfda1e48368bac56b97667954342c",
      HEALTH_CENTRAL -> "e3d0ccbfc82b096a4dfe83bc269b64be",
      DRUGS -> "51494d2d8f657182dda095e0affc4792",
      MSN -> "14f1a20a34a9356b91dd8b860925478c",
      ENGADGET -> "015756f28e553328b5d358aa09764558",
      STYLELIST -> "c36faf798a517e544c981bf938b3951f",
      DAILY_FINANCE -> "289b1a904ef4e22e9667c441d933cda0",
      HUFFINGTON_POST -> "95ec266b244de718b80c652a08af06fa",
      HUFF_PO_DO_NOT_USE -> "cf545885573c3357caa22029d7471f3e", // Redirects to HUFFINGTON_POST, but here for good measure
      AOL -> "ea5ddf3148ee6dad41c76e96cb00e1a9",
      AOL_MAIL -> "ddf0cce778b5d930cab2eb5d8028098c",
      AOL_ON -> "f65128600d3e0725c20254455e803b8b",
      AOL_JOBS -> "495a110279df4221edbb256aec96d494",
      AOL_REAL_ESTATE -> "0e740ce594c2b3313c41fd4b590526d0",
      AOL_AUTOS -> "c91672cd0d51c90f788e34b583f465d4",
      AOL_ENTERTAINMENT -> "8400b9d01e8e61cf6a5e932c23ca3499",
      AOL_CA -> "7a7a0a22d92092fb4f2fc5d7ff1781b4",
      AOL_DE -> "7de67666a52c9f9bdb0e8c9c42af945d",
      AOL_FR -> "544a0bc4138e0ef88e7cf6e826140046",
      AOL_CO_UK -> "e6a7a37855562006fe84697faecec443",
      AUTOBLOG -> "3cafef6339dbfb9fffdbd6dbc2b4cc69",
      CAMBIO -> "3ad74fb424f36757d5441e9d62738514",
      KITCHEN_DAILY -> "4c5e5db39552904c94eaab6eef4d6eec",
      MAKERS -> "35b6417a0cbd84704f9ee47e0feec59f",
      THE_GUARDIAN -> "14b492cf6727dd1ab3a6efc7556b91bc",
      JOYSTIQ -> "5c26a2114d4f60aa21d23f7d57cb9d7a",
      TUAW -> "3cc14347831153f118a543c3b9412747",
      FOREX_TEST_CAMPAIGN -> "8400b9d01e8e61cf6a5e932c23ca3499",
      BILLBOARD -> "339db668c63acf75d5bcc8be7728b693",
      WASHINGTON_POST -> "251a5b8e140a4a07def415e616d000a0",
      FORBES -> "df51572e4a248537619f3b6b11928388",
      GRAVITY -> "bc210cd6294b0d20913c259869298190",
      THE_ORBIT -> "e9fce3f7001d9d32fe584d8b6b309439"

    /* Apps, which should be added to Beaconing.sitesToIgnoreDomainCheckFor */
      ,AOL_APPS -> "a30157c3ce52c2bf689879fbec66dd71"
      ,ENGADGET_APPS -> "69172acf9ee255c97b5b931120fc06c3"
      ,LOCAL_SYR -> "4b23748da228c012bab84f963863284d"
      ,WHERE_TRAVELLER -> "c6669b63be75ac6cf74bf77888447844"
      ,HUFFPO_INTL -> "72ef14275a4db480ac8010c74ede260c"
      ,THOUGHTLEADR -> "5e97b390a1f70594258dbd78e1f5766c"
      ,NAPA_VALLEY_REGISTER -> "ffc6f551c374e9e26baa20a15cbe6838"
      ,LEE_DOT_NET -> "8e869b3a021a8bf8e030fa6bc6ebd3d6"
      ,THR -> "01b0b9ad501e0c849a822e6124ff0fc2"
      ,ADWEEK -> "afdf2ccd6cc93e74261d26cd813caf22"
      ,CNN -> "93f8c51d6c0425946a44c5302fb62bff"
      ,DISQUS -> "aaf35e5f84a07252a9830cff120252a7"
      ,DISQUS_PREMIUM -> "300cdf96cba8e096db823041908518df"
      ,ADVERTISINGDOTCOM -> "e587a764f73a92f7c5f268beb2db8438"
      ,IMEDIA -> "1d76c447fb66a21aaa429ff0948d2958"
      ,TL_MEDIA -> "20ced15386744c32e602f2a3c73ecd22"
      ,TMZ -> "82893b79564009a4d8fab7b9db32cfea"
      ,MOVIE_PHONE -> "6f188e340e03250e80e2e5e06948c85c"
      ,HUFF_PO_UK -> "d32f28bf8764c69c6310d370fe17788f"
      ,DISCOVERY_TV_NETWORKS -> "83110be68df66197e67a0d285945d18d"
      ,WOW -> "f9832b49f3c8dcfb187982a1835e161c"
    ,TECHCRUNCH_JP -> "9a898963423328b8a9fd1b98fdb0a9b4"
    )

    val siteGuidToPartner: Map[String, String] = partnerToSiteGuid.map(kv => kv._2 -> kv._1)

    val aolPartners: Set[String] = Set(
      partnerToSiteGuid(ENGADGET),
      partnerToSiteGuid(HUFFINGTON_POST),
      partnerToSiteGuid(AOL),
      partnerToSiteGuid(AOL_MAIL),
      partnerToSiteGuid(AOL_ON),
      partnerToSiteGuid(AOL_JOBS),
      partnerToSiteGuid(AOL_REAL_ESTATE),
      partnerToSiteGuid(AOL_AUTOS),
      partnerToSiteGuid(AOL_ENTERTAINMENT),
      partnerToSiteGuid(AUTOBLOG),
      partnerToSiteGuid(CAMBIO),
      partnerToSiteGuid(KITCHEN_DAILY),
      partnerToSiteGuid(MAKERS),
      partnerToSiteGuid(DAILY_FINANCE),
      partnerToSiteGuid(TECHCRUNCH),
      partnerToSiteGuid(JOYSTIQ),
      partnerToSiteGuid(TUAW),
      partnerToSiteGuid(STYLELIST),
      partnerToSiteGuid(AOL_CA),
      partnerToSiteGuid(AOL_DE),
      partnerToSiteGuid(AOL_FR),
      partnerToSiteGuid(AOL_CO_UK)
    )
    def isAolPartner(siteGuid: String): Boolean = aolPartners.contains(siteGuid)

    val topicModelPartners: Set[String] = Set[String](
      //"d32f28bf8764c69c6310d370fe17788f", // HuffingtonPost.co.uk (AOL UK)
      "3ad74fb424f36757d5441e9d62738514", // AOL - Cambio
      "d6ef948a1cde62eaf90e545b3869a1cc", // StyleMePretty.com (AOL)
      //"0e740ce594c2b3313c41fd4b590526d0", // RealEstate.Aol.com
      //"bc210cd6294b0d20913c259869298190", // Gravity.com
      //"7a7a0a22d92092fb4f2fc5d7ff1781b4", // AOL.ca (Canada)
      "9a898963423328b8a9fd1b98fdb0a9b4", // TechCrunch Japan (AOL)
      "82893b79564009a4d8fab7b9db32cfea", // TMZ.com
      "fca4fa8af1286d8a77f26033fdeed202", // TechCrunch.com (AOL)
      //"d1e9e149003eb0d3e18f27474a30276c", // AOL UK Mail
      "e6a7a37855562006fe84697faecec443", // AOL UK
      //"289b1a904ef4e22e9667c441d933cda0", // Daily Finance (AOL)
      "f9832b49f3c8dcfb187982a1835e161c", // Wow.com
      //"d6fe5c098b37a902c53c1a6d0b9fb042", // Strength in Numbers Blog
      //"544a0bc4138e0ef88e7cf6e826140046", // AOL.fr (France)
      //"a626130d3c648490246aa8b496edf8bf", // AMP (test)
      "3cafef6339dbfb9fffdbd6dbc2b4cc69", // AutoBlog.com (AOL)
      //"ddf0cce778b5d930cab2eb5d8028098c", // AOL Mail
      "72ef14275a4db480ac8010c74ede260c", // Huffington Post International
      //"69172acf9ee255c97b5b931120fc06c3", // Engadget Apps (AOL)
      "ea5ddf3148ee6dad41c76e96cb00e1a9", // AOL.com
      "6f188e340e03250e80e2e5e06948c85c", // MovieFone.com (AOL)
      "95ec266b244de718b80c652a08af06fa", // HuffingtonPost.com (AOL)
      //"495a110279df4221edbb256aec96d494", // AOL Jobs
      //"b69761d98b48305125b83486d1f88a6a", // chatfuel.com
      //"6577dfecd881bfba99cf1d4a63e3452e", // Gravity SDK (test)
      "7de67666a52c9f9bdb0e8c9c42af945d", // AOL.de (Germany)
      "f65128600d3e0725c20254455e803b8b", // AOL On
      //"1e900333e59d82c94ca1a3a3f1408488", // hindog.com
      //"adbadb9d8559c5d217ea405c1e8c1695", // User Feedback Ad Labs (test)
      "ad7e219efd4e8322c5d100c8991a6add", // TechCrunch China (AOL)
      "83110be68df66197e67a0d285945d18d"  // Discovery TV Networks
    )

    // put sites here then workflow deploy, once generated promote them to above collection topicModelPartners
    val topicModelPartnerStaging: Set[String] = Set[String](
      "d32f28bf8764c69c6310d370fe17788f", // HuffingtonPost.co.uk (AOL UK)
      "3ad74fb424f36757d5441e9d62738514", // AOL - Cambio
      "d6ef948a1cde62eaf90e545b3869a1cc", // StyleMePretty.com (AOL)
      "0e740ce594c2b3313c41fd4b590526d0", // RealEstate.Aol.com
      "bc210cd6294b0d20913c259869298190", // Gravity.com
      "7a7a0a22d92092fb4f2fc5d7ff1781b4", // AOL.ca (Canada)
      "9a898963423328b8a9fd1b98fdb0a9b4", // TechCrunch Japan (AOL)
      "82893b79564009a4d8fab7b9db32cfea", // TMZ.com
      "fca4fa8af1286d8a77f26033fdeed202", // TechCrunch.com (AOL)
      "d1e9e149003eb0d3e18f27474a30276c", // AOL UK Mail
      "e6a7a37855562006fe84697faecec443", // AOL UK
      "289b1a904ef4e22e9667c441d933cda0", // Daily Finance (AOL)
      "f9832b49f3c8dcfb187982a1835e161c", // Wow.com
      "d6fe5c098b37a902c53c1a6d0b9fb042", // Strength in Numbers Blog
      "544a0bc4138e0ef88e7cf6e826140046", // AOL.fr (France)
      "a626130d3c648490246aa8b496edf8bf", // AMP (test)
      "3cafef6339dbfb9fffdbd6dbc2b4cc69", // AutoBlog.com (AOL)
      "ddf0cce778b5d930cab2eb5d8028098c", // AOL Mail
      "72ef14275a4db480ac8010c74ede260c", // Huffington Post International
      "69172acf9ee255c97b5b931120fc06c3", // Engadget Apps (AOL)
      "ea5ddf3148ee6dad41c76e96cb00e1a9", // AOL.com
      "6f188e340e03250e80e2e5e06948c85c", // MovieFone.com (AOL)
      "95ec266b244de718b80c652a08af06fa", // HuffingtonPost.com (AOL)
      "495a110279df4221edbb256aec96d494", // AOL Jobs
      "b69761d98b48305125b83486d1f88a6a", // chatfuel.com
      "6577dfecd881bfba99cf1d4a63e3452e", // Gravity SDK (test)
      "7de67666a52c9f9bdb0e8c9c42af945d", // AOL.de (Germany)
      "f65128600d3e0725c20254455e803b8b", // AOL On
      "1e900333e59d82c94ca1a3a3f1408488", // hindog.com
      "adbadb9d8559c5d217ea405c1e8c1695", // User Feedback Ad Labs (test)
      "ad7e219efd4e8322c5d100c8991a6add", // TechCrunch China (AOL)
      "83110be68df66197e67a0d285945d18d", // Discovery TV Networks
      "f41394ede26e0ba798159768b1758d7d"  // Excelsior.com.mx (HuffPost MX Partner)
    )

    def isTopicModelPartner(siteGuid:String): Boolean = {
      topicModelPartners.contains(siteGuid)
    }
  }

  object Partners extends Partners

  def siteGuid(p: (Partners) => String): String = Partners.partnerToSiteGuid(p(Partners))

  import com.gravity.utilities.analytics.articles.ArticleWhitelist.Partners._

  def getSiteGuidByPartnerArticleUrl(url: String): Option[String] = getPartnerName(url) match {
    case Some(pn) => partnerToSiteGuid.get(pn)
    case None => None
  }

  def getPartnerNameBySiteguid(siteguid: String): Option[String] = partnerToSiteGuid find {
    case (theirName, theirSiteguid) => {
      siteguid == theirSiteguid
    }
  } map (_._1)

  def hasWhitelistAvailable(url: String): Boolean = getPartnerName(url) match {
    case Some(_) => true
    case None => false
  }

  def hasWhitelistBySiteGuid(siteGuid: String): Boolean = siteGuidToPartner.get(siteGuid).isDefined

  /**
   * Check to see if the URL is an article on a site that may have a whitelist.
   * @param url the URL to check
   * @return
   */
  def isArticleOnWhitelistSite(url: String): Boolean = {
    getPartnerName(url) match {
      case Some(partner) => true
      case None => false
    }
  }

  /**
   * Check to see if a particular URL should be crawled.
   * @param url the URL to check
   * @param filterType allows you to add multiple regexes for other purposes like key page analytics which needs photos included
   *                   in the url patterns
   * @return true if the URL meets our business rules, otherwise false
   */
  def urlCanBeCrawled(url: String, siteGuidOverride: Option[String] = None): Boolean = {
    val prelimChecks = siteGuidOverride match {
      case Some(forSiteGuid) => {
        if (!hasWhitelistBySiteGuid(forSiteGuid)) {
          !URLUtils.urlAppearsToBeHomePage(url)
        } else {
          isValidContentArticleBySiteGuid(url, forSiteGuid)
        }
      }
      case None => {
        if (!ArticleWhitelist.isArticleOnWhitelistSite(url)) {
          //Article is NOT on a site that has a whitelist registered.  So let's do the best we can.
          val appearsToBeHomePage = URLUtils.urlAppearsToBeHomePage(url)
          !appearsToBeHomePage
        } else {
          ArticleWhitelist.isValidPartnerArticle(url)
        }
      }
    }

    // if our preliminary check(s) failed, no use trying any further. fail now
    if (!prelimChecks) return false

    // even if we made it through the preliminary checks, we cannot crawl an invalid URL
    url.tryToURL.isDefined
  }

  def isValidContentArticleBySiteGuid(url: String, sourceSiteGuid: String): Boolean = isValidContentArticleByPartner(url, siteGuidToPartner.get(sourceSiteGuid))

  def isValidContentArticleByPartner(url: String, partnerOption: Option[String]): Boolean = {
    if (ScalaMagic.isNullOrEmpty(url)) return false

    for {
      partner <- partnerOption
      isArticle <- getUrlCheckByPartner(partner)
    } {
      return isArticle(url)
    }

    false
  }

  def isTimeArticle(url: String): Boolean = isTimeWordpressArticle(url) || isTimeOtherArticle(url)

  def alwaysTrue(url: String): Boolean = true

  def isWhitelistCheckAvailableForSite(siteGuid: String): Boolean = getUrlCheckBySiteGuid(siteGuid) match {
    case None => false
    case Some(_) => true
  }

  def getUrlCheckBySiteGuid(siteGuid: String): Option[(String) => Boolean] = siteGuidToPartner.get(siteGuid).flatMap(partner => getUrlCheckByPartner(partner))

  def getUrlCheckByPartner(partner: String): Option[(String) => Boolean] = partner match {
    case BUZZNET => Some(isBuzznetArticle)
    case SCRIBD => Some(isScribdArticle)
    case DOGSTER => Some(isDogsterArticle)
    case CATSTER => Some(isCatsterArticle)
    case XOJANE => Some(isXoJaneArticle)
    case WSJ => Some(isWsjArticle)
    case YAHOO_NEWS => Some(isYahooNewsArticle)
    case TIME => Some(isTimeArticle)
    case CNN_MONEY => Some(isCnnMoneyArticle)
    case TECHCRUNCH => Some(isTechCrunchArticle)
    case WESH => Some(isWeshArticle)
    case BOSTON => Some(isBostonArticle)
    case VOICES => Some(isVoicesArticle)
    case SFGATE => Some(isSFGateArticle)
    case MENSFITNESS => Some(isMensFitnessArticle)
    case DYING_SCENE => Some(isDyingSceneArticle)
    case SUITE_101 => Some(isSuite101Article)
    case WALYOU => Some(isWalyouArticle)
    case YAHOO_OLYMPICS => Some(isForYahooOlympics(_, mustBeArticle = true))
    case UNITTEST => Some(isUnitTestArticle)
    case SPORTING_NEWS => Some(isSportingNewsArticle)
    case WETPAINT => Some(isWetPaintArticle)
    case DAILYMOTION => Some(isDailyMotionArticle)
    case WEBMD => Some(isWebMdArticle)
    case GADGETREVIEW => Some(isGadgetReviewArticle)
    case HIGHLIGHTER_PLUGIN => Some(alwaysTrue)
    case ESPN => Some(isEspnArticle)
    case HEALTH_CENTRAL => Some(isHealthCentralArticle)
    case DRUGS => Some(isDrugsArticle)
    case MSN => Some(isMsnArticle)
    case THE_GUARDIAN => Some(isTheGuardianArticle)
    case _ => None
  }


  /**
   * if this is an actual article url from one of our non-plugin based bros then test if we should crawl it potentially
   * @param url the URL to check
   */
  def isValidPartnerArticle(url: String): Boolean = isValidContentArticleByPartner(url, getPartnerName(url))

  def getRegexPatternsForSiteGuid(siteGuid: String): Seq[String] = Partners.siteGuidToPartner.get(siteGuid) match {
    case Some(pname) => getRegexPatternsForPartner(pname)
    case None => Seq.empty[String]
  }

  def getRegexPatternsForPartner(partnerName: String): Seq[String] = partnerName match {
    case BUZZNET => Seq(regexBuzznet.pattern.pattern())
    case SCRIBD => Seq(regexScribd.pattern.pattern())
    case DOGSTER => Seq(regexDogster.pattern.pattern())
    case CATSTER => Seq(regexCatster.pattern.pattern())
    case XOJANE => Seq(regexXojane.pattern.pattern())
    case CNN_MONEY => Seq(regexCnnMoney.pattern.pattern())
    case TECHCRUNCH => Seq(regexTechCrunch.pattern.pattern())
    case WSJ => Seq(wsjArticleRegex.pattern.pattern(), wsjBlogRegex.pattern.pattern())
    case TIME => Seq(regexTimeWP.pattern.pattern(), regexTimeOther.pattern.pattern())
    case YAHOO_NEWS => Seq(regexYahooNews.pattern.pattern())
    case WESH => Seq(regexWesh.pattern.pattern())
    case BOSTON => Seq(regexBoston.pattern.pattern())
    case VOICES => Seq(regexVoices.pattern.pattern())
    case SFGATE => Seq(regexSFGate.pattern.pattern())
    case MENSFITNESS => Seq(regexMensFitness.pattern.pattern())
    case DYING_SCENE => Seq(regexDyingScene.pattern.pattern())
    case SUITE_101 => Seq(regexSuite101.pattern.pattern())
    case WALYOU => Seq(regexWalyou.pattern.pattern())
    case SPORTING_NEWS => Seq(regexSportingNews.pattern.pattern())
    case HUFFINGTON_POST => Seq(regexHuffingtonPostArticle.pattern.pattern())
    case DAILY_FINANCE => Seq(regexDailyFinanceArticle.pattern.pattern())
    case ENGADGET => Seq(regexEngadgetArticle.pattern.pattern())
    case STYLELIST => Seq(regexStylelistView.pattern.pattern())
    case THE_GUARDIAN => Seq(regexTheGuardian.pattern.pattern())
    case AOL => Seq()
    case _ => Seq.empty[String]
  }

  val regexTheGuardian: Regex = """^http://www\.theguardian\.com/[a-zA-Z0-9_-]+/\d{4}/.*$""".r
  val regexTheGuardianBlogs: Regex = """^http://www\.theguardian\.com/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+/\d{4}/.*$""".r

  def isTheGuardianArticle(url: String): Boolean = {
    // must match this: http://www.theguardian.com/*/<4 digit year>/*

    if (!url.startsWith("http://www.theguardian.com/")) return false

    isRegexMatchedArticle(url, regexTheGuardian) || isRegexMatchedArticle(url, regexTheGuardianBlogs)
  }

  def isHealthCentralArticle(url: String): Boolean = {
    // must match this: http://www.healthcentral.com/*/cf/slideshows/*

    if (!url.startsWith("http://www.healthcentral.com/")) return false

    url.tryToURL match {
      case Some(jurl) if jurl.getPath.contains("/cf/slideshows/") && !jurl.getPath.endsWith("/") => true
      case _ => false
    }
  }

  def isDrugsArticle(url: String): Boolean = {
    // matches similar to this: http://www.drugs.com/*/answers/*

    if (!url.startsWith("http://www.drugs.com/")) return false

    url.tryToURL match {
      case Some(jurl) if jurl.getPath.endsWith("/") => false

      case Some(jurl) if jurl.getPath.contains("/answers/") => true
      case Some(jurl) if jurl.getPath.contains("/article/") => true
      case Some(jurl) if jurl.getPath.contains("/cg/") => true
      case Some(jurl) if jurl.getPath.contains("/condition/") => true
      case Some(jurl) if jurl.getPath.contains("/dosage/") => true
      case Some(jurl) if jurl.getPath.contains("/drug-interactions/") => true
      case Some(jurl) if jurl.getPath.contains("/health-guide/") => true
      case Some(jurl) if jurl.getPath.contains("/mtm/") => true
      case Some(jurl) if jurl.getPath.contains("/news/") => true
      case Some(jurl) if jurl.getPath.contains("/pregnancy/") => true
      case Some(jurl) if jurl.getPath.contains("/price-guide/") => true
      case Some(jurl) if jurl.getPath.contains("/sfx/") => true

      case _ => false
    }
  }

  def isMsnArticle(rawUrlStr: String): Boolean = {
    // matches similar to this: http://www.msn.com/en-us/money/companies/gm-to-pay-dollar420-million-in-signing-bonuses/ar-BBmxUSH

    if (rawUrlStr == null)
      return false

    val urlStr = rawUrlStr.toLowerCase

    // Don't try to ingest from bad URLs.
    urlStr.tryToURL match {
      case None =>
        false

      case Some(url) =>
        // Only ingest from http.
        if (Option(url.getProtocol).exists(_ == "http") == false)
          return false

        // Only ingest from www.msn.com (and not from e.g. zone.msn.com)
        if (Option(url.getHost).exists(_ == "www.msn.com") == false)
          return false

        Option(url.getPath) match {
          case None =>
            false

          case Some(path) =>
            // Has to start with one of the wanted paths.
            val incStarts = List("/en-us/")

            val excStarts = List(
              // Exclude these entire sections.
              "/en-us/travel/",
              "/en-us/weather/",

              // Exclude these search results pages.
              "/en-us/autos/searchresults",
              "/en-us/entertainment/search",
              "/en-us/foodanddrink/searchresults",
              "/en-us/health/search",
              "/en-us/money/searchresults",
              "/en-us/money/stockdetails",
              "/en-us/money/watchlist",
              "/en-us/movies/search",
              "/en-us/music/search",
              "/en-us/news/trending/topicsearch",
              "/en-us/sports/searchresults",
              "/en-us/travel/searchresults",
              "/en-us/tv/search",
              "/en-us/video/searchresults",
              "/en-us/weather/weathersearch"
            )

            val excMatches = Set(
              "/en-us/autos",
              "/en-us/entertainment",
              "/en-us/foodanddrink",
              "/en-us/health",
              "/en-us/lifestyle",
              "/en-us/money",
              "/en-us/movies",
              "/en-us/music",
              "/en-us/news",
              "/en-us/sports",
              "/en-us/travel",
              "/en-us/tv",
              "/en-us/video/_log",
              "/en-us/weather"
            )
            
            if (incStarts.forall(inc => !path.startsWith(inc)))
              false
            else if (path endsWith "/")
              false
            else if (excStarts.exists(path startsWith _))
              false
            else if (excMatches contains path)
              false
            else
              true
        }
    }
  }

  def isWebMdArticle(url: String): Boolean = {
    // first throw out known black list cases
    if (url.startsWith("http://www.webmd.com/hw-popup/")) return false

    url.tryToURL match {
      case Some(jUrl) => {
        nullToEmpty(jUrl.getAuthority).endsWith("webmd.com") && !nullToEmpty(jUrl.getPath).endsWith("/default.htm")
      }
      case None => false
    }
  }

  val knownWordPressBadUrlParts: Seq[String] = Seq(
    "/page/",
    "/tag/",
    "/categories/"
  )

  def isGadgetReviewArticle(url: String): Boolean = {
    if (!url.startsWith("http://www.gadgetreview.com/")) return false

    for (bad <- knownWordPressBadUrlParts; if url.contains(bad)) return false

    url.tryToURL match {
      case None => false
      case Some(jUrl) => {
        val path = nullToEmpty(jUrl.getPath)
        if (path.endsWith(".html")) return true

        val parts = tokenize(path, "/")
        parts.length > 1
      }
    }
  }

  def isDailyMotionArticle(url: String): Boolean = url.startsWith("http://www.dailymotion.com/video")

  def isWetPaintArticle(url: String): Boolean = url.startsWith("http://www.wetpaint.com")

  val regexSportingNews: Regex = """^http://aol\.sportingnews\.com/[a-z-]+/story/\d{4}-\d{2}-\d{2}/[a-z0-9_-]+.*$""".r

  def isSportingNewsArticle(url: String): Boolean = {
    if (!url.startsWith("http://aol.sportingnews.com/")) return false

    isRegexMatchedArticle(url, regexSportingNews)
  }

  def isUnitTestArticle(url: String): Boolean = {
    url.startsWith("http://cats.com")
  }

  val regexBuzznet: Regex = """^http://[a-zA-Z0-9-]+\.buzznet\.com/user/(?:journal/\d{4,}/[a-zA-Z-]+|photos|audio/[a-zA-Z-]+\d{4,}|polls/\d{4,})/(?:(?!(?:[a-zA-Z0-9-]+/|)\?id=\d{3,})).*$""".r
  val regexBuzznetKeyPage: Regex = """^http://[a-zA-Z0-9-]+\.buzznet\.com/user/(?>journal|photos|audio|polls)/.*$""".r

  def isBuzznetArticle(url: String): Boolean = isRegexMatchedArticle(url, regexBuzznet)

  val wsjBlogPrefix = "http://blogs.wsj.com/"
  val wsjArticlePrefix = "http://online.wsj.com/article/"

  val wsjBlogRegex: Regex = """^http://blogs\.wsj\.com/[a-z0-9_-]+/\d{4}/\d{2}/\d{2}/[a-z0-9_-]+/.*$""".r
  val wsjArticleRegex: Regex = """^http://online\.wsj\.com/article/sb[0-9]+\.html.*$""".r

  def isWsjPrefixedOk(url: String): Boolean = url.startsWith(wsjArticlePrefix) || url.startsWith(wsjBlogPrefix)

  def isWsjArticle(url: String): Boolean = {
    if (url.startsWith(wsjArticlePrefix)) return isRegexMatchedArticle(url, wsjArticleRegex)

    if (url.startsWith(wsjBlogPrefix) && isRegexMatchedArticle(url, wsjBlogRegex) && StringUtils.countMatches(url, "/") == 8) {
      return true
    }

    false
  }

  val regexCatster: Regex = """^http:.*catster.com/(!:cat-health-care|cat-health-care|cat-pet-insurance|cat-pet-insurance|kittens|cat-behavior|green-cats|cats-101|cat-food|cat-adoption)/[\w-]+.*$""".r

  def isCatsterArticle(url: String): Boolean = {
    /**
     * we don't want certain urls like groups/forums
     * http://www.catster.com/group/Fancypants_cafe_where_everybody_knows_your_name-7838
     * http://www.catster.com/forums/Cat_Health/thread/705473
     * http://www.catster.com/answers/question/what_are_some_simple_ways_to_green_your_cats_lifestyle-48071
     * http://www.catster.com/register/
     * http://www.catster.com/games/cat-breed-photo-game/
     * http://www.catster.com/polls/What_is_your_cats_favorite_thing_to_steal-265
     * http://www.catster.com/photoviewer/gallery/Bag
     * http://www.catster.com/cats/1155722
     * http://www.catster.com/family/1004964
     *
     * urls that we do want
     *
     * http://www.catster.com/cat-food/cat-food-wet-or-dry
     * http://www.catster.com/cat-health-care/cleaning-cat-ears
     * http://www.catster.com/cat-pet-insurance/ - this is a tough one because it doesn't have another segment after the first part
     * http://www.catster.com/cat-adoption/why-adopt-a-cat
     * http://blogs.catster.com/kitty-news-network/2010/12/17/holiday-miracle-cat-left-to-die-making-full-recovery/
     * http://www.catster.com/cat-health-care/cleaning-cat-ears
     * http://www.catster.com/kittens/How-to-Give-Your-Cat-a-Massage-127
     * http://www.catster.com/cat-behavior/cat-socialization
     * http://www.catster.com/green-cats/how-to-reduce-your-cats-carbon-pawprint
     * http://www.catster.com/cats-101/lost-cat
     */

    if (url.startsWith("http://blogs.catster.com")) {
      return true
    } else {
      url.toLowerCase match {
        case regexCatster(domain) => return true
        case _ =>
      }
    }
    false
  }

  val regexDogster: Regex = """^http:.*dogster.com/(!:puppies|dog-health-care|puppies|dogs-101|dog-food|dog-adoption|dog-travel)/[\w-]+.*$""".r

  def isDogsterArticle(url: String): Boolean = {
    /**
     * we don't want certain urls like groups/forums
     * http://www.dogster.com/group/Fancypants_cafe_where_everybody_knows_your_name-7838
     * http://www.dogster.com/forums/Dog_Health
     * http://www.dogster.com/diary/dcentral.php
     *
     * urls that we do want
     * http://www.dogster.com/puppies/How-to-Keep-Your-Pet-Safe-with-Microchipping-and-Tagging-70
     * http://www.dogster.com/dog-health-care/dog-obesity
     * http://www.dogster.com/dog-food/how-to-choose-a-healthy-dog-food
     * http://www.dogster.com/puppies/puppy-socialization
     * http://www.dogster.com/dog-adoption/costs-to-adopt-a-dog
     * http://www.dogster.com/dog-travel/dog-car-travel
     * http://www.dogster.com/dogs-101/dog-boarding
     * http://dogblog.dogster.com/2011/05/18/tornado-dog-crawls-home-with-two-shattered-legs
     * http://dogblog.dogster.com/2011/05/13/special-photo-essay-dogs-of-war/
     */

    if (url.startsWith("http://dogblog.dogster.com")) {
      return true
    } else {
      url.toLowerCase match {
        case regexDogster(domain) => return true
        case _ =>
      }
    }
    false
  }

  val regexScribd: Regex = """^http://.*(scribd.com)/doc/\d+/.*$""".r
  val regexEnglishScribd: Regex = """^http://(www.scribd.com)/doc/\d+/.*$""".r

  def isScribdArticle(url: String): Boolean = isRegexMatchedArticle(url, regexScribd)

  def isEnglishScribdArticle(url: String): Boolean = isRegexMatchedArticle(url, regexEnglishScribd)

  // good article:
  // http://www.xojane.com/fashion/leather-paper-bag

  val regexXojane: Regex = """^http:.*xojane.com/(!:terms|fashion|janes-phone|new-agey|beauty|sports|entertainment|healthy|sex|ihtm|it-happened-me|janes-stuff|relationships|diy|tech|fun|family-drama)/[\w-]+.*$""".r

  def isXoJaneArticle(url: String): Boolean = isRegexMatchedArticle(url, regexXojane)

  val regexTimeWP: Regex = """^http://.*(time.com)/\d{4}/\d{2}/\d{2}/[a-zA-Z0-9_-]+/.*$""".r

  def isTimeWordpressArticle(url: String): Boolean = isRegexMatchedArticle(url, regexTimeWP)

  val regexTimeOther: Regex = """^http://(www.time.com)/time/[a-z/]+/article/[0-9,_]+\.html.*$""".r

  def isTimeOtherArticle(url: String): Boolean = isRegexMatchedArticle(url, regexTimeOther)

  val regexCnnMoney: Regex = """^http://(.*fortune|.*money)\.cnn\.com/.*\d{4}/\d{2}/\d{2}/.*$""".r
  val cnnMoneyDomainParts: Set[String] = Set("fortune", "cnn", "com", "money")

  def isCnnMoneyArticle(url: String): Boolean = isRegexMatchedArticle(url, regexCnnMoney)

  val regexTechCrunch: Regex = """^http://(techcrunch.com)/\d{4}/\d{2}/\d{2}/[a-zA-Z0-9_-]+/.*$""".r

  def isTechCrunchArticle(url: String): Boolean = isRegexMatchedArticle(url, regexTechCrunch)


  val regexYahooNews: Regex = """^http://news\.yahoo\.com/[a-zA-Z0-9_/%-]+\.html.*$""".r

  def ignoreYahooNews(url: String): Boolean = {
    if (url.startsWith("http://news.yahoo.com/video/")) {
      return true
    } else if (url.startsWith("http://news.yahoo.com/local/")) {
      return true
    }

    val parts = tokenize(url, "/")

    if (parts.isEmpty) return false

    val last = parts.last

    val pos = last.indexOf(".html")

    if (pos < 1) return false

    last.substring(0, pos).tryToInt match {
      case Some(_) => true
      case None => false
    }
  }

  def isYahooNewsArticle(url: String): Boolean = {
    if (ignoreYahooNews(url)) {
      false
    } else if (url.startsWith("http://news.yahoo.com/")) {
      isRegexMatchedArticle(url, regexYahooNews)
    } else {
      url.tryToURL.map(_.getAuthority.endsWith("yahoo.com")).getOrElse(false)
    }
  }

  val regexWesh: Regex = """^http://www\.wesh\.com/[a-z-]+/\d{2,12}/detail.html.*$""".r

  def isWeshArticle(url: String): Boolean = {
    if (url.startsWith("http://www.wesh.com/video")) {
      false
    } else if (url.startsWith("http://www.wesh.com/")) {
      isRegexMatchedArticle(url, regexWesh)
    } else false
  }

  val regexBoston: Regex = """^http://(?:www.boston|boston)\.com/[a-zA-Z0-9/_-]+/\d{4}/\d{2}/[a-zA-Z0-9/_-]+(?:index.html|.html|).*$""".r
  val regexBostonPhotos: Regex = """^http://(?:www.boston|boston)\.com/[a-zA-Z0-9/_-]+/(?:photogallery|gallery)/[a-zA-Z0-9/_-]+.*$""".r
  val regexBostonPhotos2: Regex = """^http://(?:www.boston|boston)\.com/[a-zA-Z0-9/_-]+/(?:photogallery|gallery)\.html.*$""".r
  val regexBostonVideos: Regex = """^http://(?:www.boston|boston)\.com/[a-zA-Z0-9/_-]+/video/\?bctid=\d{3,}.*$""".r
  val regexBostonEvents: Regex = """^http://calendar\.boston\.com/boston_ma/events/show/\d{3,}[a-zA-Z0-9-]+.*$""".r

  def isBostonArticle(url: String): Boolean = {
    if (url.startsWith("http://www.boston.com/") || url.startsWith("http://boston.com/")) {
      if (url.contains("/photogallery/") || url.contains("/gallery/") ||
        url.contains("/photogallery.html") || url.contains("/gallery.html")) {
        isRegexMatchedArticle(url, regexBostonPhotos) || isRegexMatchedArticle(url, regexBostonPhotos2)
      } else if (url.contains("/video/?bctid=")) {
        isRegexMatchedArticle(url, regexBostonVideos)
      } else {
        isRegexMatchedArticle(url, regexBoston)
      }
    } else if (url.startsWith("http://articles.boston.com/")) {
      false
    } else if (url.startsWith("http://calendar.boston.com/boston_ma/events/show/")) {
      isRegexMatchedArticle(url, regexBostonEvents)
    } else {
      false
    }
  }

  val regexVoices: Regex = """^http://voices\.yahoo\.com/(?:article/\d{5,10}?/|)[a-z0-9_-]+.html.*$""".r

  def ignoreVoicesUrl(url: String): Boolean = {
    if (url.startsWith("http://voices.yahoo.com/content_date")) return true
    if (url.startsWith("http://voices.yahoo.com/library_slideshow.html")) return true
    if (url.startsWith("http://voices.yahoo.com/library_video.html")) return true
    if (url.startsWith("http://voices.yahoo.com/search_advanced.html")) return true

    false
  }

  def isVoicesArticle(url: String): Boolean = {
    if (ignoreVoicesUrl(url)) {
      false
    } else if (url.startsWith("http://voices.yahoo.com/")) {
      isRegexMatchedArticle(url, regexVoices)
    } else false
  }

  //////////////////////http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2012/03/19/BA0T1NN13J.DTL
  val regexSFGate: Regex = """^http://(?:www|insidescoopsf|blog)\.sfgate\.com/(?:cgi-bin/article\.cgi\?f=/[a-z]/[a-z]/\d{4}/\d{2}/\d{2}/[a-z0-9_/-]{5,}\.dtl|[a-z0-9-]+/\d{4}/\d{2}/\d{2}/[a-z0-9%_-]+/).*$""".r

  def isSFGateArticle(url: String): Boolean = {
    if (url.startsWith("http://www.sfgate.com/cgi-bin/article.cgi") ||
      url.startsWith("http://insidescoopsf.sfgate.com/blog/") ||
      url.startsWith("http://blog.sfgate.com/")) {
      isRegexMatchedArticle(url, regexSFGate)
    } else {
      false
    }
  }

  val regexMensFitness: Regex = """^http://www.mensfitness.com/[a-z0-9]+.*$""".r

  def ignoreMFUrl(url: String): Boolean = {
    if (!url.startsWith("http://www.mensfitness.com/")) return true
    if (url.startsWith("http://www.mensfitness.com/terms")) return true
    if (url.startsWith("http://www.mensfitness.com/privacy")) return true
    if (url.startsWith("http://www.mensfitness.com/contact")) return true
    if (url.startsWith("http://www.mensfitness.com/search/")) return true
    if (url.startsWith("http://www.mensfitness.com/doubleclick/")) return true

    false
  }

  def isMensFitnessArticle(url: String): Boolean = {
    if (ignoreMFUrl(url)) return false

    isRegexMatchedArticle(url, regexMensFitness)
  }

  val regexWalyou: Regex = """^http://walyou\.com/[a-z0-9-]+.*$""".r

  def ignoreWalyouUrl(url: String): Boolean = {
    if (!url.startsWith("http://walyou.com/")) return true
    if (url.startsWith("http://walyou.com/page/")) return true
    if (url.startsWith("http://walyou.com/category/")) return true
    if (url.startsWith("http://walyou.com/tag/")) return true
    if (url.startsWith("http://walyou.com/about/")) return true
    if (url.startsWith("http://walyou.com/free-newsletter/")) return true
    if (url.startsWith("http://walyou.com/privacy-policy/")) return true
    if (url.startsWith("http://walyou.com/video/")) return true
    if (url.startsWith("http://walyou.com/advertise/")) return true
    if (url.startsWith("http://walyou.com/press/")) return true
    if (url.startsWith("http://walyou.com/contact/")) return true
    if (url.startsWith("http://walyou.com/links/")) return true
    if (url.startsWith("http://walyou.com/hands-on-reviews/")) return true
    if (url.startsWith("http://walyou.com/walyou-team/")) return true
    if (url.startsWith("http://walyou.com/write-for-us/")) return true

    false
  }

  def isWalyouArticle(url: String): Boolean = {
    if (ignoreWalyouUrl(url)) return false

    isRegexMatchedArticle(url, regexWalyou)
  }

  val regexDyingScene: Regex = """^http://dyingscene\.com/news/[a-z0-9_%\(\)-]+.*$""".r

  def isDyingSceneArticle(url: String): Boolean = {
    if (url.startsWith("http://dyingscene.com/news/")) {
      isRegexMatchedArticle(url, regexDyingScene)
    } else {
      false
    }
  }

  val regexSuite101: Regex = """^http://suite101\.com/article/[a-z0-9_-]+-a\d{3,10}.*$""".r

  def isSuite101Article(url: String): Boolean = {
    if (url.startsWith("http://suite101.com/article/")) {
      isRegexMatchedArticle(url, regexSuite101)
    } else {
      false
    }
  }

  val regexYahooOlympics1: Regex = """^http://sports\.yahoo\.com/news/[a-z0-9-]+--oly\.html.*$""".r
  val regexYahooOlympics2: Regex = """^http://sports\.yahoo\.com/news/[a-z0-9-]+olympic[a-z0-9-]*\.html.*$""".r
  val regexYahooOlympics3: Regex = """^http://sports\.yahoo\.com/news/olympic[a-z0-9-]+\.html.*$""".r
  val regexYahooOlympicsBlog: Regex = """^http://sports\.yahoo\.com/blogs/olympics-fourth-place-medal/[a-z0-9-]+\.html.*$""".r
  val regexYahooOlympicsPhotos1: Regex = """^http://sports\.yahoo\.com/photos/olympics[a-z0-9-]+/.*$""".r
  val regexYahooOlympicsPhotos2: Regex = """^http://sports\.yahoo\.com/photos/olympics[a-z0-9-]+/[a-z0-9-]+\.html.*$""".r

  def isForYahooOlympics(url: String, mustBeArticle: Boolean = true): Boolean = {
    if (url.startsWith("http://sports.yahoo.com/blogs/olympics-fourth-place-medal/")) {
      if (mustBeArticle) {
        return isRegexMatchedArticle(url, regexYahooOlympicsBlog)
      } else {
        return true
      }
    }

    if (url.startsWith("http://sports.yahoo.com/news/olympic")) {
      if (mustBeArticle) {
        return isRegexMatchedArticle(url, regexYahooOlympics3)
      } else {
        return true
      }
    }

    if (url.startsWith("http://sports.yahoo.com/photos/olympics")) {
      if (mustBeArticle) {
        if (isRegexMatchedArticle(url, regexYahooOlympicsPhotos1) || isRegexMatchedArticle(url, regexYahooOlympicsPhotos2)) {
          return true
        }
      } else {
        return true
      }
    }

    if (url.startsWith("http://sports.yahoo.com/news/")) {
      if (isRegexMatchedArticle(url, regexYahooOlympics1) || isRegexMatchedArticle(url, regexYahooOlympics2)) {
        return true
      }
    }

    if (!mustBeArticle && url.startsWith("http://sports.yahoo.com/olympics/")) return true

    false
  }

  def isEspnArticle(url: String): Boolean = {
    if (url.contains("espn.go.com/") && (url.contains("/_/id/") || url.contains("gameId="))) true else false
  }

  val regexEngadgetArticle: Regex = """^http://(?:www.engadget|engadget)\.com/\d{4}/\d{2}/\d{2}/[a-zA-Z0-9/_-]+.*$""".r
  def isEngadgetArticle(url: String): Boolean = {
    if (isRegexMatchedArticle(url, regexEngadgetArticle)) {
      return true
    }
    false
  }

  val regexStylelistView: Regex = """^http://(?:www.stylelist|stylelist).com/news/[a-z0-9_%\(\)-]+.*$""".r
  def isStylelistArticle(url: String): Boolean = {
    if (url.startsWith("http://www.stylelist.com/view/")) {
      if (isRegexMatchedArticle(url, regexStylelistView)) {
        return true
      }
    }

    false
  }

  val regexHuffingtonPostArticle: Regex = """^http://(?:www.huffingtonpost|huffingtonpost)\.com/\d{4}/\d{2}/\d{2}/[a-zA-Z0-9/_-]+\.html.*$""".r
  def isHuffingtonPostArticle(url: String): Boolean = {
    if (isRegexMatchedArticle(url, regexStylelistView)) {
      return true
    }

    false
  }

  val regexDailyFinanceArticle: Regex = """^http://(?:www.dailyfinance|dailyfinance)\.com/\d{4}/\d{2}/\d{2}/[a-zA-Z0-9/_-]+.*$""".r
  def isDailyFinanceArticle(url: String): Boolean = {
    if (isRegexMatchedArticle(url, regexDailyFinanceArticle)) {
      return true
    }
    false
  }

  def isRegexMatchedArticle(url: String, regex: Regex): Boolean = regex.pattern.matcher(url.toLowerCase).matches()

  /**
   * determine if this url is from one of our major partners
   * this is just a primary filter so we want this one to be fast (read: no regex)
   */
  def getPartnerName(url: String): Option[String] = url.tryToURL match {
    case Some(u) => {

      def partnerMatchFromHost(host: String): Option[String] = host.toLowerCase match {
        case SCRIBD => Some(SCRIBD)
        case BUZZNET => Some(BUZZNET)
        case CATSTER => Some(CATSTER)
        case DOGSTER => Some(DOGSTER)
        case XOJANE => Some(XOJANE)
        case WSJ => Some(WSJ)
        case TIME => Some(TIME)
        case TECHCRUNCH => Some(TECHCRUNCH)
        case WESH => Some(WESH)
        case BOSTON => Some(BOSTON)
        case SFGATE => Some(SFGATE)
        case MENSFITNESS => Some(MENSFITNESS)
        case DYING_SCENE => Some(DYING_SCENE)
        case METAL_RIOT => Some(METAL_RIOT)
        case SUITE_101 => Some(SUITE_101)
        case WALYOU => Some(WALYOU)
        case WORDPRESS => Some(WORDPRESS)
        case GILTCITY => Some(GILTCITY)
        case UNITTEST => Some(UNITTEST)
        case WETPAINT => Some(WETPAINT)
        case DAILYMOTION => Some(DAILYMOTION)
        case ENGADGET => Some(ENGADGET)
        case STYLELIST => Some(STYLELIST)
        case DAILY_FINANCE => Some(DAILY_FINANCE)
        case HUFFINGTON_POST => Some(HUFFINGTON_POST)
        case AOL => Some(AOL)
        case MSN => Some(MSN)
        case _ => None
      }

      nullToEmpty(u.getAuthority) match {
        case "money.cnn.com" => Some(CNN_MONEY)

        case "voices.yahoo.com" => Some(VOICES)

        case "aol.sportingnews.com" => Some(SPORTING_NEWS)

        case "sportsillustrated.cnn.com" => Some(SPORTSILLUSTRATED)

        case "sports.yahoo.com" => if (isForYahooOlympics(url, mustBeArticle = false)) Some(YAHOO_OLYMPICS) else None

        case "www.healthcentral.com" => Some(HEALTH_CENTRAL)

        case "www.drugs.com" => Some(DRUGS)

        case "www.theguardian.com" => Some(THE_GUARDIAN)

        case maybeMsn if maybeMsn.endsWith(".msn.com") => Some(MSN)   // e.g. www.msn.com, zone.msn.com, etc.

        case maybeWebMd if maybeWebMd.endsWith("webmd.com") => Some(WEBMD)

        case maybeWebMdBoots if maybeWebMdBoots.endsWith("webmd.boots.com") => Some(WEBMD)

        case maybeCbsSports if (!ScalaMagic.isNullOrEmpty(maybeCbsSports) && maybeCbsSports.endsWith("cbssports.com")) => Some(CBS_SPORTS)

        case maybeGadget if maybeGadget.endsWith("gadgetreview.com") => Some(GADGETREVIEW)

        case maybeEspn if maybeEspn.endsWith("espn.go.com") => Some(ESPN)

        case auth: String if auth.endsWith("yahoo.com") => Some(YAHOO_NEWS)

        // all others ignore all but the piece preceeding the TLD
        case host => tokenize(host, ".").toList match {
          case Nil => None // not sure if this is even possible
          case server :: Nil => partnerMatchFromHost(server) // http://servername/
          case domain :: tld :: Nil => partnerMatchFromHost(domain) // http://scrawlfx.com/
          case parts => {
            // cnn has some crazy level subdomains like: postcards.blogs.fortune.cnn.com
            var score = 0
            for (part <- parts) {
              if (cnnMoneyDomainParts.contains(part)) score += 1
            }
            if (score >= 2) {
              Some(CNN_MONEY)
            } else {
              partnerMatchFromHost(parts.dropRight(1).takeRight(1).head) // all others
            }
          }
        }
      }
    }
    case None => None
  }


}

object ContainsPerformanceTest {
  def main(args: Array[String]) {
    for (j <- 0 until 10) {
      val clock: StopWatch = new StopWatch()

      clock.start()
      for (i <- 0 until 100000) {
        val url = "http://mysite.com/article/123"
        if (url.contains("mysite")) "true" else "false"
      }
      clock.stop()

      println("It takes " + clock.getTime + " milliseconds to use contains")
    }


  }
}


object RegexPerformanceTest {
  val regex: Regex = """mysite""".r

  def main(args: Array[String]) {
    for (j <- 0 until 10) {

      val clock: StopWatch = new StopWatch()
      clock.start()
      for (i <- 0 until 100000) {
        val url = "http://mysite.com/article/123"
        url match {
          case regex(url) => "true"
          case _ => "false"
        }
      }
      clock.stop()

      println("It takes " + clock.getTime + " milliseconds to use regex")
    }
  }
}
