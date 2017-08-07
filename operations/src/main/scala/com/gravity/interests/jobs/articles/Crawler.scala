package com.gravity.interests.jobs.articles

import java.net.URI

import com.gravity.goose.Article
import com.gravity.goose.network.NotHtmlException
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.logging.Logstashable
import com.gravity.service.grvroles
import com.gravity.utilities.ScalaMagic._
import com.gravity.utilities._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.web.ContentUtils

import scala.util.matching.Regex
import scalaz.Scalaz._
import scalaz._

/**
 * Our synchronous crawling logic.
 */
trait Crawler {
 import com.gravity.logging.Logging._
  import Crawler._
  import com.gravity.utilities.Counters._
  val counterCategory = "Crawler"

  val defaultMinimumContentLength = 5
  val crawlErrorLimit = 3

  def validateAndCrawl(url: String, siteGuid: String): Validation[CrawlRetry, CrawlSuccess] = validateAndCrawl(CrawlCandidate(siteGuid, url, 0, ingestionSource = IngestionTypes.fromCrawlingService, articleType = ArticleService.detectArticleType(url, siteGuid)))

  def validateAndCrawl(cc: CrawlCandidate, forUnitTesting: Boolean = false): Validation[CrawlRetry, CrawlSuccess] = {
    if (cc.isPubSub) pubSubMsgCnt.increment

    val urlLowerCase = cc.url.toLowerCase

    // Added to research unusual siteGuid behavior for theorbit.com 08/04/2014 - TJC.
    if (cc.siteGuid == "e9fce3f7001d9d32fe584d8b6b309439" || urlLowerCase.contains("theorbit.com"))    // TJC-DEBUG
      info(s"validateAndCrawl-theorbit.com url=`${cc.url}`, sg=`${cc.siteGuid}`")

    if (urlLowerCase.endsWith(".bz2") || urlLowerCase.contains("wikimedia")) {
      skippedDueToExtension.increment
      return blacklistedFailure
    }

    // If we've hit the crawl limit (3 retries) then let him RIP.
    if (cc.errors > crawlErrorLimit) {
      totalFailureCnt.increment
      return Failure(RetriesExhausted)
    }

    tryCrawl(cc, forUnitTesting)
  }

  def isContentCrappy(content: String, siteGuid: String): Boolean = {
    // so far we only have cases for Yahoo! News
    if (siteGuid != SiteService.YAHOONEWSGUID) return false

    // first search for positive ids and cast out on the first occurence returning true (is crappy)
    for (cp <- crapPositives) {
      if (content.contains(cp)) return true
    }

    // second search for negative ids and ignore all else returning false (not crappy)
    for (cn <- crapNegatives) {
      if (content.contains(cn)) return false
    }

    // really this means that none of the indicators of good were found, so return that it's crap
    true
  }

  protected def tryCrawl(cc: CrawlCandidate, forUnitTest: Boolean = false, useUrlBasedPublishTime: Boolean = false): Validation[CrawlRetry, CrawlSuccess] = {
    val result = tryCrawlLow(cc, forUnitTest, useUrlBasedPublishTime)
    result
  }

  val crawlBlacklistSGs: List[String] = List("f65128600d3e0725c20254455e803b8b")
  val crawlBlacklistHosts: List[String] = List("stylemepretty.com", "b.photobucket.com")

  protected def tryCrawlLow(inCc: CrawlCandidate, forUnitTest: Boolean, useUrlBasedPublishTime: Boolean): Validation[CrawlRetry, CrawlSuccess] = {
    var cc = inCc

    if (isNullOrEmpty(cc.url)) return blacklistedFailure

    val lowCcUrl  = cc.url.toLowerCase
    val lowCCAuth = lowCcUrl.tryToURL.flatMap(url => Option(url.getAuthority)).getOrElse("")

    if (crawlBlacklistSGs.contains(cc.siteGuid) || crawlBlacklistHosts.exists(lowCCAuth endsWith)) {
      info("Attempted to crawl blacklisted URL: : {0} and with siteGuid: {1} and source: {2}", cc.url, cc.siteGuid, cc.ingestionSource.toString)
      return blacklistedFailure
    }

    // For now, keeping this code here as a fallback.
    if (lowCcUrl.startsWith("http://www.stylemepretty.com") || lowCcUrl.startsWith("https://www.stylemepretty.com")) {
      info("Attempted to crawl a StyleMePretty URL: {0} and with siteGuid: {1} and source: {2}", cc.url, cc.siteGuid, cc.ingestionSource.toString)
      return blacklistedFailure
    }

    // Temporarily (added 11/21/2014) patch incoming WaPo HIGHLIGHTER beacons to have WaPo siteGuid,
    // to ensure that beacon ingestion is not accidentally marking WaPo articles with the HIGHLIGHTER SiteGuid.
    if (cc.siteGuid == SiteService.HIGHLIGHTER && lowCcUrl.startsWith("http://www.washingtonpost.com")) {
      countPerSecond(counterCategory, "Received HIGHLIGHTER beacon for WaPo")
      cc = inCc.copy(siteGuid = "251a5b8e140a4a07def415e616d000a0")
    }

    def writeCrawlFailed(failed: FailureResult) {
      val failedMsg = if(failed.hasException) failed.toString else failed.message
      if (!Crawler.areWeRtbRole) {
        warn(UrlCrawlFailure(cc.url, failedMsg, "Failed to crawl URL: '" + cc.url + "' " + failedMsg))
      }

      ArticleService.modifyPutArticle(cc.url)(_.value(_.url, cc.url).value(_.crawlFailed, failedMsg)) match {
        case Success(_) =>
        case Failure(fails) => warn(fails, "Failed to put crawlFailed for article URL: " + cc.url)
      }
    }

    if(cc.ingestionSource.id != IngestionTypes.fromAccountManager.id && cc.ingestionSource.id != IngestionTypes.fromCrawlingService.id && cc.url.startsWith("http://www.rantsports.com/clubhouse/")) {
      info("Rejecting rantspots clubhouse page because they are all responding slowly / returning null article titles.")
      return blacklistedFailure
    }

    val siteMetaOption = SiteService.siteMeta(cc.siteGuid)

    // Don't return an AlreadyRssIngested failure if the sg/url is wanted by a beacon-ingestable campaign.
    if (!CampaignIngestion.isUrlBeaconIngestableBySite(cc.url, cc.siteGuid)) {
      siteMetaOption.foreach(site => {
        if (site.isRssIngested) {
          // Only re-crawl an RSS-ingested site if we can meaningfully improve our situation.
          if (ArticleCrawlingService.isInArticlesTableWithNonEmptyTitleAndValidSiteGuid(cc.url)) {
            trace("Crawler :: RSS ingested site article already ingested with title :: " + cc.url)
            alreadyRssIngestedSkips.increment
            return AlreadyRssIngested.failure
          }
        }
      })
    }

    def getCrawlSettings: (Boolean, Int) = {
      if (cc.ingestionSource == IngestionTypes.fromAccountManager) {
        // we an account manager is adding this content, then let's force image extraction and skip the content length restriction
        return (true, 0)
      }

      siteMetaOption match {
        case Some(s) => (s.supportsArticleImageExtraction, s.column(_.minArticleContentLength).getOrElse(defaultMinimumContentLength))

        case None => (false, 0) // if this siteGuid is NOT in the sites table, treat it as a virtual and therefore do not require a minimum content length
      }
    }

    try {
      val (extractImages, minContentLength) = getCrawlSettings

      // call goose to extract out the infos
      val article = fetchAndExtractMetadata(cc.url, extractImages, useUrlBasedPublishTime)

      if (article == null || article.title == null || article.cleanedArticleText.length < minContentLength) {

        failedUrlCount.increment
        val failedMsg = if (article == null) {
          if (useUrlBasedPublishTime) {
            "Returned null Article.  Unable to extract publishTime form Url"
          }
          else {
            "fetchAndExtractMetadata returned a null Article!"
          }
        } else if (article.title == null) {
          "returned Article title was null!"
        } else {
          "returned Article cleanedArticleText length was " + article.cleanedArticleText.length + " and therefore less than the min: " + minContentLength
        }

        writeCrawlFailed(FailureResult(failedMsg))
        Failure(Retry)
      }
      else {
//        if (isContentCrappy(article.cleanedArticleText, cc.siteGuid)) {
//          yahooArticlesRejectedCounter.increment
//          info("Rejected Yahoo Crap Article: " + cc.url)
//          return blacklistedFailure
//        }

        val result = if (forUnitTest) {
          val testResult = CrawlSuccess(article, cc.siteGuid, shouldSaveToHbase = false, cc.sectionPath, cc.ingestionSource, cc.articleType)
          info("Article logString: " + testResult)

          testResult
        }
        else {
          trace("CRAWLER :: Successfully crawled URL: {0} withImages: {1}", cc.url, extractImages)
          urlsCrawledCount.increment
          cc.siteGuid match {
            case SiteService.TECHCRUNCH => techcrunchArticlesSentToRemoteCounter.increment
            case _ => // nothing to see here, but it beats a scala.MatchError
          }

          if (cc.isPubSub) pubsubCrawlCnt.increment

          val sectionPath = article.additionalData.get("section") match {
            case Some(section) => section
            case None => cc.sectionPath
          }

          CrawlSuccess(article, cc.siteGuid, shouldSaveToHbase = true, sectionPath, cc.ingestionSource, cc.articleType)
        }

        result.success
      }
    } catch {
      case e: NotHtmlException =>
        notHtmlCnt.increment
        info("url: " + cc.url + " does not seem to be HTML!, aborting")
        writeCrawlFailed(FailureResult(e))
        Failure(NoHtmlReturned)
      case e: Exception =>
        failedUrlCount.increment
        info("url: " + cc.url + " got exception " + ScalaMagic.formatException(e))
        writeCrawlFailed(FailureResult(e))
        Failure(Retry)
    }
  }

  def fetchAndExtractMetadata(url: String, withImages: Boolean, useUrlBasedPublishTime: Boolean = false): Article = {
    trace("CRAWLER :: attempting URL: {0} withImages: {1}", url, withImages)
    if (useUrlBasedPublishTime) {
      ContentUtils.fetchAndExtractMetadataWithUrlBasedPublishTime(url, withImages)
    }
    else {
      ContentUtils.fetchAndExtractMetadata(url, withImages)
    }
  }

  def sendToHbase(crawlSuccess: CrawlSuccess, query: ArticleService.QuerySpec = _.withFamilies(_.meta)): ValidationNel[FailureResult, ArticleRow] = {
    ArticleService.saveGooseArticle(
      crawlSuccess.gooseArticle, crawlSuccess.siteGuid, sendGraphingRequest = false,
      sectionPath = crawlSuccess.sectionPath, query = query, ingestionSource = crawlSuccess.ingestionSource,
      articleType = crawlSuccess.articleType)
  }

}

object Crawler {
  import Counters._
  getOrMakePerSecondCounter("crawler", "")
  val urlsCrawledCount: PerSecondCounter = getOrMakePerSecondCounter("crawler", "URLs crawled", shouldLog = true)
  val failedUrlCount: PerSecondCounter = getOrMakePerSecondCounter("crawler", "Failure To Crawl")

  val notHtmlCnt: PerSecondCounter =  getOrMakePerSecondCounter("crawler", "Not HTML Exception")

  val skippedDueToExtension: PerSecondCounter = getOrMakePerSecondCounter("crawler", "Wrong Extensions")

  val pubSubMsgCnt: PerSecondCounter = getOrMakePerSecondCounter("crawler", "PubSub Articles Submitted")
  val pubsubCrawlCnt: PerSecondCounter = getOrMakePerSecondCounter("crawler", "PubSub Articles Crawled")
  val totalFailureCnt: PerSecondCounter = getOrMakePerSecondCounter("crawler", "Total failures logged to disk")
  val doNotCrawlCnt: PerSecondCounter = getOrMakePerSecondCounter("crawler", "Do Not Crawl Messages")
  val alreadyRssIngestedSkips: PerSecondCounter = getOrMakePerSecondCounter("crawler", "Already RSS Ingested")

  val articlesSavedToHbaseCnt: PerSecondCounter = getOrMakePerSecondCounter("crawler", "Articles Saved To HBASE")
  val articlesFailedToHbaseCnt: PerSecondCounter = getOrMakePerSecondCounter("crawler", "Articles Failed To HBASE")
  val articlesSentToRemoteCounter: PerSecondCounter = getOrMakePerSecondCounter("crawler", "Articles sent to remote service")

  val techcrunchArticlesSentToRemoteCounter: PerSecondCounter = getOrMakePerSecondCounter("crawler", "Techcrunch Articles sent to remote service")

  val crapNegatives: Seq[String] = Seq("\n", "<br", "<BR", "<p", "<P", "<div", "<DIV")
  val crapPositives: Seq[String] = Seq.empty[String] //Seq("Unfortunately your browser does not support IFrames")

  val blacklistedFailure: Validation[CrawlRetry, CrawlSuccess] = Failure(Blacklisted)

  val grvDelim = "<GRV>"

  val grvDelimRegex: Regex = grvDelim.r

  val cleanReps: ReplaceSequence = buildReplacement("\\^", "<CAR>").chain("\\n", "<BR>").chain("(\n|\r\n|\r)", "<BR>")

  def cleanText(input: String): String = cleanReps.replaceAll(input).toString

  val SOME_PUBLISH_OPTION: Some[String] = Some("publish")

  lazy val areWeRtbRole: Boolean = grvroles.isInRole("RTB_PROC")

  case class UrlCrawlFailure(url: String, reason: String, message: String) extends Logstashable {
    import com.gravity.logging.Logstashable._
    override def getKVs: Seq[(String, String)] = {
      Seq(Url -> url, Message -> message)
    }
  }

  def resolveFullCanonicalLink(article: Article): String = {
    import com.gravity.utilities.grvstrings._

    article.canonicalLink.tryToURL match {
      // if the canonicalLink is a fully-qualified url, then it's OK
      case Some(url) => url.toString
      case None =>
       article.finalUrl.tryToURL match {
          // try to resolve the canonicalLink against the finalUrl
          case Some(finalUrl) => finalUrl.toURI.resolve(article.canonicalLink).toString
          // if we don't have a finalUrl (is this even possible?), then try to resolve it against hostname
          case None =>
            new URI("http://" + article.domain + "/").resolve(article.canonicalLink).toString
       }
    }

  }
}

case class CrawlCandidate(siteGuid: String, url: String, errors: Int, crawlType: String = "igi", sectionPath: String = emptyString, articleType: ArticleTypes.Type = ArticleTypes.content, ingestionSource: IngestionTypes.Type = IngestionTypes.fromBeacon){
  def isPubSub: Boolean = crawlType == "pubsub"

  def isReGraph: Boolean = crawlType == "regraph"

  def automaticallySaveToHbase: Boolean = CrawlCandidate.shouldAutomaticallySaveToHbase(crawlType)

  def toQueueMessage(incrementErrors: Boolean = false): String = {
    val errs = if (incrementErrors) errors + 1 else errors
    (new StringBuilder).append(siteGuid) // 0
      .append(Crawler.grvDelim).append(url) // 1
      .append(Crawler.grvDelim).append(errs) // 2
      .append(Crawler.grvDelim).append(crawlType) // 3
      .append(Crawler.grvDelim).append(sectionPath) // 4
      .append(Crawler.grvDelim).append(articleType.id) // 5
      .append(Crawler.grvDelim).append(ingestionSource.id) // 6
      .toString()
  }
}

object CrawlCandidate {
 import com.gravity.logging.Logging._
  def parse(message: String): Option[CrawlCandidate] = {
    val parts = message.splitBetter(Crawler.grvDelim, 7)
    trace("message: `{0}` has parts: {1}", message, parts.mkString("`", "`, `", "`"))
    for {
      siteGuid <- parts.lift(0)
      url <- parts.lift(1)
      errStr <- parts.lift(2)
      errors <- errStr.tryToInt
    } yield {
      val articleType = (for {
        aTypeIdStr <- parts.lift(5)
        aTypeId <- aTypeIdStr.tryToInt
        aType <- ArticleTypes.get(aTypeId.toByte)
      } yield aType).getOrElse(ArticleTypes.unknown)

      val ingestionSource = (for {
        iTypeIdStr <- parts.lift(6)
        iTypeId <- iTypeIdStr.tryToInt
        iType <- IngestionTypes.get(iTypeId.toByte)
      } yield iType).getOrElse(IngestionTypes.fromBeacon)

      CrawlCandidate(siteGuid, url, errors, parts.lift(3).getOrElse("igi"), parts.lift(4).getOrElse(emptyString), articleType, ingestionSource)
    }
  }

  def shouldAutomaticallySaveToHbase(crawlType: String): Boolean = {
    crawlType match {
      case "pubsub" | "regraph" | "anywhere" => true
      case _ => false
    }
  }
}

case class CrawlSuccess(article: Article, siteGuid: String, shouldSaveToHbase: Boolean,
                        sectionPath: String = emptyString, ingestionSource: IngestionTypes.Type = IngestionTypes.fromBeacon,
                        articleType: ArticleTypes.Type = ArticleTypes.unknown) {
  def gooseArticle: Article = article

  //override lazy val toString: String = Crawler.convertArticleToBeaconArchiverString(article, siteGuid)
}

sealed trait CrawlRetry
case object Blacklisted extends CrawlRetry
case object AlreadyRssIngested extends CrawlRetry
case object NoHtmlReturned extends CrawlRetry
case object Retry extends CrawlRetry
case object RetriesExhausted extends CrawlRetry
