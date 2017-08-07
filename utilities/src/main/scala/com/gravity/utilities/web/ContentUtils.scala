package com.gravity.utilities.web

import java.nio.charset.CodingErrorAction
import java.util.Date
import javax.xml.datatype.DatatypeFactory

import com.gravity.goose.extractors.{AdditionalDataExtractor, ContentExtractor, PublishDateExtractor}
import com.gravity.goose.network.MaxBytesException
import com.gravity.goose.{Article, Configuration, Goose}
import com.gravity.utilities.ScalaMagic._
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.time.GrvDateMidnight
import com.gravity.utilities.web.extraction._
import com.gravity.utilities.{ScalaMagic, Settings}
import org.joda.time.{DateTime, DateTimeZone}
import org.jsoup.nodes.Element
import org.jsoup.select.{Elements, Selector}
import org.xml.sax.SAXParseException

import scala.collection.JavaConversions._
import scala.collection._
import scala.io.Codec
import scala.util.matching.Regex
import scala.xml.{Elem, XML}
import scalaz.std.option._
import scalaz.syntax.foldable._
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, Validation, ValidationNel}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object ContentUtils {
 import com.gravity.logging.Logging._
  implicit def ZZ_elements2BetterElements(x: Elements): ZZ_BetterElements = ZZ_BetterElements(x)

  val gravityBotUserAgent = "Mozilla/5.0 (compatible; Gravitybot/1.0; +http://www.gravity.com/)"

  val COMMA_SPLITTER: Regex = ",".r

  def checkSummary(article: Article): Option[String] = {
    article.additionalData.get("summary").foreach(summary => if (summary.nonEmpty) return Some(summary))

    if (isNullOrEmpty(article.cleanedArticleText)) return None

    if (article.cleanedArticleText.length > 300) Some(article.cleanedArticleText.substring(0, 300)) else Some(article.cleanedArticleText)
  }

  def extractSummary(article: Article): String = checkSummary(article).getOrElse(emptyString)

  /**
   * pass in a url and receive the content of that page back as a string, pass
   * in an optional timeout for how long you want to wait for this page to return
   * httpconnectionmanager was locking in prod, this is a workaround alternative
   * to fetch content from webpages
   */
  def getWebContentAsString(urlStr: String, logException: Boolean = true): Option[String] = {

    try {
      val resp = HttpConnectionManager.execute(urlStr)

      if (resp.status == 200 && resp.hasContent) {
        Some(resp.getContent)
      } else {
        None
      }
    } catch {
      case e: Exception =>
        if (logException) {
          critical(e, "getWebContentAsString failed with url: " + urlStr)
        }
        None
    }

  }

  def resolveCanonicalUrl(url: String, optDebugClue: Option[String] = None): ValidationNel[FailureResult, CanonicalResult] = {
    val debugClue = optDebugClue.getOrElse("")

    if (debugClue.nonEmpty)
      trace(s"Beg Canonicalization for `$debugClue` -- `$url`")

    val startMillis = System.currentTimeMillis

    try {
      GrvHtmlFetcher.resolveCanonicalUrl(url, optDebugClue)
    } finally {
      if (debugClue.nonEmpty)
        trace(s"End Canonicalization for `$debugClue` (${System.currentTimeMillis - startMillis} ms) -- `$url`")
    }
  }

  def getWebContentAsValidationString(url: String,
                                      headers: Map[String, String] = Map.empty,
                                      maxBytes: Int = 0,
                                      argsOverrides: Option[HttpArgumentsOverrides] = None,
                                      retryDelayMs: Long = 1000L,
                                      tries: Int = 1): Validation[FailureResult, String] = {
    val result = getWebContentAndStatusAsValidationString(url, headers, maxBytes, argsOverrides)._2

    if (result.isSuccess || tries <= 1)
      result
    else {
      Thread.sleep(retryDelayMs)
      getWebContentAsValidationString(url, headers, maxBytes, argsOverrides, retryDelayMs, tries - 1)
    }
  }

  def getWebContentAndStatusAsValidationString(url: String, headers: Map[String, String] = Map.empty, maxBytes: Int = 0, argsOverrides : Option[HttpArgumentsOverrides] = None): (Int, Validation[FailureResult, String]) = {
    val defaultStatus = 0
    val trimmedUrl = url.trim
    if (ScalaMagic.isNullOrEmpty(url)) (defaultStatus, FailureResult("url must be non-null & non-empty!").failure)
    else if (trimmedUrl.tryToURL.isEmpty) (defaultStatus, FailureResult("Invalid URL: " + url).failure)
    else {
      try {
        val resp = HttpConnectionManager.execute(trimmedUrl, headers = headers, maxBytes = maxBytes, argsOverrides = argsOverrides)
        if (resp.status == 200) {
          if (resp.hasContent) {
            (resp.status, resp.getContent.success)
          } else {
            (resp.status, FailureResult("No content returned for url: " + trimmedUrl).failure)
          }
        } else {
          (resp.status, FailureResult("Received HTTP response status code: " + resp.status + " for url: " + trimmedUrl).failure)
        }
      } catch {
        case max: MaxBytesException => (defaultStatus, FailureResult("Url " + trimmedUrl + " exceeded max bytes " + maxBytes).failure)
        case ex: Exception =>
          (defaultStatus, FailureResult("Failed to execute url: " + trimmedUrl + ": \n" + ex.getMessage, ex).failure)
      }
    }
  }

  // Note: By the time a UTF-8 byte-order mark has made it into a Java UTF-8 stream, it looks like this.
  // Gotta a be a better way to do this, but I'm racing the clock at the moment (04/14/14).
  // "I swear I'll come back and make this better!" -- TJC.
  // Hey @tchappell, https://code.google.com/p/springapps/source/browse/dom4j-test/src/main/java/com/studerb/dom4j_test/BomUtil.java <- similar?
  val byteMarkTakeVal: List[Int] = List(0xFEFF)

  def skipByteMarkIfPresent(rawStr: String): String = {
    val hasByteMark = (rawStr take 1).map(_ + 0).toList == byteMarkTakeVal

    if (hasByteMark)
      rawStr drop 1
    else
      rawStr
  }

  def getWebContentAsXml(urlStr: String,
                         argsOverrides : Option[HttpArgumentsOverrides] = None,
                         params: Map[String, String] = Map.empty,
                         headers: Map[String, String] = Map.empty,
                         processor: RequestProcessor = RequestProcessor.empty,
                         compress: Boolean = true): Validation[FailureResult, Elem] = urlStr.tryToURL match {
    case Some(url) =>
      val useUrlStr = url.toString
      try {
        HttpConnectionManager.request(useUrlStr, params = params, argsOverrides = argsOverrides, headers = headers, processor = processor) {
          case resultStream: HttpResultStream =>
            if (resultStream.status == 200) {
              resultStream.responseStream match {
                case Some(stream) =>
                  val codec = resultStream.codec.getOrElse(Codec.UTF8).onMalformedInput(CodingErrorAction.IGNORE).onUnmappableCharacter(CodingErrorAction.IGNORE)
                  val filteredCharacters = scala.io.Source.fromInputStream(stream)(codec).filter(c => c == '\n' || !c.isControl).toIterable
                  val lines = scala.io.Source.fromIterable(filteredCharacters).getLines()
                  try {
                    XML.loadString(skipByteMarkIfPresent(lines.mkString("\n")).dropWhile(_ == '\n')).success
                  } catch {
                    case parseEx: SAXParseException =>
                      FailureResult(s"Failed to parse response stream as XML for url: $useUrlStr", new GrvXmlLoadException(lines, parseEx)).failure
                  }
                case None => FailureResult(s"HTTP response did not contain any content for url: $useUrlStr").failure
              }
            } else {
              FailureResult(s"HTTP response status did not equal 200! Status code received: ${resultStream.status} for url: $useUrlStr").failure
            }
        }
      } catch {
        case ex: Exception => FailureResult(s"Failed to execute HTTP request for url: $useUrlStr", ex).failure
      }
    case None => FailureResult(s"`$urlStr` is not a valid URL!").failure
  }

  def fetchAndExtractMetadataWithUrlBasedPublishTime(url: String, withImages: Boolean = false): Article = {
    getUrlBasedDateTime(url) match {
      case Some(dt) => fetchAndExtractMetadata(url, withImages, dt.some)
      case _ => null
    }
  }

  def fetchAndExtractMetadata(url: String, withImages: Boolean = false, publishTimeOverride: Option[DateTime] = None): Article = {
    val article = if (ScribdDocumentFetcher.isScribdUrl(url)) {
      trace("Scribd Article to be crawled: " + url)
      ScribdDocumentFetcher.getAsGooseArticle(url) match {
        case Success(a) =>
          trace("Successfully crawled Scribd article: `{0}` with extracted title: `{1}`", url, a.title)
          a
        case Failure(fails) =>
          trace(fails, "Failed to crawl Scribd article: " + url)
          // add counters for the failure cases and increment on matches from fails.list.foreach
          new Article
      }
    }
    else if (HuffPoLiveArticleFetcher.isHuffPoLiveUrl(url)) {
      HuffPoLiveArticleFetcher.getGooseArticle(url) match {
        case Success(a) =>
          trace("Successfully crawled HuffPoLive article: `{0}` with extracted title: `{1}`", url, a.title)
          a
        case Failure(fails) =>
          trace(fails, "Failed to crawl HuffPoLive article: " + url)
          // add counters for the failure cases and increment on matches from fails.list.foreach
          new Article
      }
    }
    else {
      val config = new Configuration()
      config.imagemagickIdentifyPath = Settings.INSIGHTS_BOOMERANG_IDENTIFY_PATH
      config.imagemagickConvertPath = Settings.INSIGHTS_BOOMERANG_CONVERT_PATH
      config.setEnableImageFetching(withImages)
      config.setHtmlFetcher(GrvHtmlFetcher)

      Extractors.getExtractorOption(url).foreach(extractors => {
        config.setPublishDateExtractor(extractors.publishDateExt)
        config.setAdditionalDataExtractor(extractors.additionalDataExt)

        extractors.contentExtractorOption.foreach(ce => config.setContentExtractor(ce))
      })

      new Goose(config).extractContent(url)
    }

    publishTimeOverride.foreach(pt => article.publishDate = pt.toDate)

    if (article == null || article.additionalData == null || article.additionalData.isEmpty) return article

    article.additionalData.getOrElse("tags", emptyString) match {
      case ignore if isNullOrEmpty(ignore) =>
      case tags => article.tags = COMMA_SPLITTER.split(tags).toSet
    }

    article.additionalData.get("image").foreach(img => article.topImage.imageSrc = img)

    article.additionalData.get("attribution.name").foreach(name => {
      // Y! News prefixes their sourced content with: LOCATION ({attributionName}) --
      // let's remove the "({attributionName}) "
      val attribName = if (name == "Associated Press") "AP" else name
      val whatToRemove = "(" + attribName + ") "
      val removeStartIdx = article.cleanedArticleText.indexOf(whatToRemove)

      if (removeStartIdx > -1) {
        val nowRemoved = article.cleanedArticleText.substring(0, removeStartIdx) + article.cleanedArticleText.substring(removeStartIdx + whatToRemove.length)

        val fixed = if (removeStartIdx == 0) {
          nowRemoved.substring(2)
        } else {
          nowRemoved
        }
        article.cleanedArticleText = fixed
      }
    })

    def appendContent(content: String) {
      if (article.cleanedArticleText.nonEmpty) {
        article.cleanedArticleText = content + "<BR>" + article.cleanedArticleText
      } else {
        article.cleanedArticleText = content
      }
    }

    article.additionalData.get("description") match {
      case Some(descr) =>
        val content = article.additionalData.get("title") match {
          case Some(title) =>
            article.title = title
            title + ": " + descr
          case None => descr
        }

        appendContent(content)
      case None =>
        article.additionalData.get("title") match {
          case Some(title) =>
            article.title = title
            appendContent(title)
          case None =>
        }
    }


    article
  }

  case class FetchedArticle(title: String, url: String, summary: String, text: String, image: String, tags: Set[String], keywords: String, meta: String)

  /**
   * Fetches and extracts a URL's content into a no-null, lib-agnostic, JSON-serializable object.
   */
  def fetchAndExtractMetadataSafe(url: String): FetchedArticle = {
    val article = fetchAndExtractMetadata(url, withImages = false)
    FetchedArticle(
      Option(article.title).getOrElse(""),
      Option(article.canonicalLink).getOrElse(""),
      extractSummary(article),
      Option(article.cleanedArticleText).getOrElse(""),
      Option(article.topImage) flatMap (img => Option(img.getImageSrc)) getOrElse "",
      Option(article.tags).getOrElse(Set.empty),
      Option(article.metaKeywords).getOrElse(""),
      Option(article.metaDescription).getOrElse("")
    )
  }

  def hasPublishDateExtractor(url: String): Boolean = Extractors.getExtractorOption(url) match {
    case Some(exts) => exts.publishDateExt != EmptyPublishDateExtractor
    case None => false
  }

  case class PartnerExtractors(publishDateExt: PublishDateExtractor, additionalDataExt: AdditionalDataExtractor, contentExtractorOption: Option[ContentExtractor] = None)

  object EmptyPublishDateExtractor extends PublishDateExtractor {
    override def extract(rootElement: Element): Date = null
  }

  object EmptyAdditionalDataExtractor extends GravityBaseAdditionalDataExtractor {
    override def customExtract(rootElement: Element): Predef.Map[String, Nothing] = Map.empty
  }

  object Extractors {

    import com.gravity.utilities.analytics.articles.ArticleWhitelist.Partners._

    val PARTNERS: Predef.Set[String] = partnerToSiteGuid.keySet

    val partnerExtractors: Map[String, PartnerExtractors] = Map(
      SCRIBD -> PartnerExtractors(ScribdPublishDateExtractor, ScribdAdditionalDataExtractor),
      WSJ -> PartnerExtractors(WsjPublishedDateExtractor, WsjAdditionalDataExtractor),
      BUZZNET -> PartnerExtractors(BuzznetPublishedDateExtractor, BuzznetAdditionalDataExtractor),
      YAHOO_NEWS -> PartnerExtractors(YahooNewsPublishedDateExtractor, YahooNewsAdditionalDataExtractor),
      YAHOO_OLYMPICS -> PartnerExtractors(YahooNewsPublishedDateExtractor, YahooNewsAdditionalDataExtractor, Some(DashTitleFirstPieceContentExtractor)),
      WESH -> PartnerExtractors(WeshPublishedDateExtractor, EmptyAdditionalDataExtractor),
      VOICES -> PartnerExtractors(VoicesPublishDateExtractor, EmptyAdditionalDataExtractor, Some(DashTitleFirstPieceContentExtractor)),
      MENSFITNESS -> PartnerExtractors(MensFitnessPublishDateExtractor, EmptyAdditionalDataExtractor, Some(DashTitleFirstPieceContentExtractor)),
      DYING_SCENE -> PartnerExtractors(DyingScenePublishDateExtractor, DyingSceneAdditionalDataExtractor, Some(new RemoveSuffixTitleCleaner(" – Dying Scene"))),
      SUITE_101 -> PartnerExtractors(Suite101PublishDateExtractor, Suite101AdditionalDataExtractor, Some(PipeTitleFirstPieceContentExtractor)),
      WALYOU -> PartnerExtractors(WalyouPublishDateExtractor, EmptyAdditionalDataExtractor, Some(PipeTitleFirstPieceContentExtractor)),
      CBS_SPORTS -> PartnerExtractors(EmptyPublishDateExtractor, CbsSportsAdditionalDataExtractor),
      WEBMD -> PartnerExtractors(EmptyPublishDateExtractor, WebMdAdditionalDataExtractor, Some(LeaveTitleAsIsContentExtractor))
    )

    def getPartnerName(url: String): Option[String] = ArticleWhitelist.getPartnerName(url)

    def getExtractorOption(url: String): Option[PartnerExtractors] = getPartnerName(url) match {

      case Some(partnerName) => partnerName match {

        case CATSTER =>
          if (url.startsWith("http://blogs.catster.com")) {
            getUrlBasedPublishDateExtractor(url, 2) match {
              case Some(pbe) => Some(PartnerExtractors(pbe, EmptyAdditionalDataExtractor))
              case None => None
            }
          } else None

        case DOGSTER =>
          if (!url.startsWith("http://blogs.dogster.com")) return None

          val feedUrl = "http://feeds.feedburner.com/dogster/TTUb"

          getExtractorsForRssBasedPartner(feedUrl, url)

        case XOJANE => getExtractorsForRssBasedPartner("http://www.xojane.com/rss", url, Some(XojaneFallbackExtractors))

        case TIME =>
          if (ArticleWhitelist.isTimeWordpressArticle(url)) {
            getUrlBasedPublishDateExtractor(url) match {
              case Some(pde) => Some(PartnerExtractors(pde, RelTagAdditionalDataExtractorForTagsOnly))
              case None => None
            }
          } else if (ArticleWhitelist.isTimeOtherArticle(url)) {
            Some(PartnerExtractors(TimeOtherPublishDateExtractor, RelTagAdditionalDataExtractorForTagsOnly))
          } else None

        case TECHCRUNCH => getUrlBasedPublishDateExtractor(url) match {
          case Some(pde) => Some(PartnerExtractors(pde, RelTagAdditionalDataExtractorForTagsOnly))
          case None => Some(PartnerExtractors(EmptyPublishDateExtractor, RelTagAdditionalDataExtractorForTagsOnly))
        }

        case CNN_MONEY => if (ArticleWhitelist.isCnnMoneyArticle(url)) {
          getUrlBasedPublishDateExtractor(url) match {
            case Some(pde) => Some(PartnerExtractors(pde, EmptyAdditionalDataExtractor))
            case None => None
          }
        } else None

        case BOSTON => if (ArticleWhitelist.isBostonArticle(url)) {
          val contentEx = if (url.startsWith("http://www.boston.com/news/health/blog/healthylifestyles/")) {
            Some(DashTitleLastPieceContentExtractor)
          } else if (url.startsWith("http://www.boston.com/sports/baseball/redsox/articles/")) {
            Some(DashTitleSecondPieceContentExtractor)
          } else {
            Some(DashTitleFirstPieceContentExtractor)
          }

          Some(PartnerExtractors(BostonPublishDateExtractor(url), EmptyAdditionalDataExtractor, contentEx))
        } else None

        case SFGATE => if (ArticleWhitelist.isSFGateArticle(url)) {
          if (url.startsWith("http://www.sfgate.com/cgi-bin/article.cgi")) {
            Some(PartnerExtractors(SFGatePublishDateExtractorForCGI, EmptyAdditionalDataExtractor))
          } else if (url.startsWith("http://blog.sfgate.com/") || url.startsWith("http://insidescoopsf.sfgate.com/blog/")) {
            Some(PartnerExtractors(SFGatePublishDateExtractorForBlogs(url), EmptyAdditionalDataExtractor, Some(PipeTitleFirstPieceContentExtractor)))
          } else None
        } else None

        case WORDPRESS => getUrlBasedPublishDateExtractor(url, -1) match {
          case Some(pde) => Some(PartnerExtractors(pde, EmptyAdditionalDataExtractor))
          case None => None
        }

        case _ => partnerExtractors.get(partnerName)
      }
      case None => None
    }

    def safeParseDate(txt: String, pattern: String): java.util.Date = {
      if (isNullOrEmpty(txt) || isNullOrEmpty(pattern)) return null

      try {
        new java.text.SimpleDateFormat(pattern).parse(txt)
      } catch {
        case ex: Exception =>
          info("text: \"%s\" could not be parsed to date with pattern: \"%s\"".format(txt, pattern))
          null
      }
    }

    val datatypeFactory: DatatypeFactory = DatatypeFactory.newInstance()
    def safeParseISO8601Date(txt: String): java.util.Date = {
      if (isNullOrEmpty(txt)) return null

      try {
        datatypeFactory.newXMLGregorianCalendar(txt).toGregorianCalendar.getTime
      } catch {
        case ex: Exception =>
          info("text: \"%s\" could not be parsed to date as it did not meet the ISO 8601 spec".format(txt))
          null
      }
    }

    def getUrlBasedPublishDateParts(url: String, howManySlashesToYear: Int = 1): Option[(Int, Int, Int)] = {
      // example url: http://swampland.time.com/2011/08/04/congress-reaches-deal-to-end-faa-shutdown/

      if (howManySlashesToYear < 0) {
        url.tryToURL.foreach(u => {
          val path = u.getPath
          val parts = tokenize(path, "/")
          if (parts.length < 3) return None

          var yyyy = -1
          var mm = -1
          var dd = -1

          for (p <- parts; if yyyy == -1 || mm == -1 || dd == -1) {
            if (yyyy < 0 && p.length == 4) {
              p.tryToInt.foreach(i => yyyy = i)
            } else if (yyyy > 1995 && mm < 0 && p.length == 2) {
              p.tryToInt.foreach(i => if (i > 0 && i < 13) {
                mm = i
              })
            } else if (yyyy > 1995 && mm > 0 && mm < 13 && dd < 0 && p.length == 2) {
              p.tryToInt.foreach(i => if (i > 0 && i < 32) {
                dd = i
              })
            }
          }

          if (yyyy > 1995 && mm > 0 && mm < 13 && dd > 0 && dd < 32) return Some((yyyy, mm, dd))

          return None
        })
      }

      val parts = tokenize(url, "/")

      val start = 2 + howManySlashesToYear

      if (parts.size < start + 2) return None

      for {
        year <- parts(start).tryToInt
        month <- parts(start + 1).tryToInt
        day <- parts(start + 2).tryToInt
      } {
        return Some((year, month, day))
      }

      None
    }

    def getUrlBasedPublishDateExtractor(url: String, howManySlashesToYear: Int = 1): Option[PublishDateExtractor] = {
      getUrlBasedPublishDateParts(url, howManySlashesToYear) match {
        case Some((year, month, day)) =>
          val publishDate = new DateTime(year, month, day, 0, 0, 0, 0)
          val pde = new PublishDateExtractor {
            override def extract(rootElement: Element): Date = publishDate.toDate
          }

          Some(pde)
        case None => None
      }
    }

    object WebMdAdditionalDataExtractor extends GravityBaseAdditionalDataExtractor {
      override def customExtract(rootElement: Element): immutable.Map[String, String] = {
        for {
          script <- Selector.select("script", rootElement)
          scriptContent <- nullOrEmptyToNone(script.html())
          (startToken, endToken) <- if (scriptContent.contains("s_topic=")) {
            Some("s_topic=" -> "\"")
          } else if (scriptContent.contains("s_topic =")) {
            Some("s_topic = " -> "';")
          } else {
            None
          }
        } {
          val padding = startToken.length + 1
          val startAt = scriptContent.indexOf(startToken) + padding
          val endAt = scriptContent.indexOf(endToken, startAt)

          if (endAt > startAt && endAt < scriptContent.length) {
            val section = scriptContent.substring(startAt, endAt).trim
            if (section.nonEmpty) return immutable.Map("section" -> section)
          }
        }

        immutable.Map.empty[String, String]
      }
    }

    object CbsSportsAdditionalDataExtractor extends GravityBaseAdditionalDataExtractor {
      val vidTitle = "div.videoTitle"
      val vidDesc = "div.videoDescription"

      override def customExtract(rootElement: Element): immutable.Map[String, String] = {
        val title = Selector.select(vidTitle, rootElement).headOption match {
          case Some(titleElem) => titleElem.text()
          case None => emptyString
        }
        val description = Selector.select(vidDesc, rootElement).headOption match {
          case Some(descrElem) => descrElem.text()
          case None => emptyString
        }

        if (title.nonEmpty && description.nonEmpty) {
          immutable.Map("title" -> title, "description" -> description)
        } else if (title.nonEmpty) {
          immutable.Map("title" -> title)
        } else if (description.nonEmpty) {
          immutable.Map("description" -> description)
        } else {
          immutable.Map.empty[String, String]
        }
      }
    }

    object SFGatePublishDateExtractorForCGI extends PublishDateExtractor {
      val selectDate = "meta[name=PUBDATE]"
      val selectTime = "meta[name=PUBTIME]"
      val dateTimePattern = "MMM dd, yyyy 'at' hh:mm a"

      override def extract(rootElem: Element): Date = {
        val metas = rootElem.getElementsByTag("meta")
        Selector.select(selectDate, metas).headOption match {
          case Some(metaDate) =>
            val dateStr = metaDate.attr("content")
            val timeStrRaw = Selector.select(selectTime, metas).headOption match {
              case Some(metaTime) => metaTime.attr("content")
              case None => ""
            }

            val timeStr = if (timeStrRaw.length() == 8) timeStrRaw else "00:00 AM"

            val dateTimeStr = dateStr + " at " + timeStr

            safeParseDate(dateTimeStr, dateTimePattern)
          case None => null
        }
      }
    }

    object SFGatePublishDateExtractorForBlogs {
      val selectorEntryDate = "span.entry-date"
      val selectorPubDate = "span.pubdate"

      val dateTimeSlashPattern = "MM/dd/yyyy 'at' hh:mm a"

      // March 28, 2012 at 9:06 am
      val dateVerbosePattern = "MMMMM d, yyyy 'at' h:mm a"

      def apply(url: String): SFGatePublishDateExtractorForBlogs = new SFGatePublishDateExtractorForBlogs(url)

      def parseDate(dateTimeStr: String): Date = {
        if (dateTimeStr.indexOf("/") > -1) {
          safeParseDate(dateTimeStr, dateTimeSlashPattern)
        } else {
          safeParseDate(dateTimeStr, dateVerbosePattern)
        }
      }
    }

    class SFGatePublishDateExtractorForBlogs(url: String) extends PublishDateExtractor {
      import com.gravity.utilities.web.ContentUtils.Extractors.SFGatePublishDateExtractorForBlogs._

      override def extract(rootElem: Element): Date = {
        Selector.select(selectorEntryDate, rootElem).headOption match {
          case Some(entryDate) =>
            val dateTimeStr = entryDate.text()

            parseDate(dateTimeStr)
          case None =>
            Selector.select(selectorPubDate, rootElem).headOption match {
              case Some(pubDate) =>
                val timeStr = pubDate.text().takeRight(8)

                getUrlBasedPublishDateParts(url, 2) match {
                  case Some((year, month, day)) =>
                    val (hour, minute) = if (timeStr.length() == 8) {
                      val isPM = timeStr.endsWith("pm")
                      val timeArray = tokenize(timeStr.take(5), ":", 2)
                      if (timeArray.length == 2) {
                        val hRaw = timeArray(0).tryToInt.getOrElse(0)
                        val m = timeArray(1).tryToInt.getOrElse(0)

                        val h = if (isPM) {
                          if (hRaw > 11) 12 else hRaw + 12
                        } else {
                          if (hRaw > 11) 0 else hRaw
                        }

                        (h, m)
                      } else (0, 0)

                    } else (0, 0)

                    try {
                      new DateTime(year, month, day, hour, minute, 0, 0).toDate
                    }
                    catch {
                      case _: org.joda.time.IllegalFieldValueException => null
                    }
                  case None => null
                }
              case None => null
            }
        }

      }

    }

    object DyingScenePublishDateExtractor extends PublishDateExtractor {
      // ('div#content div.postedBy-andDate').attr("postDate")
      val selector = "div#content div.postedBy-andDate"

      override def extract(rootElem: Element): Date = {
        Selector.select(selector, rootElem).headOption match {
          case Some(pubElem) =>
            pubElem.attr("postDate").tryToLong match {
              case Some(timestampInSeconds) =>
                // for some reason, Lucas Buck's code generates an epoch (in seconds not milliseconds) in OUR time zone
                // as backwards as this implementation looks below, it f**king works
                val dt = new DateTime(timestampInSeconds * 1000, DateTimeZone.UTC)
                val now = new DateTime()
                val areWeInDST = now.getZone.toTimeZone.inDaylightTime(now.toDate)
                println(dt)
                val fixWith = if (areWeInDST) 7 else 8
                dt.plusHours(fixWith).toDate
              case None => null
            }
          case None => null
        }
      }
    }

    object DyingSceneAdditionalDataExtractor extends TagsOverrideAdditionalDataExtractor {
      val sel = "span.tagLinks > a"

      def extractTags(rootElem: Element): Set[String] = {
        Selector.select(sel, rootElem).flatMap((e: Element) => {
          e.text() match {
            case mt if isNullOrEmpty(mt) => None
            case tag => Some(tag)
          }
        }).toSet
      }
    }

    object MensFitnessPublishDateExtractor extends PublishDateExtractor {
      val selector = "meta[name=sailthru.date]"
      val dateTimePattern = "yyyy-MM-dd"

      override def extract(rootElem: Element): Date = {
        Selector.select(selector, rootElem).headOption match {
          case Some(metaDate) =>
            val dateTimeStr = metaDate.attr("content")

            safeParseDate(dateTimeStr, dateTimePattern)
          case None => null
        }
      }
    }

    object WalyouPublishDateExtractor extends PublishDateExtractor {
      val selector = "div.metadata"
      // the actual text within this div looks like:
      // May 18th, 2012 . By Federico Lo Giudice in Geek Culture
      val daySuffixRegex: Regex = """(?<= \d{1,2})(?:st|nd|rd|th),(?= \d{4}$)""".r
      val dtPattern = "MMM dd yyyy"

      override def extract(rootElem: Element): Date = {
        Selector.select(selector, rootElem).headOption match {
          case Some(metaData) =>
            val metaText = metaData.text()
            val dateTimeStr = tokenize(metaText, " ", 3).mkString(" ")
            val dtStrClean = daySuffixRegex.replaceAllIn(dateTimeStr, emptyString)

            safeParseDate(dtStrClean, dtPattern)
          case None => null
        }
      }
    }

    class BostonPublishDateExtractor(url: String) extends PublishDateExtractor {
      def isEOMportal: Boolean = BostonPublishDateExtractor.metaDateSupportedUrlRegex.pattern.matcher(url).matches()

      override def extract(rootElem: Element): Date = {
        val bestAttempt = if (isEOMportal) {
          // what to look for: <meta name="eomportal-lastUpdate" content="Mon Nov 21 19:07:45 EST 2011"/>
          Selector.select("meta[name=eomportal-lastUpdate]", rootElem).headOption match {
            case Some(elem) =>
              val timestamp = elem.attr("content")

              safeParseDate(timestamp, "EEE MMM dd HH:mm:ss z yyyy")
            case None => Selector.select("span.pubdate", rootElem).headOption match {
              case Some(elem) => safeParseDate(elem.text(), "MMMM d, yyyy")
              case None => null
            }
          }
        } else {
          Selector.select("span#dateline", rootElem).headOption match {
            case Some(span) =>
              val spanText = span.text()
              val breakHere = spanText.indexOf("\n")
              if (breakHere > 0) {
                val timestamp = spanText.take(breakHere) + " -0500"

                safeParseDate(timestamp, "MMMM d, yyyy hh:mm a Z")
              } else {
                safeParseDate(spanText, "MMMM d, yyyy")
              }
            case None => Selector.select("span.pubdate", rootElem).headOption match {
              case Some(pubSpan) => safeParseDate(pubSpan.text(), "MMMM d, yyyy")
              case None => null
            }
          }
        }

        if (bestAttempt == null) {
          val matcher = BostonPublishDateExtractor.urlYMDregex.pattern.matcher(url)
          val ymd = if (matcher.matches() && matcher.groupCount() > 0) matcher.group(1) else null
          safeParseDate(ymd, "yyyy/MM/dd")
        } else bestAttempt

      }
    }

    object BostonPublishDateExtractor {
      val metaDateSupportedUrlRegex: Regex = """^http://(?:www.boston|boston)\.com/Boston/[a-zA-Z0-9]+/\d{4}/\d{2}/[a-zA-Z0-9_-]+/[a-zA-Z0-9]{22}/index.html.*$""".r
      val urlYMDregex: Regex = """^http://(?:www|articles)\.boston\.com/[a-zA-Z0-9_/-]+/(\d{4}/\d{2}/\d{2})/.*$""".r

      def apply(url: String): BostonPublishDateExtractor = new BostonPublishDateExtractor(url)
    }

    object VoicesPublishDateExtractor extends PublishDateExtractor {
      val dateRegex: Regex = """(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s\d{1,2},\s\d{4}""".r

      def extract(rootElem: Element): Date = {
        Selector.select("div.byline_links", rootElem).headOption match {
          case Some(elem) =>
            val text = elem.text()

            dateRegex.findFirstIn(text) match {
              case Some(dateStr) => safeParseDate(dateStr, "MMM d, yyyy")
              case None =>
                info("Failed to find the date portion of:%n\t%s%n%nusing pattern:%n\t%s%n%n".format(text, dateRegex.pattern.pattern()))
                null
            }
          case None =>
            info("Failed to find div.byline_links")
            null
        }
      }
    }

    object TimeOtherPublishDateExtractor extends SimplePublishDateExtractor("div.byline span.date", "EEEE, MMM. d, yyyy", "EEEE, MMMM d, yyyy") {
      override protected def mapDateText(text: String) = text.replace("Sept.", "Sep.")
    }

    def getExtractorsForRssBasedPartner(feedUrl: String, url: String, fallBack: Option[PartnerExtractors] = None): Some[PartnerExtractors] = Some(RssPartnerExtractors(feedUrl, url, fallBack).getExtractors)

    object XojanePublishDateExtractor extends PublishDateExtractor {
      val selector = "div.text-meta p.time"

      override def extract(rootElem: Element): Date = {
        Selector.select(selector, rootElem).headOption match {
          case Some(elem) =>
            val text: String = elem.text() match {
              case nullOrMt if isNullOrEmpty(nullOrMt) => return null
              case txtWithLink if txtWithLink.contains("|") => txtWithLink.substring(0, txtWithLink.indexOf("|")).trim()
              case txt => txt
            }
            safeParseDate(text, "MMM d, yyyy 'at' h:mma")
          case None => null
        }
      }
    }

    object XojaneAdditionalDataExtractor extends SimpleAuthorAdditionalDataExtractor("div.text-meta span.name") {
      override def customExtract(rootElem: Element): Predef.Map[String, String] = {
        val baseMap = super.customExtract(rootElem)

        val tagNodes = Selector.select("div.article-topic-list a", rootElem)

        if (tagNodes.size() > 0) {
          val tagIter = tagNodes.iterator()
          val sb = new StringBuilder

          var pastFirst = false
          while (tagIter.hasNext) {
            if (pastFirst) sb.append(",") else pastFirst = true
            val tag = tagIter.next().text()
            sb.append(tag)
          }

          baseMap ++ immutable.Map("tags" -> sb.toString())
        }

        baseMap
      }
    }

    val XojaneFallbackExtractors: PartnerExtractors = PartnerExtractors(XojanePublishDateExtractor, XojaneAdditionalDataExtractor)

    object ScribdPublishDateExtractor extends PublishDateExtractor {
      val primarySelector = "div.section_content"
      val dateNodeSelector = "div.label:contains(Uploaded:) + div.value"

      override def extract(rootElem: Element): Date = {
        val content = Selector.select(primarySelector, rootElem)
        if (content.size() == 0) return null

        val dateNode = Selector.select(dateNodeSelector, content)
        if (dateNode.size() == 0) return null

        safeParseDate(dateNode.text(), "MM/dd/yyyy")
      }
    }

    object WsjPublishedDateExtractor extends SimplePublishDateExtractor("li.dateStamp > small", "MMM dd, yyyy")

    object WeshPublishedDateExtractor extends PublishDateExtractor {
      val selector = "meta[name=lastmodified]"
      val fmtPattern = "EEE, d MMM yyyy HH:mm:ss z"

      override def extract(rootElem: Element): Date = {
        Selector.select(selector, rootElem).headOption match {
          case Some(elem) =>
            val timestamp = elem.attr("content")
            safeParseDate(timestamp, fmtPattern)
          case None => null
        }
      }
    }

    object YahooNewsPublishedDateExtractor extends PublishDateExtractor {
      val selector = "div.bd abbr[title]"
      // Special parsing required as Simple
      // 2011-11-09T23:30:44Z <-- XML Schema 1.0 ISO 8601 Date format

      override def extract(rootElem: Element): Date = {
        Selector.select(selector, rootElem).headOption match {
          case Some(elem) =>
            val timestamp = elem.attr("title")
            safeParseISO8601Date(timestamp)
          case None => null
        }
      }
    }

    object YahooNewsAdditionalDataExtractor extends GravityBaseAdditionalDataExtractor {
      val authorSelector = "cite.byline > span.fn"
      val attribNameSel = "span.provider.org" // .text()
      val attribLogoSel = "img.logo" // .attr("src")
      val attribSiteSel = "div.bd > h1.headline + a" // jQuery("div.bd > h1.headline + a").attr("href")
      val funkySiteUrlDetectionString = "**http%3A//"
      val funkyDetectLength: Int = funkySiteUrlDetectionString.length

      override def customExtract(rootElem: Element): Predef.Map[String, String] = {
        val authorMap = Selector.select(authorSelector, rootElem).headOption match {
          case Some(elem) =>
            val author = elem.text()
            if (author.isEmpty) immutable.Map.empty[String, String] else immutable.Map("author" -> author)
          case None => immutable.Map.empty[String, String]
        }

        val attribNameMap = Selector.select(attribNameSel, rootElem).headOption match {
          case Some(elem) =>
            val name = elem.text()
            if (name.isEmpty) immutable.Map.empty[String, String] else immutable.Map("attribution.name" -> name)
          case None => immutable.Map.empty[String, String]
        }

        val attribLogoMap = Selector.select(attribLogoSel, rootElem).headOption match {
          case Some(elem) =>
            val logo = elem.attr("src")
            if (logo.isEmpty) immutable.Map.empty[String, String] else immutable.Map("attribution.logo" -> logo)
          case None => immutable.Map.empty[String, String]
        }

        val attribSiteMap = Selector.select(attribSiteSel, rootElem).headOption match {
          case Some(elem) =>
            val rawSite = elem.attr("href")
            if (rawSite.isEmpty) {
              immutable.Map.empty[String, String]
            } else {
              val actualUrlStartIfFunky = rawSite.indexOf(funkySiteUrlDetectionString)

              val site = if (actualUrlStartIfFunky > 0) {
                "http://" + rawSite.substring(actualUrlStartIfFunky + funkyDetectLength)
              } else {
                rawSite
              }

              immutable.Map("attribution.site" -> site)
            }
          case None => immutable.Map.empty[String, String]
        }

        authorMap ++ attribNameMap ++ attribLogoMap ++ attribSiteMap
      }
    }

    object BuzznetPublishedDateExtractor extends PublishDateExtractor {
      val selector = "meta[name=description]"

      val SPACE_REGEX: Regex = " ".r

      override def extract(rootElem: Element): Date = {
        val bestAttempt = Selector.select(selector, rootElem).headOption match {
          case Some(elem) =>
            val description = elem.attr("content")
            if (isNullOrEmpty(description)) return null

            val descParts = SPACE_REGEX.split(description).toList
            if (descParts.length > 1) {
              val lastPiece = descParts.takeRight(1).head

              if (lastPiece.length() == 14) {
                safeParseDate(lastPiece, "yyyyMMddhhmmss")
              } else {
                null
              }
            } else null
          case None => null
        }

        if (bestAttempt == null) {
          Selector.select("span.time", rootElem).headOption match {
            case Some(timeElem) =>
              val timeStr = timeElem.text()
              safeParseDate(timeStr, "MMM dd, yyyy")
            case None => null
          }
        } else {
          bestAttempt
        }
      }
    }


    sealed class SimplePublishDateExtractor(selector: String, dateFormat: String, moreDateFormats: String*) extends PublishDateExtractor {

      override def extract(rootElem: Element): Date = {
        val dateExtractions = for {
          elem <- Selector.select(selector, rootElem).headOption.toStream
          dateFormat <- (dateFormat +: moreDateFormats).toStream
          date = safeParseDate(mapDateText(elem.text()), dateFormat)
          if date != null
        } yield date

        dateExtractions.headOption.orNull
      }

      protected def mapDateText(text: String) = text
    }

    object ScribdAdditionalDataExtractor extends SimpleAuthorAdditionalDataExtractor("a.username")

    object BuzznetAdditionalDataExtractor extends GravityBaseAdditionalDataExtractor {
      val primarySelector = "div#user-profile-box"
      val nameSelector = "a#profilebox-name"
      val genderSelector = "div ul li:matches(Male|Female)"
      val FEMALE = "FEMALE"
      val MALE = "MALE"
      val keyAuthor = "author"
      val keyAuthorLink = "authorLink"
      val keyGender = "gender"

      override def customExtract(rootElem: Element): immutable.Map[String, String] = {

        def buildMap(a: String, aLink: String = emptyString, genderOpt: Option[String] = None): immutable.Map[String, String] = {
          val authorMap = genderOpt match {
            case Some(gender) => immutable.Map(keyAuthor -> a, keyAuthorLink -> aLink, keyGender -> gender)
            case None => immutable.Map(keyAuthor -> a, keyAuthorLink -> aLink)
          }

          for {
            metaOgImage <- Selector.select("meta[property=og:image]", rootElem).headOption.toStream
            imageUrl <- metaOgImage.attr("content").tryToURL
          } {
            return authorMap ++ immutable.Map("image" -> imageUrl.toString)
          }

          authorMap
        }

        for {
          profileBox <- Selector.select(primarySelector, rootElem).headOption
          nameElem <- Selector.select(nameSelector, profileBox).headOption
        } {
          val author = nameElem.text()
          val authorLink = nameElem.absUrl("href")
          val gElem = Selector.select(genderSelector, profileBox).headOption
          if (gElem == None) return buildMap(author, authorLink)

          val genderText = gElem.get.text().toUpperCase
          if (isNullOrEmpty(genderText)) return buildMap(author, authorLink)

          if (genderText.contains(FEMALE)) return buildMap(author, authorLink, Some("F"))
          if (genderText.contains(MALE)) return buildMap(author, authorLink, Some("M"))

          return buildMap(author, authorLink)
        }

        Map.empty
      }
    }

    object WsjAdditionalDataExtractor extends SimpleAuthorAdditionalDataExtractor("h3.byline a")

    object Suite101PublishDateExtractor extends PublishDateExtractor {
      val sel = "meta[name=DC.date]" // jQuery('meta[name="DC.date"]').attr("content")
      val datePattern = "MMM d, yyyy"

      override def extract(rootElem: Element): Date = {
        Selector.select(sel, rootElem).headOption match {
          case Some(dateElem) => safeParseDate(dateElem.attr("content"), datePattern)
          case None => null
        }
      }
    }

    object Suite101AdditionalDataExtractor extends GravityBaseAdditionalDataExtractor {

      def authorMeta(rootElement: Element): mutable.HashMap[String, String] = {
        Selector.select("a[rel=author]", rootElement).headOption match {
          case Some(aElem) =>
            val authorName = aElem.text()
            val authorLink = aElem.absUrl("href")

            val data = mutable.HashMap[String, String]()

            if (!authorName.isEmpty) data += "author" -> authorName
            if (!authorLink.isEmpty) data += "authorLink" -> authorLink

            data
          case None => mutable.HashMap.empty[String, String]
        }
      }

      def image(rootElement: Element): Option[String] = Selector.select("meta[property=og:image]", rootElement).headOption match {
        case Some(imgElem) =>
          val img = imgElem.absUrl("content")
          if (img.isEmpty) {
            None
          } else {
            Some(img)
          }
        case None => None
      }

      override def customExtract(rootElement: Element): Predef.Map[String, String] = {
        val data = authorMeta(rootElement)
        image(rootElement).foreach(i => data += "image" -> i)

        if (data.isEmpty) {
          Map.empty
        } else {
          data.toMap
        }
      }
    }

    class SimpleAuthorAdditionalDataExtractor(selector: String) extends GravityBaseAdditionalDataExtractor {

      override def customExtract(rootElem: Element): Predef.Map[String, String] = {
        Selector.select(selector, rootElem).headOption match {
          case Some(elem) => immutable.Map("author" -> elem.text())
          case None => Map.empty
        }
      }
    }

    object RelTagAdditionalDataExtractorForTagsOnly extends GravityBaseAdditionalDataExtractor {
      val relTagSelector = "a[rel=tag]"

      override def customExtract(rootElement: Element): Predef.Map[String, String] = {
        val tags = Selector.select(relTagSelector, rootElement).map(_.text())
        if (tags.isEmpty) immutable.Map("tags" -> ",") else immutable.Map("tags" -> tags.mkString(","))
      }
    }

    case class RssPartnerExtractors(feedUrl: String, articleUrl: String, fallBack: Option[PartnerExtractors] = None) {
      lazy val rssItem: Option[RssItem] = try {
        RssRetriever.getRssItemFromChannelByLinkUrl(feedUrl, articleUrl)
      }
      catch {
        case ex: Exception =>
          warn(ex, "Failed to extract RSS Item from feedUrl: '%s' for articleUrl: '%s'! Will now try fallback extractors.".format(feedUrl, articleUrl))
          None
      }

      class RssPublishDateExtractor extends PublishDateExtractor {
        override def extract(rootElement: Element): Date = rssItem match {
          case Some(item) => item.pubDate.toDate
          case None => fallBack match {
            case Some(fb) => fb.publishDateExt.extract(rootElement)
            case None => null
          }
        }
      }

      class RssAdditionalDataExtractor extends GravityBaseAdditionalDataExtractor {
        override def customExtract(rootElement: Element): Predef.Map[String, String] = rssItem match {
          case Some(item) => immutable.Map("author" -> item.creator, "tags" -> item.categories.mkString(","))
          case None => fallBack match {
            case Some(fb) => fb.additionalDataExt.extract(rootElement)
            case None => Map.empty
          }
        }
      }

      def getExtractors: PartnerExtractors = PartnerExtractors(new RssPublishDateExtractor, new RssAdditionalDataExtractor)

    }

  }

  def getUrlBasedPublishDateParts(url: String): Option[(Int, Int, Int)] = Extractors.getUrlBasedPublishDateParts(url, -1)

  def getUrlBasedDateTime(url: String): Option[DateTime] = getUrlBasedPublishDateParts(url) match {
    case Some((y, m, d)) => try {
      val dm = new GrvDateMidnight(y, m, d)
      Some(dm.toDateTime)
    } catch {
      case _: Exception => None
    }
    case None => None
  }

  case class ZZ_BetterElements(orig: Elements) {
    def headOption: Option[Element] = orig.size() match {
      case n if n > 0 => Some(orig.get(0))
      case _ => None
    }

    def getOption(idx: Int): Option[Element] = orig.size() match {
      case n if n >= idx => Some(orig.get(idx))
      case _ => None
    }
  }

  /** Ensure the HTML fragment is well-formed, the opposite of Goose. */
  def validateHtmlFragment(html: String): ValidationNel[String, Elem] = validateHtmlFragment(html, None)
  def validateHtmlFragment(html: String, tagFilter: Option[HtmlTagFilter]): ValidationNel[String, Elem] = {
    try {
      val elem = xml.XML.loadString("<root>" + html + "</root>")

      tagFilter match {
        case Some(tf) => tf.validateElem(elem, validateRootNode = false)
        case None => elem.successNel
      }
    } catch {
      case saxEx: SAXParseException => saxEx.toString.failureNel
    }
  }

}

class GrvXmlLoadException(lines: Iterator[String], inner: SAXParseException) extends Exception(inner) {

  lazy val text: String = try {
    val lineNumber = inner.getLineNumber
    val preText = inner.getMessage + " :: Line " + lineNumber + " :: Column " + inner.getColumnNumber + " :: => "
    lines.toIndexedSeq.lift(lineNumber) match {
      case Some(badLine) => preText + badLine
      case None => preText + " Line number not found! Full text: " + lines.mkString("\n")
    }
  } catch {
    case ex: Exception => "Failed to read input stream to text due to: " + ex.getMessage
  }

  override def getMessage: String = {
    super.getMessage + "\nText:\n" + text
  }
}