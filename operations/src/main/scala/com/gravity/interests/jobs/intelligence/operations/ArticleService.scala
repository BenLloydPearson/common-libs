package com.gravity.interests.jobs.intelligence.operations

import com.gravity.domain.BeaconEvent
import com.gravity.domain.FieldConverters._
import com.gravity.domain.articles.{Author, AuthorData}
import com.gravity.hbase.schema._
import com.gravity.interests.graphs.graphing.PhraseTfIdfScorer
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.algorithms.{SiteSpecificGraphingAlgoResult, SiteSpecificGraphingAlgos, StoredInterestSimilarityAlgos}
import com.gravity.interests.jobs.intelligence.operations.articles.{ArticleManager, CovisitationManager}
import com.gravity.interests.jobs.intelligence.operations.graphing.{GraphArticleRemoteMessage, RegraphArticleRemoteMessage}
import com.gravity.interests.jobs.intelligence.operations.recommendations.CNNMoneyDataObj
import com.gravity.interests.jobs.intelligence.operations.sites.SiteGuidService
import com.gravity.interests.jobs.intelligence.schemas.{ArticleImage, ArticleIngestionData}
import com.gravity.ontology.{OntologyGraph2, OntologyGraphName}
import com.gravity.service.remoteoperations.{ProductionRemoteOperationsDispatcher, RemoteOperationsDispatcher}
import com.gravity.utilities.ScalaMagic.isNullOrEmpty
import com.gravity.utilities._
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.cache.LongKeySingletonCache
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvfields._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.time.DateHour
import com.gravity.utilities.web.ContentUtils
import org.joda.time.{DateTime, Days}

import scala.collection._
import scala.collection.mutable.ListBuffer
import scalaz.Scalaz._
import scalaz._

object ArticleService extends ArticleService with ProductionArticleGraphDispatcher

trait ArticleService extends ArticleManager with ArticleOperations with CovisitationManager {
  this: ArticleGraphDispatcher =>
  import com.gravity.utilities.Counters._
  import com.gravity.logging.Logging._

  override val counterCategory: String = "Article Service"

  def sendGraphRequestByDefault: Boolean = sendByDefault

  val MAX_CONTENT_LEN = 65536

  // The URL for a default article from TC
  val DEF_ARTICLE_URL = "http://techcrunch.com/2014/01/23/aol-acquires-gravity/"

  /** @see [[CampaignService.exampleCampaignRow]] a parent campaign of this example article. */
  val exampleArticleKey = ArticleKey(-9199077545046241704L)
  val exampleArticleRow = fetch(exampleArticleKey, skipCache = false)(_.withAllColumns)

  def modifyArticle(url: String, writeToWAL: Boolean = true)(putOp: ModifySpec): ValidationNel[FailureResult, ArticlePutSpec] = {
    val key = ArticleKey(url)
    for {
      spec <- modify(key, writeToWAL)(putOp)
    } yield ArticlePutSpec(key, spec, writeToWAL)
  }

  def modifyPutArticle(url: String, writeToWAL: Boolean = true)(putOp: ModifySpec): ValidationNel[FailureResult, OpsResult] = {
    val key = ArticleKey(url)
    modifyPut(key, writeToWAL)(putOp)
  }

  def dedupeArticlesByTopicSimilarity(articles: Seq[ArticleRow], percentileThreshold: Double = 0.90): Seq[ArticleRow] = {
    val st = percentileThreshold
    val results = mutable.Buffer[ArticleKey]()
    for {
      (thisArticle, thisIdx) <- articles.toIterator.zipWithIndex
      thatArticle <- articles.toIterator.drop(thisIdx + 1)
      if thisArticle.articleKey != thatArticle.articleKey
    } {
      val thisGraph = thisArticle.autoGraph
      val thatGraph = thatArticle.autoGraph
      val result = StoredInterestSimilarityAlgos.PercentileSimilarityByTopics.score(thisGraph, thatGraph).score

      if (result > st) {
        results += thatArticle.articleKey
      } else {
      }
    }

    articles.filterNot(a => results.exists(r => r.articleId == a.articleKey.articleId))

  }

  /*
  def dedupeArticlesByTitleSimilarity(articles: Seq[ArticleRow], percentileThreshold: Double = 0.90): Seq[ArticleRow] = {
    val st = percentileThreshold
    val results = Buffer[ArticleKey]()
    for {
      (thisArticle, thisIdx) <- articles.toIterator.zipWithIndex
      thatArticle <- articles.toIterator.drop(thisIdx + 1)
      if thisArticle.articleKey != thatArticle.articleKey
    } {

      val thisGraph = graphArticleRowWithSpecificGrapher(thisArticle,"TitleOnlyGrapher")
      val thatGraph = graphArticleRowWithSpecificGrapher(thatArticle,"TitleOnlyGrapher")
      val result = StoredInterestSimilarityAlgos.PercentileSimilarityByTopics.score(thisGraph, thatGraph).score

      if (result > st) {
        results += thatArticle.articleKey
      } else {
      }
    }

    articles.filterNot(a => results.exists(r => r.articleId == a.articleKey.articleId))

  }
  */

  def detectArticleType(url: String, siteGuid: String): ArticleTypes.Type = {
    CampaignIngestion.articleTypeOpinion(url, siteGuid) match {
      case Some(campIngestArtType) => campIngestArtType

      case None =>
        if (ArticleWhitelist.isValidContentArticleBySiteGuid(url, siteGuid))
          ArticleTypes.content
        else
          ArticleTypes.unknown
    }
  }

  def rejectArticles(keys: Set[ArticleKey]): ValidationNel[FailureResult, Int] = {
    withMaintenance {
      val putOpt = keys.toList match {
        case Nil => None

        case head :: Nil => Schema.Articles.put(head).value(_.doRecommendations, false).some

        case head :: tail =>
          val putThing = Schema.Articles.put(head).value(_.doRecommendations, false)
          tail.foreach(key => putThing.put(key).value(_.doRecommendations, false))
          putThing.some
      }

      putOpt match {
        case Some(putThis) => try {
          putThis.execute().numPuts.successNel
        } catch {
          case ex: Exception => FailureResult("Failed to reject articles!", ex).failureNel
        }

        case None => 0.successNel
      }
    }
  }

  /**
   * Takes a BeaconEvent and saves it as an article.
   * If the siteGuid of the article supports live recommendations, then the article will be saved to the _recentArticles family of the Site.
   * Also, the article will be dispatched for live graphing.
    *
    * @param beacon The BeaconEvent to be saved.
   * @return
   */
  def saveBeaconAsArticle(beacon: BeaconEvent, query: QuerySpec = _.withFamilies(_.meta), sendGraphingRequest: Boolean = true): ValidationNel[FailureResult, ArticleRow] = {
    for {
      articleFields <- ArticleIngestionFields.fromBeacon(beacon, ArticleTypes.content)
      articleRow <- articleFields.save(IngestionTypes.fromBeacon, beacon.href, query = query, sendGraphingRequest = sendGraphingRequest)
    } yield articleRow
  }

  def saveGooseArticle(article: com.gravity.goose.Article, siteGuid: String, query: QuerySpec = _.withFamilies(_.meta), sendGraphingRequest: Boolean = true, sectionPath: String = emptyString, ingestionSource: IngestionTypes.Type = IngestionTypes.fromBeacon, articleType: ArticleTypes.Type = ArticleTypes.unknown): ValidationNel[FailureResult, ArticleRow] = {
    // IIO's GushedCrawlSubmissionActor.validateAndGather may have misclassified an url due to a race condition
    // (if CampaignIngestion's Inc/Exc strings have recently been updated, or if CRAWLER has been updated and IIO has not),
    // so do a sanity-check fix-up here.
    val useArticleType = articleType match {
      case ArticleTypes.unknown => CampaignIngestion.articleTypeOpinion(article.finalUrl, siteGuid) match {
        case Some(campIngestArtType) if campIngestArtType.id > 0 => campIngestArtType
        case _ => articleType
      }
      case at => at
    }

    if (useArticleType != articleType)
      countPerSecond(counterCategory, s"Fixed up articleType (from $articleType to $useArticleType)")

    for {
      articleFields <- ArticleIngestionFields.fromGoose(article, siteGuid, useArticleType, sectionPath)
      articleRow <- articleFields.save(ingestionSource, sendGraphingRequest = sendGraphingRequest, query = query)
    } yield {
      if (useArticleType.id > 0)
        CampaignIngestion.addBeaconIngestedArticleToMatchingCampaigns(siteGuid, articleRow, article)

      articleRow
    }
  }

  /**
   * Will graph an article's row data and return the graph or a failure result
   */
  private def graphArticleRow(article: ArticleLike)(implicit ogName: OntologyGraphName): ValidationNel[FailureResult, SiteSpecificGraphingAlgoResult] = try {
    val result = SiteSpecificGraphingAlgos.getGrapher(article.siteGuid).graphArticleRow(article)
    if (result.autoStoredGraph.isDefined || result.conceptGraph.isDefined)
      result.successNel
    else
      FailureResult("No graph for url: " + article.url).failureNel
  }
  catch {
    case ex: Exception => FailureResult("Failed to graph article for url: " + article.url, ex).failureNel
  }

  def graphArticleRowWithSpecificGrapher(article: ArticleLike, grapher: String)(implicit ogName: OntologyGraphName): ValidationNel[FailureResult, SiteSpecificGraphingAlgoResult] = try {
    val result = SiteSpecificGraphingAlgos.getGrapher(grapher).graphArticleRow(article)
    if (result.autoStoredGraph.isDefined || result.conceptGraph.isDefined)
      result.successNel
    else
      FailureResult("No graph for url: " + article.url).failureNel
  }
  catch {
    case ex: Exception => FailureResult("Failed to graph article for url: " + article.url, ex).failureNel
  }

  def addGraphMessage(articleKey: ArticleKey, message: String) {

    modifyPut(articleKey)(_.value(_.lastGraphedMessage, message)) match {
      case Success(_) =>
      case Failure(fails) => warn(fails, "Unable to put lastGraphedMessage `" + message + "` for articleId: " + articleKey.articleId)
    }
  }

  /**
   * Handy dandy method to (re)graph any article by its key.  Will return Failure if any unexpected circumstances are met, such as the article possessing insufficient metadata.
   * If the article simply doesn't have enough content to graph, the result will be a Success but with an empty StoredGraph instance.
   */
  def graphArticle(articleKey: ArticleKey)(implicit ogName: OntologyGraphName): ValidationNel[FailureResult, StoredGraph] = {
    for {
      article <- fetch(articleKey)(_.withFamilies(_.meta, _.text))
      conceptGraph <- graphAndSaveArticleRow(article)
    } yield {
      dispatchArticleIndexRequestViaPersistentQueue(article.url)
      conceptGraph
    }
  }

  /**
   * Will graph an article and save the results to the database.
   */
  def graphAndSaveArticleRow(article: ArticleRow)(implicit ogName: OntologyGraphName): ValidationNel[FailureResult, StoredGraph] = {
    for {
      graphRes <- graphArticleRow(article)
      conceptGraph <- graphRes.conceptGraph.toValidationNel(FailureResult("Failed to get conceptGraph from graphing algo result!"))
      _ <- modifyPut(article.articleKey) {
        putter => {
          putter.value(_.ontologyDate, OntologyGraph2.graph.ontologyCreationDate)
          putter.value(_.conceptStoredGraph, conceptGraph)
          graphRes.phraseConceptGraph.foreach(putter.value(_.phraseConceptGraph, _))
          graphRes.autoStoredGraph.foreach(putter.value(_.autoStoredGraph, _))
          putter
        }
      }
    } yield conceptGraph
  }

  def saveAndGraphAsArticle(beacon: BeaconEvent)(implicit ogName: OntologyGraphName): ValidationNel[FailureResult, StoredGraph] = {
    for {
      article <- saveBeaconAsArticle(beacon, _.withFamilies(_.meta, _.text))
      autoGraph <- graphAndSaveArticleRow(article)
    } yield autoGraph
  }

  /**
   * Turns an ArticleRow into an ArticleRowDomain, which is required for graphing.
   * @param strictChecking If true, None will be returned for things without required data.
   * @return
   */
  def articleRowToArticleRowDomain(result: ArticleRow, strictChecking: Boolean = true): Option[ArticleRowDomainDoNotUseAnymorePlease] = {
    for {
      meta <- articleRowToArticleMeta(result, strictChecking)
      text <- articleRowToArticleText(result)
    } yield ArticleRowDomainDoNotUseAnymorePlease(meta, text)
  }

  /**
   * Turns an ArticleRow into an ArticleMeta object.  Will return none if there is no Url, no BeaconTime, no PublishTime (if requirePublishDate is set), or no SiteGuid.
   * @param strictChecking If true, will return None if there is no PublishTime field.
   * @return
   */
  def articleRowToArticleMeta(result: ArticleRow, strictChecking: Boolean = true): Option[ArticleMeta] = {
    val urlOption = result.column(_.url)
    val beaconTimeOption = result.column(_.beaconTime)
    val publishDateOption = result.column(_.publishTime)
    val siteGuidOption = result.column(_.siteGuid)

    //The items are individually checked to ease debugging
    if (urlOption.isEmpty) {
      //      println("No URL")
      None
    } else if (strictChecking && beaconTimeOption.isEmpty) {
      //      println("No Beacon Time")
      None
    } else if (strictChecking && publishDateOption.isEmpty) {
      //      println("No Publish Date")

      None
    } else if (siteGuidOption.isEmpty) {
      //      println("No site guid")

      None
    } else {
      val url = result.column(_.url).get
      val beaconTime = result.column(_.beaconTime).getOrElse(new DateTime(1, 1, 1, 1, 1, 1, 1))
      val publishDate = result.column(_.publishTime).getOrElse(new DateTime(1, 1, 1, 1, 1, 1, 1))
      val siteGuid = result.column(_.siteGuid).get
      val title = result.column(_.title).getOrElse(url)
      val author = result.column(_.author).getOrElse(emptyString)
      val image = result.column(_.image).getOrElse(emptyString)
      val behindPaywall = result.column(_.behindPaywall).getOrElse(false)
      val category = result.column(_.category).getOrElse(emptyString)
      val altTitle = result.column(_.altTitle).getOrElse(emptyString)
      val imageList = result.column(_.images).orNull
      val metaLink = result.column(_.metaLink).orNull
      val termVector1 = result.column(_.termVector).orNull
      val termVector2 = result.column(_.termVector2).orNull
      val termVector3 = result.column(_.termVector3).orNull
      val termVectorG = result.column(_.termVectorG).orNull
      val phraseVectorKea = result.column(_.phraseVectorKea).orNull
      val relegenceStoryId   = result.relegenceStoryId
      val relegenceStoryInfo = result.relegenceStoryInfo
      val relegenceEntities  = result.relegenceEntities
      val relegenceSubjects  = result.relegenceSubjects

      val customCnnDataObject = null

      Some(ArticleMeta(url, beaconTime, title, author, siteGuid, publishDate, image, category, behindPaywall, altTitle, metaLink, imageList,
        TermVectors(termVector1, termVector2, termVector3, termVectorG, phraseVectorKea),
        customCnnDataObject, publisher = result.publisher,
        relegenceStoryId = relegenceStoryId, relegenceStoryInfo = relegenceStoryInfo, relegenceEntities = relegenceEntities, relegenceSubjects = relegenceSubjects))
    }
  }

  def articleRowToArticleText(result: ArticleRow): Option[ArticleText] = {
    if (result.result.isEmpty) return None

    val tags = result.column(_.tags) match {
      case Some(ts) => ts.items
      case None => Set.empty[String]
    }
    Some(ArticleText(
      result.column(_.keywords).getOrElse(emptyString),
      result.column(_.content).getOrElse(emptyString),
      tags,
      result.column(_.summary).getOrElse(emptyString)
    ))
  }

  def fetchArticleGrvMap(articleKey: ArticleKey, oneScopedKey: ArtGrvMap.OneScopeKey, skipCache: Boolean = true): ValidationNel[FailureResult, ArtGrvMap.OneScopeMap] = {
    fetchOrEmptyRow(articleKey, skipCache)(_.withColumn(_.allArtGrvMap, oneScopedKey)).flatMap(_.columnFromFamily(_.allArtGrvMap, oneScopedKey).getOrElse(ArtGrvMap.emptyOneScopeMap).successNel)
  }

  def saveArticle(url: String, sourceSiteGuid: String, ingestionSource: IngestionTypes.Type,
                  receivedTime: DateTime = grvtime.currentTime, ingestionNotes: String = emptyString,
                  sendGraphingRequest: Boolean = true, writeToWAL: Boolean = true,
                  articleType: ArticleTypes.Type = ArticleTypes.content,
                  publishDateOption: Option[DateTime] = None, sectionPathOption: Option[SectionPath] = None, isOrganicSource: Boolean = true)(putSpec: ModifySpec, query: QuerySpec = _.withFamilies(_.meta)): ValidationNel[FailureResult, ArticleRow] = {

    trace("saveArticle(url: \"{0}\", sourceSiteGuid: \"{1}\", ingestionSource: \"{2}\"...)", url, sourceSiteGuid, ingestionSource.toString)

    var liveNotes = ingestionNotes

    // We really should only trust the publish date that comes from RSS, but if we get a date that's older, then it's safe to use it
    def trustedPublishTimeOption = publishDateOption match {
      case sPublishTime@Some(publishTime) if ingestionSource == IngestionTypes.fromRss => sPublishTime
      case sPublishTime@Some(publishTime) if ingestionSource == IngestionTypes.fromFirehose => sPublishTime
      case sNotSoSure@Some(notSoSurePublishTime) =>
        fetch(ArticleKey(url), skipCache = false)(_.withColumns(_.publishTime, _.url)) match {
          case Success(aRow) => aRow.column(_.publishTime) match {
            case sExisting@Some(existingPublishTime) => if (existingPublishTime.isBefore(notSoSurePublishTime)) {
              sExisting
            } else {
              sNotSoSure
            }
            case None => sNotSoSure
          }
          case Failure(fails) =>
            val msg = "Failed to lookup existing publishTime for URL: " + url
            liveNotes += " :: WITH CAUGHT FAILURE :: " + msg
            info(fails, msg)
            sNotSoSure
        }
      case _ => None
    }

    for {
      _ <- validateUrlAndSiteGuid(url, sourceSiteGuid)
      articlePutStuff <- modifyArticle(url, writeToWAL)(putSpec)
      (useThisPublishTimeOption, completePutSpec) = {
        val pubTimeOpt = trustedPublishTimeOption match {
          case sPubTime@Some(pd) =>
            articlePutStuff.spec.value(_.publishTime, pd)
              .valueMap(_.standardMetricsHourlyOld, Map(DateHour(pd) -> StandardMetrics.OnePublish))
            sPubTime
          case None => None
        }
        // be sure that our known values are staged within this PUT.
        val ingestionKey = ArticleIngestionKey(receivedTime, ingestionSource)
        val ingestionData = ArticleIngestionData(sourceSiteGuid, liveNotes, articleType, isOrganicSource)

        for {
          sectionPath <- sectionPathOption
          if sectionPath.nonEmpty
        } {
          articlePutStuff.spec.valueMap(_.siteSections, Map(SiteKey(sourceSiteGuid) -> sectionPath.paths))
        }

        val putSpec = articlePutStuff.spec.value(_.url, url).value(_.siteGuid, sourceSiteGuid).value(_.beaconTime, receivedTime).valueMap(_.ingestedData, Map(ingestionKey -> ingestionData))

        (pubTimeOpt, putSpec)
      }
      _ <- put(completePutSpec)
      _ <- useThisPublishTimeOption match {
        case Some(pd) if articleType.id > 0 =>
          val destinations = if (isOrganicSource) RecentArticlesDestinations.fromOrganic else RecentArticlesDestinations.fromNonOrganic
          addToRecentArticlesIfSupportedAndRecent(sourceSiteGuid, url, pd, sectionPathOption, destinations, ingestionSource)
          true.successNel
        case _ => false.successNel
      }
      _ = CampaignRecoRequirements.reevaluateForArticle(articlePutStuff.key)
      row <- fetch(articlePutStuff.key)(query)
    } yield {
      if (sendGraphingRequest) {
        trace("Attempting to call `dispatchArticleGraphRequest` after successful article save for url: " + url)
        //dispatchArticleGraphRequest(articlePutStuff.key)
        dispatchArticleRegraphRequestViaPersistentQueue(url)
      } else {
        trace("Skipping `dispatchArticleGraphRequest` after successful article save for url: " + url)
      }
      row
    }
  }

  def generateArticleKeyAndPut(beacon: BeaconEvent): ValidationNel[FailureResult, (ArticleKey, PutSpec)] = {
    for {
      articleFields <- ArticleIngestionFields.fromBeacon(beacon, ArticleTypes.content)
      articlePut <- articleFields.generatePut(IngestionTypes.fromBeacon, beacon.toDelimitedFieldString(), writeToWAL = false)
    } yield articlePut.key -> articlePut.spec
  }

  private def validateUrlAndSiteGuid(url: String, siteGuid: String): ValidationNel[FailureResult, (String, String)] = {
    def validateUrl: ValidationNel[FailureResult, String] = if (url.tryToURL.isEmpty) {
      FailureResult("Invalid URL: " + url).failureNel
    } else {
      url.successNel
    }

    def validateSiteGuid: ValidationNel[FailureResult, String] = if (isNullOrEmpty(siteGuid) || siteGuid.length != 32) {
      FailureResult("Invalid siteGuid: " + siteGuid).failureNel
    } else {
      siteGuid.successNel
    }

    (validateUrl |@| validateSiteGuid) {
      (validUrl: String, validSiteGuid: String) => validUrl -> validSiteGuid
    }
  }

  def addToRecentArticlesIfSupportedAndRecent(siteGuid: String, url: String, publishDate: DateTime, sectionPath: Option[SectionPath], destinations: RecentArticlesDestinations, ingestionType: IngestionTypes.Type): ValidationNel[FailureResult, RecentArticlePutResult] = {
    if (isNullOrEmpty(siteGuid)) return FailureResult("siteGuid must be non-empty!").failureNel

    addToRecentArticlesIfSupportedAndRecent(SiteKey(siteGuid), url, publishDate, sectionPath, destinations, ingestionType)
  }

  val trueSuccessNel: ValidationNel[FailureResult, Boolean] = true.successNel[FailureResult]
  val falseSuccessNel: ValidationNel[FailureResult, Boolean] = false.successNel[FailureResult]

  def addToRecentArticlesIfSupportedAndRecent(sk: SiteKey, url: String, publishDate: DateTime, sectionPath: Option[SectionPath], destinations: RecentArticlesDestinations, ingestionType: IngestionTypes.Type): ValidationNel[FailureResult, RecentArticlePutResult] = {
    if (isNullOrEmpty(url)) return FailureResult("url must be non-empty!").failureNel

    if (!SiteService.supportsRecentArticles(sk)) return RecentArticlePutResult.emptySuccessNel

    // If we have an RSS-enabled site, but this article didn't come from an RSS ingestion source, then don't add it to recent articles
    if (SiteService.siteMeta(sk).exists(_.isRssIngested && ingestionType != IngestionTypes.fromRss)) {
      countPerSecond(counterCategory, "Non-RSS articles skipped for RSS-enabled site")
      return RecentArticlePutResult.emptySuccessNel
    }

    if (math.abs(Days.daysBetween(new DateTime(), publishDate).getDays) > 7) return RecentArticlePutResult.emptySuccessNel

    val ak = ArticleKey(url)
    val recentArticles = Map(PublishDateAndArticleKey(publishDate, ak) -> ak)

    val siteRes = if (destinations.toSites) {
      SiteService.modifyPut(sk)(_.valueMap(_.recentArticles, recentArticles)) match {
        case Success(ops) if ops.numPuts > 0 =>
          countPerSecond(counterCategory, "Recent Article Added to Site", ops.numPuts)
          trueSuccessNel
        case Failure(fails) =>
          warn(fails, "Failed to write recent article `" + url + "` for siteId: " + sk.siteId)
          fails.failure[Boolean]
        case _ => falseSuccessNel
      }
    } else {
      falseSuccessNel
    }

    val sectionRes = if (destinations.toSections) {
      (for {
        sp <- sectionPath.toIterable
        sectionKey <- sp.sectionKeys(sk)
      } yield {
        sectionKey -> SectionService.modifyPut(sectionKey)(_.valueMap(_.recentArticles, recentArticles))
      }).headOption match {
        case Some((sectKey, sectRes)) => sectRes match {
          case Success(ops) if ops.numPuts > 0 =>
            countPerSecond(counterCategory, "Recent Article Added to Section", ops.numPuts)
            trueSuccessNel
          case Failure(fails) =>
            warn(fails, "Failed to write recent article `" + url + "` for sectionKey: " + sectKey)
            fails.failure[Boolean]
          case _ => falseSuccessNel
        }
        case None => falseSuccessNel
      }
    } else {
      falseSuccessNel
    }

    val campRes = if (destinations.toCampaigns) CampaignService.addRecentArticle(ak, recentArticles) else falseSuccessNel

    RecentArticlePutResult.fromSeparateResults(siteRes, sectionRes, campRes)
  }

  def getArticleRow(url: String): ValidationNel[FailureResult, ArticleRow] = getArticleRow(ArticleKey(url))

  def getArticleRow(key: ArticleKey): ValidationNel[FailureResult, ArticleRow] = fetch(key)(_.withFamilies(_.meta, _.text))

  def getArticleRows(keys: Seq[ArticleKey]): ValidationNel[FailureResult, scala.Seq[ArticleRow]] = fetchMultiOrdered(keys)(_.withColumn(_.siteGuid).withFamilies(_.meta, _.text, _.allArtGrvMap))

  def titleAndKeywordGraphArticles(articles: List[ArticleRow], siteGuid: String)(implicit ogName: OntologyGraphName): StoredGraph = {

    val siteGrapher = SiteSpecificGraphingAlgos.getGrapher(siteGuid)
    var sg = StoredGraph.makeEmptyGraph
    for (articleRow <- articles;
         result = siteGrapher.graphArticleRow(articleRow);
         cg <- result.conceptGraph) {
      sg = sg.plusOne(cg)
    }
    sg
  }


  def tagKeywordGraphArticle(article: ArticleLike, minTfidfScore: Double = 0.5): Option[StoredGraph] = {
    val tags = article.tags.items.map(_.trim).filter(_ != "")
    val keywords = article.keywords.split(",").map(_.trim).filter(_ != "").toSet


    val tokens = tags ++ keywords

    tagGraphArticles(List(article), tokens, minTfidfScore)
  }


  def tagGraphArticle(article: ArticleLike, minTfidfScore: Double = 0.5): Option[StoredGraph] = {
    val tokens = article.tags.items.map(_.trim)
    tagGraphArticles(List(article), tokens, minTfidfScore)
  }

  def tagGraphArticles(articles: List[ArticleLike], tokens: Set[String], minTfidfScore: Double = 0.5): Option[StoredGraph] = {

    def getTagGraph(article: ArticleLike): StoredGraph = {
      val URI_PREFIX = ""
      val articleTags = ListBuffer[String]()
//      val keywords = article.keywords.split(",").map(_.trim)
      if (tokens.nonEmpty) {
        articleTags ++= tokens
      }
//      else {
//        articleTags ++= keywords
//      }

      val builder = StoredGraph.builderFrom(StoredGraph.makeEmptyGraph)
      val filteredTags = articleTags.map(_.toLowerCase)

      val tagCountMap = mutable.HashMap[String, Int]()

      filteredTags.foreach(
        tag => {
          tagCountMap(tag) = tagCountMap.getOrElse(tag, 0) + 1
        }
      )

      val phraseScorer = new PhraseTfIdfScorer(tagCountMap)
      phraseScorer.tfidScoreMap.filter(phraseScore => phraseScore._2 >= minTfidfScore).foreach{
        case (tagName, tagTfidfScore) =>
          builder.addNode(
            URI_PREFIX + tagName,
            tagName,
            NodeType.Term,
            100,
            tagCountMap(tagName),
            tagTfidfScore)
      }

      builder.build
    }


    var sg = StoredGraph.makeEmptyGraph
    for (articleRow <- articles;
         tg = getTagGraph(articleRow)) {
      sg = sg.plusOne(tg)
    }

    sg.some
  }
}

trait ArticleGraphDispatcher extends RemoteOperationsDispatcher {
  def sendByDefault: Boolean

  def dispatchArticleGraphRequest(articleKey: ArticleKey)

  def dispatchArticleRegraphRequestViaPersistentQueue(url: String)

  def dispatchArticleRegraphRequest(articleKey: ArticleKey)

  def dispatchArticleIndexRequestViaPersistentQueue(url: String)

  def dispatchArticleDeleteFromIndexRequestViaPersistentQueue(url: String)

}

trait ProductionArticleGraphDispatcher extends ArticleGraphDispatcher with ProductionRemoteOperationsDispatcher {
  import com.gravity.utilities.Counters._

  def sendByDefault: Boolean = true

//  val ctr_RemoteGraphsDispatched: Counter = new Counter("Remote Graph Requests Dispatched", "ArticleService", false, CounterType.PER_SECOND)
//  val ctr_RemoteGraphsFailed: Counter = new Counter("Remote Graph Requests Failed", "ArticleService", false, CounterType.PER_SECOND)
//  val ctr_RemoteArticleIndexesDispatched: Counter = new Counter("Remote Article Indexes Requests Dispatched", "ArticleService", false, CounterType.PER_SECOND)
//  val ctr_RemoteArticleRemoveIndexesDispatched: Counter = new Counter("Remote Article De-Indexes Requests Dispatched", "ArticleService", false, CounterType.PER_SECOND)

  lazy val indexQueueProducer: ContentQueueProducer = new ContentQueueProducer("solr.articles")
  lazy val deindexQueueProducer: ContentQueueProducer = new ContentQueueProducer("solr.articles.delete")
  lazy val regraphQueueProducer: ContentQueueProducer = new ContentQueueProducer("RegraphArticleRequest")

  /**
   * Dispatches a remote graphing request to ONTOLOGY_MANAGEMENT.
    *
    * @param articleKey just as it says
   */
  def dispatchArticleGraphRequest(articleKey: ArticleKey) {
    sendRemoteMessage(GraphArticleRemoteMessage(articleKey)).foreach {
      case Success(success) =>
      //ctr_RemoteGraphsDispatched.increment
      case Failure(fail) =>
       // ArticleService.ctr_RemoteGraphsFailed.increment
        fail.printError()
    }
  }

  /**
   * Dispatches a remote graphing request to ONTOLOGY_MANAGEMENT via a more robust persistent messaging queue.
    *
    * @param url url... duh
   */
  def dispatchArticleRegraphRequestViaPersistentQueue(url: String) {
    // Only submit regraph request if we haven't for this URL in our allotted time
    if (RegraphUrlTempCache.cacheUrlAndReturnIfExisted(ArticleKey(url))) return

    val msg = url + "<GRV>0"
    regraphQueueProducer.send(msg)
    countPerSecond("Graphing", "regraph.reqs.sent")
  }

  /**
   * Dispatches a remote re-graphing request to ONTOLOGY_MANAGEMENT.
    *
    * @param articleKey just as it says
   */
  def dispatchArticleRegraphRequest(articleKey: ArticleKey) {
    sendRemoteMessage(RegraphArticleRemoteMessage(articleKey)).foreach {
      case Success(success) =>
        //ctr_RemoteGraphsDispatched.increment
      case Failure(fail) =>
        //ctr_RemoteGraphsFailed.increment
        fail.printError()
    }
  }

  /**
   * Dispatches a remote article indexing request to SEARCH_INDEXING.
    *
    * @param url just as it says
   */
  def dispatchArticleIndexRequestViaPersistentQueue(url: String) {
    // TJC: Currently disabled, as there is no active consumer of these persistant messages.

//    val msg = url + "<GRV>0"
//    indexQueueProducer.send(msg)
//    ctr_RemoteArticleIndexesDispatched.increment
  }

  def dispatchArticleDeleteFromIndexRequestViaPersistentQueue(url: String) {
    // TJC: Currently disabled, as there is no active consumer of these persistant messages.

//    val msg = url + "<GRV>0"
//    deindexQueueProducer.send(msg)
//    ctr_RemoteArticleRemoveIndexesDispatched.increment
  }


}

case class OptionalArticleFields(title: Option[String], publishTime: Option[DateTime], image: Option[String], text: Option[String],
                                 author: Option[String], authorLink: Option[String], attributionSite: Option[String], attributionName: Option[String],
                                 attributionLogo: Option[String], behindPaywall: Option[Boolean], summary: Option[String], keywords: Option[String],
                                 category: Option[String], tags: Set[String] = Set.empty[String], images: Seq[ArticleImage] = Seq.empty[ArticleImage],
                                 cnnData: Option[CNNMoneyDataObj] = None, sectionPath: Option[SectionPath] = None) {
  def authorDataOption: Option[AuthorData] = author.map(authorName => AuthorData(Author(authorName, authorLink)))
}

object OptionalArticleFields {
  val empty: OptionalArticleFields = OptionalArticleFields(None, None, None, None, None, None, None, None, None, None, None, None, None)
}

case class ArticleIngestionFields(url: String, siteGuid: String, receivedTime: DateTime, articleType: ArticleTypes.Type, optionalFields: OptionalArticleFields) {
  val articleKey: ArticleKey = ArticleKey(url)

  lazy val virtualSiteGuid: String = SiteGuidService.findOrGenerateSiteGuidByUrl(url) match {
    case Success(sg) => sg
    case Failure(_) => siteGuid
  }

  private def linkCheck(input: Option[String]): Option[String] = for {
    link <- input
    _ <- link.tryToURL
  } yield link

  def generatePut(ingestionSource: IngestionTypes.Type, ingestionNotes: String = emptyString, writeToWAL: Boolean = true): ValidationNel[FailureResult, ArticlePutSpec] = {
    ArticleService.modifyArticle(url, writeToWAL)(modder => {
      modder.value(_.url, url)
      modder.value(_.siteGuid, siteGuid)
      modder.value(_.beaconTime, receivedTime)
      modder.value(_.virtualSiteGuid, virtualSiteGuid)
      optionalFields.title.foreach(modder.value(_.title, _))
      optionalFields.image.foreach(imgUrl => {
        ImageWritingMonitor.noteWritingArticleImage(url, imgUrl)    // Low-Level generatePut used by Beacons.

        modder.value(_.image, imgUrl)
      })
      optionalFields.text.foreach(txt => modder.value(_.content, truncStringTo(txt, ArticleService.MAX_CONTENT_LEN)))
      optionalFields.attributionName.foreach(modder.value(_.attributionName, _))
      optionalFields.attributionSite.foreach(modder.value(_.attributionSite, _))
      linkCheck(optionalFields.attributionLogo).foreach(modder.value(_.attributionLogo, _))
      optionalFields.authorDataOption.foreach(modder.value(_.authorData, _))
      linkCheck(optionalFields.authorLink).foreach(modder.value(_.authorLink, _))
      optionalFields.behindPaywall.foreach(modder.value(_.behindPaywall, _))
      optionalFields.summary.foreach(modder.value(_.summary, _))
      optionalFields.keywords.foreach(modder.value(_.keywords, _))
      optionalFields.category.foreach(modder.value(_.category, _))

      if (optionalFields.tags.nonEmpty) modder.value(_.tags, CommaSet(optionalFields.tags))
      if (optionalFields.images.nonEmpty) modder.value(_.images, optionalFields.images)

      modder
    })
  }

  def save(ingestionSource: IngestionTypes.Type, ingestionNotes: String = emptyString, sendGraphingRequest: Boolean = true, query: ArticleService.QuerySpec = _.withFamilies(_.meta), writeToWAL: Boolean = true): ValidationNel[FailureResult, ArticleRow] = {
    val articlePutSpec = generatePut(ingestionSource, ingestionNotes, writeToWAL) match {
      case Success(p) => p
      case Failure(fails) => return fails.failure
    }

    ArticleService.saveArticle(url, siteGuid, ingestionSource, receivedTime, ingestionNotes, sendGraphingRequest,
      writeToWAL = articlePutSpec.writeToWAL, articleType = articleType,
      publishDateOption = optionalFields.publishTime, sectionPathOption = optionalFields.sectionPath)(_ => articlePutSpec.spec, query)
  }
}

trait ImageBlacklister {
 import com.gravity.logging.Logging._
  // We blacklist images from these domains because they frequently are something like a 1x1 transparent image
  // that is only there for e.g. doubleclick's ad-tracking purposes.
  val blacklistedImageDomainSuffixes: Set[String] = Set(
    "feedburner.com",
    "doubleclick.net",
    "feedsportal.com",
    "buysellads.com",
    "gravatar.com"
  )

  def blacklistedSuffixesMatchesUrl(url: java.net.URL): Boolean = {
    val auth = url.getAuthority

    if (auth == null) {
      // One example where this happens is in www.refinery29.com's RSS feed, which can have URLs like this:
      // http:\/\/s3.r29static.com/bin/entry/ecb/x,80/1263664/vesperpag.jpg
      // But the browser still renders it fine, so let's be paranoid and blacklist it, since it was opaque to us.
      warn(s"Blacklisting weird url=`$url`")
      true
    } else {
      val host = auth.toLowerCase
      for (suffix <- blacklistedImageDomainSuffixes) {
        if (host.endsWith(suffix)) return true
      }
      false
    }
  }

  val blackListByDomainSuffixes: (String) => Boolean = _.tryToURL match {
    case Some(url) => blacklistedSuffixesMatchesUrl(url)
    case None => true
  }

  val blacklistCheckers: Seq[(String) => Boolean] = Seq(
    blackListByDomainSuffixes,
    _.endsWith(".psd"),   // Adobe PhotoShop Document
    _.startsWith("http://feeds.wordpress.com/1.0/comments/tctechcrunch2011.wordpress.com/"),
    url => url.startsWith("http://rss.nytimes.com") && url.endsWith("mf.gif"),
    _.startsWith("http://www.washingtonpost.com/pb/resources/img/twp-")  // Washington Post masthead, e.g. "http://www.washingtonpost.com/pb/resources/img/twp-2048x1024.jpg"
  )

  def isImageUrlBlacklisted(imageUrlIn: String): Boolean = {
    // A little paranoia doesn't hurt here -- nulls are bad.
    if (imageUrlIn == null) {
      true
    } else {
      // If we were given a cached-image URL, get back the original human-image URL.
      val imageUrlStr = ImageCachingService.asHumanImgStr(imageUrlIn.toLowerCase)

      // The url is blacklisted if any of the checkers don't like it.
      blacklistCheckers.exists(checker => checker(imageUrlStr))
    }
  }
}

class WritingBlacklistedImageException extends Exception("WritingBlacklistedImageFailureResult call trace")

class WritingBlacklistedImageFailureResult(artUrlOrId: String, imgUrl: String)
  extends FailureResult(s"Writing blacklisted image `$imgUrl` to `$artUrlOrId`", new WritingBlacklistedImageException().some)

object ImageWritingMonitor extends ImageBlacklister {
  import com.gravity.logging.Logging._

  // Examine any writes to ArticlesTable.image for suspiciousness -- trying to track down where some Role is
  // updating WaPo article images to have their generic masthead image.
  def noteWritingArticleImage(artUrlOrId: String, imgUrl: String) {
    // We will monitor any write where the artUrlOrId is null, or doesn't appear to be an URL, or if it looks like a WaPo URL.
    val checkIt = if (artUrlOrId == null)
      true
    else {
      val lowArt = artUrlOrId.toLowerCase

      !lowArt.startsWith("http") || lowArt.findIndex("washingtonpost.com").isDefined
    }

    if (checkIt) {
      if (imgUrl != null && imgUrl != "" && isImageUrlBlacklisted(imgUrl)) {
        // Warnings go to logstash.  We should see the Role and the call trace.
        warn(new WritingBlacklistedImageFailureResult(artUrlOrId, imgUrl))
      }
    }
  }
}

object ArticleIngestionFields extends ImageBlacklister {
  def validateUrl(url: String): ValidationNel[FailureResult, String] = if (url.tryToURL.isEmpty) {
    FailureResult("Invalid URL: " + url).failureNel
  } else {
    url.successNel
  }

  def validateSiteGuid(siteGuid: String): ValidationNel[FailureResult, String] = if (isNullOrEmpty(siteGuid) || siteGuid.length != 32) {
    FailureResult("Invalid siteGuid: " + siteGuid).failureNel
  } else {
    siteGuid.successNel
  }

  def fromBeacon(beacon: BeaconEvent, articleType: ArticleTypes.Type): ValidationNel[FailureResult, ArticleIngestionFields] = {
    for {
      url <- beacon.standardizedUrl.toValidationNel(FailureResult("beacon missing URL! beacon string: " + beacon.toDelimitedFieldString))
      _ <- validateUrl(url)
      siteGuid <- beacon.siteGuidOpt.toValidationNel(FailureResult("beacon missing siteGuid! beacon string: " + beacon.toDelimitedFieldString))
      _ <- validateSiteGuid(siteGuid)
      beaconTime = beacon.timestampDate
    } yield {
      val optionalFields = OptionalArticleFields(
        nullOrEmptyToNone(beacon.articleTitle),
        beacon.getPublishedDateOption,
        beacon.image.tryToURL.flatMap(_ =>
          if (isImageUrlBlacklisted(beacon.image))
            None
          else
            Some(beacon.image)),
        nullOrEmptyToNone(beacon.payloadField),
        nullOrEmptyToNone(beacon.authorName),
        None,
        None,
        None,
        None,
        None,
        nullOrEmptyToNone(beacon.articleSummary),
        nullOrEmptyToNone(beacon.articleKeywords),
        nullOrEmptyToNone(beacon.articleCategories),
        beacon.articleTags.splitBetter(",").toSet,
        Seq.empty[ArticleImage], //always empty in current flow
        None,
        SectionPath.fromParam(beacon.sectionId)
      )

      ArticleIngestionFields(url, siteGuid, beaconTime, articleType, optionalFields)
    }
  }

  def fromGoose(article: com.gravity.goose.Article, siteGuid: String, articleType: ArticleTypes.Type, sectionPath: String = emptyString): ValidationNel[FailureResult, ArticleIngestionFields] = {
    validateSiteGuid(siteGuid) match {
      case Success(_) =>
      case Failure(fails) => return fails.failure
    }

    val url = if (article.canonicalLink.isEmpty) {
      return FailureResult("com.gravity.goose.Article#canonicalLink must be non-empty!").failureNel
    } else if (article.canonicalLink.tryToURL.isEmpty) {
      // we may have a relative canonical link, so let's try to resolve it against the article finalUrl
      val resolvedCanonicalLink = try {
        article.finalUrl.tryToURL.map(url => {
          url.toURI.resolve(
            // URI is finicky, it expects a trailing slash on a host-only URL, so let's add it if it's not there
            if (url.getPath.isEmpty && !article.canonicalLink.startsWith("/")) "/" + article.canonicalLink else article.canonicalLink
          )
        })
      } catch {
        case ex: Exception => None
      }
      resolvedCanonicalLink match {
        case Some(resolvedUrl) => resolvedUrl.toString
        case None => return FailureResult("com.gravity.goose.Article#canonicalLink was not valid: " + article.canonicalLink).failureNel
      }
    } else {
      article.canonicalLink
    }

    def checkAdditionalData(key: String, check: (String) => Boolean): Option[String] = for {
      value <- article.additionalData.get(key)
      if check(value)
    } yield value

    val beaconTime = grvtime.currentTime

    val optionalFields = OptionalArticleFields(
      nullOrEmptyToNone(article.title),
      article.publishDate match {
        case d: java.util.Date if d != null => new DateTime(d.getTime).some
        case _ => if (ArticleWhitelist.isValidPartnerArticle(url) && ContentUtils.hasPublishDateExtractor(url)) {
          None
        } else {
          beaconTime.some
        }
      },
      if (article.topImage != null &&
          article.topImage.imageSrc != null &&
          article.topImage.imageSrc.tryToURL.isDefined &&
          !isImageUrlBlacklisted(article.topImage.imageSrc) ) {
        article.topImage.imageSrc.some
      } else {
        None
      },
      nullOrEmptyToNone(article.cleanedArticleText),
      checkAdditionalData("author", _.nonEmpty),
      checkAdditionalData("authorLink", al => al.nonEmpty && al.tryToURL.isDefined),
      checkAdditionalData("attribution.site", _.nonEmpty),
      checkAdditionalData("attribution.name", _.nonEmpty),
      checkAdditionalData("attribution.logo", al => al.nonEmpty && al.tryToURL.isDefined),
      checkAdditionalData("isPaid", _.nonEmpty).flatMap(_.tryToBoolean),
      ContentUtils.checkSummary(article),
      nullOrEmptyToNone(article.metaKeywords),
      None,
      Option(article.tags).getOrElse(Set.empty[String]),
      sectionPath = SectionPath.fromParam(sectionPath)
    )

    ArticleIngestionFields(url, siteGuid, beaconTime, articleType, optionalFields).successNel
  }

}

case class ArticlePutSpec(key: ArticleKey, spec: ArticleService.PutSpec, writeToWAL: Boolean = true)

case class RecentArticlePutResult(savedToSites: Boolean, savedToSections: Boolean, savedToCampaigns: Boolean)

object RecentArticlePutResult {
  val empty: RecentArticlePutResult = RecentArticlePutResult(savedToSites = false, savedToSections = false, savedToCampaigns = false)
  val emptySuccessNel: ValidationNel[FailureResult, RecentArticlePutResult] = empty.successNel[FailureResult]

  def fromSeparateResults(toSites: ValidationNel[FailureResult, Boolean], toSections: ValidationNel[FailureResult, Boolean], toCampaigns: ValidationNel[FailureResult, Boolean]): ValidationNel[FailureResult, RecentArticlePutResult] = {
    if (toSites.isFailure && toSections.isFailure && toCampaigns.isFailure) {
      List(toSites, toSections, toCampaigns).extrude match {
        case Success(_) => FailureResult("Failed to extrude failure results!").failureNel
        case Failure(fails) => fails.failure
      }
    } else {
      RecentArticlePutResult(toSites | false, toSections | false, toCampaigns | false).successNel
    }
  }
}

case class RecentArticlesDestinations(toSites: Boolean = false, toSections: Boolean = false, toCampaigns: Boolean = true)

object RecentArticlesDestinations {
  val empty: RecentArticlesDestinations = RecentArticlesDestinations(toCampaigns = false)

  val fromOrganic: RecentArticlesDestinations = RecentArticlesDestinations(toSites = true, toSections = true)
  val fromNonOrganic: RecentArticlesDestinations = empty
  val all: RecentArticlesDestinations = RecentArticlesDestinations(toSites = true, toSections = true, toCampaigns = true)
}

object RegraphUrlTempCache extends LongKeySingletonCache[ArticleKey] {
  import com.gravity.utilities.Counters._

  def cacheName: String = "RegraphUrlTempCache"

  val ttl: Int = 60 * 60 // one hour

  def cacheUrlAndReturnIfExisted(ak: ArticleKey): Boolean = {
    if (cache.cache.get(ak.articleId) != null) {
      countPerSecond("Graphing", "regraph.url.cache.hit")
      return true
    }

    countPerSecond("Graphing", "regraph.url.cache.miss")
    cache.putItem(ak.articleId, ak, ttl)
    false
  }
}
