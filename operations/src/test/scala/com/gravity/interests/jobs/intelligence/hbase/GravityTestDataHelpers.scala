package com.gravity.interests.jobs.intelligence.hbase

import java.io.{BufferedReader, BufferedWriter, InputStream}

import com.gravity.hbase.schema.{CommaSet, PutOp}
import com.gravity.interests.graphs.graphing.ScoredTerm
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.helpers.grvhadoop
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.interests.jobs.intelligence.operations.sponsored.SponsoredStoryService
import com.gravity.interests.jobs.intelligence.operations.users.UserSiteService
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvtime._
import com.gravity.utilities.grvz._
import com.gravity.utilities.thumby.ThumbyMode
import com.gravity.utilities.time.DateHour._
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}
import com.gravity.utilities.{HashUtils, Settings2, grvtime}
import org.apache.hadoop.conf.Configuration
import org.joda.time.DateTime

import scala.collection._
import scalaz.Scalaz._
import scalaz.{Success, _}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * A bunch of factory methods that help with testing.
 *
 * Don't extend this too much.  This is mixed into operationsTesting.  See http://confluence/display/DEV/Unit+Testing+Scala+Code#UnitTestingScalaCode-PackagingSharedFunctionality
 *
 */
trait GravityTestDataHelpers {
  // Use the hash of the full dotted classname of concretes subclasses that extend ClusterUnitBase as the seed for their pseudo-random number generator.
  private val deterministicPerTestClassRandom = new scala.util.Random(getClass.getName.hashCode)
  implicit val conf: Configuration = HBaseConfProvider.getConf.defaultConf

  /**
   * Gives you a file writer into the local cluster hdfs instance
    *
    * @param relpath The relative path
   * @param recreateIfPresent If true, will delete the file if it already exists
   * @param work A function that works with the output.  The output will be closed when this function goes out of scope.
   * @return
   */
  def withHdfsWriter(relpath: String, recreateIfPresent: Boolean = true)(work: (BufferedWriter) => Unit) {
    grvhadoop.withHdfsWriter(HBaseConfProvider.getConf.fs, relpath, recreateIfPresent)(work)
  }

  /**
   * Allows you to work with a reader opened into an hdfs file on the test cluster.
    *
    * @param relpath The path to the file
   * @param work The work you will do
   * @tparam A If you want to return a value after the work, here it is.
   * @return
   */
  def withHdfsReader[A](relpath: String)(work: (BufferedReader) => A): A = {
    grvhadoop.withHdfsReader(HBaseConfProvider.getConf.fs, relpath)(work)
  }

  def withHdfsDirectoryReader[A](relpath: String)(work: (BufferedReader) => A): A = {
    grvhadoop.withHdfsDirectoryReader(HBaseConfProvider.getConf.fs, relpath)(work)
  }

  def createFileFromStream(relPath: String, is: InputStream): Unit = {
    grvhadoop.createFileFromStream(HBaseConfProvider.getConf.fs, relPath, is)
  }

  /**
   * Reads a file into a buffer, allowing you to decide what's in the buffer depending on the output of the linereader function
    *
    * @param relpath Path to local hdfs buffer
   * @param linereader Function to return an element in the buffer, given the line fo the file
   * @return
   */
  def perHdfsLineToSeq[A](relpath: String)(linereader: (String) => A): Seq[A] = {
    grvhadoop.perHdfsLineToSeq(HBaseConfProvider.getConf.fs, relpath)(linereader)
  }


  /**
   * Reads a file line by line.  If you want to have the results in a buffer, use perHdfsLineToSeq
   */
  def perHdfsLine[A](relpath: String)(linereader: (String) => Unit) {
    grvhadoop.perHdfsLine(HBaseConfProvider.getConf.fs, relpath)(linereader)
  }

  /**
   * For each line in a directory of files
    *
    * @param relpath Path to files (or glob path)
   * @param linereader Will be invoked once per line with a string representation
   * @return Bupkiss
   */
  def perHdfsDirectoryLine(relpath: String)(linereader: (String) => Unit) {
    grvhadoop.perHdfsDirectoryLine(HBaseConfProvider.getConf.fs, relpath)(linereader)
  }

  def deleteSite(siteGuid: String) {
    Schema.Sites.delete(SiteKey(siteGuid)).execute()
    PermaCacher.restart()
  }

  def deleteCampaign(campaignKey: CampaignKey) {
    Schema.Campaigns.delete(campaignKey).execute()
    PermaCacher.restart()
  }

  def deleteUser(userKey: UserSiteKey) {
    Schema.UserSites.delete(userKey).execute()
    Schema.UserSites.query2.withKey(userKey).withColumn(_.userGuid).singleOption() match {
      case Some(user) => throw new Exception("This user shouldn't exist after deletion: " + userKey)
      case _ => // passed
    }
  }

  def deleteArticle(url: String): Unit = deleteArticle(ArticleKey(url))

  def deleteArticle(ak: ArticleKey): Unit = {
    Schema.Articles.delete(ak).execute()
    Schema.Articles.query2.withKey(ak).withColumn(_.url).singleOption() match {
      case Some(article) => throw new Exception(s"This article shouldn't exist after deletion: ${article.url} (articleId: ${ak.articleId}})")
      case _ => // passed
    }

  }

  def makeBeaconString(date: DateTime, siteGuid: String = "testsiteguid",
                       articleUrl: String = "http://cats.com/kittens.html", userGuid: String = "testuserguid",
                       referrerUrl: String = "http://cats.com/maincoons.html", title: String = "Test Article Title",
                       rawUrl: String = "http://felines.com/pounce.html", section: String = ""): String = {
    val dateString = date.toString("yyyy-MM-dd HH:mm:ss")

    dateString + "^beacon^" + siteGuid + "^Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2)^" + articleUrl + "^" + referrerUrl + "^74.123.148.66^NULL^" + userGuid + "^^^^^" + title + "^^^^^^^^^^^1^^^^" + rawUrl + "^^^^^^^^^^" + section
  }

  def makeConversionBeaconString(date: DateTime, siteGuid: String = "testsiteguid",
                       articleUrl: String = "http://cats.com/kittens.html", userGuid: String = "testuserguid",
                       referrerUrl: String = "http://cats.com/maincoons.html", title: String = "Test Article Title",
                       rawUrl: String = "http://felines.com/pounce.html", campaignKey: CampaignKey): String = {
    val dateString = date.toString("yyyy-MM-dd HH:mm:ss")

    dateString + "^conversion^" + siteGuid + "^Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2)^" + articleUrl + "^" + referrerUrl + "^74.123.148.66^NULL^" + userGuid + "^^^^^" + title + "^^^^^^^^^^^1^^^^" + rawUrl + "^^^^^^^^^^"
  }

  def makeSiteGuid(name: String = null): String = HashUtils.md5(if (name != null) name else deterministicPerTestClassRandom.nextLong().toString)

  val defaultSiteGuid = "CATS11b081dc6743bbe3537728eca43d"
  val defaultBaseUrl = "http://cats.com"

  def makeSite(siteGuid: String = defaultSiteGuid, siteName: String = "Test Site", baseUrl: String = defaultBaseUrl, isEnabled: Boolean = false): ValidationNel[FailureResult, SiteRow] = {
    val res = SiteService.makeSite(siteGuid, siteName, baseUrl, isEnabled)
    PermaCacher.restart()
    res
  }

  def makeOrFetchSite(siteGuid: String = defaultSiteGuid, siteName: String = "Test Site", baseUrl: String = defaultBaseUrl, isEnabled: Boolean = false): ValidationNel[FailureResult, SiteRow] = {
    makeSite(siteGuid, siteName, baseUrl, isEnabled) match {
      case s@Success(_) => s
      case f@Failure(_) =>
        val sk = SiteKey(siteGuid)
        SiteService.fetchSiteForManagement(sk)
    }
  }

  def makeOrFetchPool(poolName: String): ValidationNel[FailureResult, SiteRow] = {
    SponsoredStoryService.createSponsoredStoryPool(poolName) match {
      case s@Success(_) => s
      case f@Failure(_) => SponsoredStoryService.fetchPoolByPoolName(poolName)
    }
  }

  def makeAndModifySite(siteGuid: String = defaultSiteGuid, siteName: String = "Test Site",
                        baseUrl: String = defaultBaseUrl)
                       (work: PutOp[SitesTable, SiteKey] => PutOp[SitesTable, SiteKey]) {
    for {
      site <- SiteService.makeSite(siteGuid, siteName, baseUrl)
      modRes <- SiteService.modifySite(site.rowid)(work)
    } {
      PermaCacher.restart()
    }
  }

  def makeUrl(stem: String): String = defaultBaseUrl + stem

  def makeUser(userGuid: String, siteGuid: String = defaultSiteGuid, viewedArticles: Map[DateTime, ArticleKey] = Map(), conceptGraph: StoredGraph = StoredGraph.EmptyGraph): UserSiteKey = {

    val userSiteKey = UserSiteKey(userGuid, siteGuid)
    val userKey = UserKey(userGuid)

    Schema.UserSites.put(userSiteKey)
      .value(_.conceptStoredGraph, conceptGraph)
      .value(_.siteGuid, siteGuid)
      .valueMap(_.clickStream, viewedArticles.map(kv => ClickStreamKey(kv._1, kv._2) -> 1l))
      .value(_.userGuid, userGuid)
      .execute()

    Schema.Users.put(userKey, writeToWAL = false)
      .value(_.userGuid, userGuid)
      .execute()

    userSiteKey
  }

  def addViews(userSiteKey: UserSiteKey, time: DateHour = grvtime.currentHour, views: Seq[(ArticleKey, Long)]) {
    val viewMap = views.map {
      case (articleKey, vs) => ClickStreamKey(time, articleKey, ClickType.viewed) -> vs
    }.toMap
    Schema.UserSites.put(userSiteKey).valueMap(_.clickStream, viewMap).execute()
  }

  /**
   * takes a UserSiteKey and graphs their clickstream to merge in all the articles they viewed
   */
  def graphUsersClickstream(userKey: UserSiteKey) {
    Schema.UserSites.query2.withKey(userKey).withFamilies(_.clickStream, _.meta).singleOption() match {
      case Some(user) =>
        UserSiteService.graphClickstream(user, SchemaTypes.CounterNoOp).fold(_.println, clickStreamGraph => {
          val annoGraph = clickStreamGraph.annoGraph.getOrElse(StoredGraph.EmptyGraph)
          val autoGraph = clickStreamGraph.autoGraph.getOrElse(StoredGraph.EmptyGraph)
          Schema.UserSites.put(userKey).value(_.autoStoredGraph, autoGraph).value(_.annoStoredGraph, annoGraph).execute()
        })
      case None => println("ERROR: No user found for key: " + userKey)
    }
  }

  def makeCampaign(name: String, siteGuid: String = defaultSiteGuid,
                   dateReqs: CampaignDateRequirements = CampaignDateRequirements.createOngoing(grvtime.currentDay),
                   maxBid: DollarValue = DollarValue(10),
                   maxBidFloorOpt: Option[DollarValue] = None,
                   budget: DollarValue = DollarValue(10000),
                   budgetType: MaxSpendTypes.Type = MaxSpendTypes.defaultValue,
                   useCachedImages: Option[Boolean] = None,
                   thumbyMode: Option[ThumbyMode.Type] = None,
                   trackingParams: Map[String, String] = Map.empty,
                   countryRestrictions: Option[Seq[String]] = None,
                   deviceRestrictions: Option[Seq[Device.Type]] = None,
                   recentArticlesMaxAge: Option[Int] = None,
                   campIngestRules: Option[CampaignIngestionRules] = None,
                   campRecoRequirements: Option[CampaignRecoRequirements] = None
                    ): CampaignKey = {
    CampaignService.createCampaign(siteGuid, name, CampaignType.sponsored.some, dateReqs, maxBid, maxBidFloorOpt, isGravityOptimized = false,
                                   BudgetSettings(budget, budgetType).some, useCachedImages, thumbyMode, trackingParams.some,
                                   Settings2.INTEREST_SERVICE_USER_ID, countryRestrictions, deviceRestrictions, None,
                                   recentArticlesMaxAge, None, campIngestRules,
                                   campRecoRequirements = campRecoRequirements) match {
      case Success(camp) => camp.campaignKey
      case Failure(fails) => throw new RuntimeException("Failed to create campaign! Failures: " + fails.list.map(_.toString).mkString(" :: AND :: "))
    }
  }

  def makeOrFetchActiveCampaign(name: String, siteGuid: String = defaultSiteGuid,
                                dateReqs: CampaignDateRequirements = CampaignDateRequirements.createOngoing(grvtime.currentDay),
                                maxBid: DollarValue = DollarValue(10),
                                maxBidFloorOpt: Option[DollarValue] = None,
                                budget: DollarValue = DollarValue(10000),
                                budgetType: MaxSpendTypes.Type = MaxSpendTypes.defaultValue,
                                useCachedImages: Option[Boolean] = None,
                                thumbyMode: Option[ThumbyMode.Type] = None,
                                trackingParams: Map[String, String] = Map.empty,
                                countryRestrictions: Option[Seq[String]] = None,
                                deviceRestrictions: Option[Seq[Device.Type]] = None,
                                campIngestRules: Option[CampaignIngestionRules] = None,
                                campRecoRequirements: Option[CampaignRecoRequirements] = None
                                 ): CampaignKey = {
    val campKey = try{
      makeCampaign(name, siteGuid, dateReqs, maxBid, maxBidFloorOpt, budget, budgetType, useCachedImages, thumbyMode, trackingParams,
        countryRestrictions, deviceRestrictions, None, campIngestRules,
        campRecoRequirements = campRecoRequirements)
    } catch {
      case ex: RuntimeException =>
        val siteCampaigns = CampaignService.getCampaignsForSite(siteGuid)

        val existingCampaign = siteCampaigns.map(keyToRowMap => {
          keyToRowMap.values.find(_.nameOrNotSet == name).map(_.campaignKey)
        })

        existingCampaign match {
          case Failure(x) => throw new RuntimeException(x.toString)
          case Success(None) => throw new RuntimeException("could not create, could not find existing")
          case Success(Some(key)) => key
        }
    }

    CampaignService.updateStatus(campKey, CampaignStatus.active)
    campKey
  }

  def makeArticle(url: String,
                  siteGuid: String = defaultSiteGuid,
                  content: String = "I went to Google and looked at kittens.",
                  graph: StoredGraph = StoredGraphExamples.catsAndAppliancesGraph,
                  conceptGraph: StoredGraph = StoredGraph.EmptyGraph,
                  standardHourlyMetrics: Map[DateHour, StandardMetrics] = Map(
                    new GrvDateMidnight().toDateHour -> StandardMetrics(10, 9, 8, 7, 1)), publishTime: DateTime = new DateTime(),
                  title: String = "The Day I went to Google, MySpace, Friendster, Facebook",
                  image: String = "http://site1.com/image1.jpg",
                  category: String = "",
                  sections: Set[SectionKey] = Set(),
                  viralMetrics: Map[GrvDateMidnight, ViralMetrics] = Map( new GrvDateMidnight() -> ViralMetrics(tweets=10) ),
                  allArtGrvMap: ArtGrvMap.AllScopesMap = Map(),
                  tagsOpt: Option[CommaSet] = Option(CommaSet(Set("Google", "MySpace", "Facebook", "Friendster"))),
                  authorOpt: Option[String] = None,
                  relegenceStoryId: Option[Long] = None,
                  relegenceStoryInfo: Option[ArtStoryInfo] = None,
                  relegenceEntities: Seq[ArtRgEntity] = Nil,
                  relegenceSubjects: Seq[ArtRgSubject] = Nil

                   ): ArticleKey = {
    val articleUrl = url
    val articleKey = ArticleKey(articleUrl)

    ImageWritingMonitor.noteWritingArticleImage(articleUrl, image)    // In test

    val putOp = Schema.Articles.put(articleKey)
      .value(_.url, articleUrl)
      .value(_.title, title)
      .value(_.image, image)
      .value(_.content, content)
      .value(_.beaconTime, new DateTime())
      .value(_.publishTime, publishTime)
      .value(_.category, category)
      .value(_.siteGuid, siteGuid)
      .value(_.autoStoredGraph, graph)
      .value(_.conceptStoredGraph, graph)
      .value(_.sectionsOld, sections)
      .value(_.phraseConceptGraph, graph)
      .value(_.autoStoredGraph, graph)
      .value(_.conceptStoredGraph, graph)
      .value(_.termVector, List(ScoredTerm("a", 0.1), ScoredTerm("b", 0.2), ScoredTerm("c", 0.3)))
      .value(_.termVector2, List(ScoredTerm("a b", 0.4), ScoredTerm("b c", 0.5)))
      .value(_.termVector3, List(ScoredTerm("a b c", 0.6)))
      .value(_.termVectorG, List(ScoredTerm("diabetes", 0.9), ScoredTerm("lemurs", 0.1)))
      .value(_.relegenceEntities,relegenceEntities)
      .value(_.relegenceSubjects,relegenceSubjects)
      .valueMap(_.standardMetricsHourlyOld, standardHourlyMetrics)
      .valueMap(_.viralMetrics, viralMetrics)
      .valueMap(_.allArtGrvMap, allArtGrvMap)

    relegenceStoryId.foreach(storyId => putOp.value(_.relegenceStoryId, storyId))
    relegenceStoryInfo.foreach(storyInfo => putOp.value(_.relegenceStoryInfo, storyInfo))

    tagsOpt.foreach(tags => putOp.value(_.tags, tags))

    authorOpt.foreach(author => putOp.value(_.author, author))

    putOp.execute()

    articleKey
  }

  def makeArticle2(url: String, siteGuid: String = defaultSiteGuid,
                   content: String = "I went to Google and looked at kittens.",
                   graph: StoredGraph = StoredGraphExamples.catsAndAppliancesGraph,
                   standardHourlyMetrics: Map[DateHour, StandardMetrics] = Map(
                     new GrvDateMidnight().toDateHour -> StandardMetrics(10, 9, 8, 7, 1)), publishTime: DateTime = new DateTime(),
                   title: String = "The Day I went to Google, MySpace, Friendster, Facebook"): ArticleKey = {
    ArticleService.makeArticle(url, siteGuid) flatMap (result => {
      ArticleService.modifyArticle(ArticleKey(url)) {
        op =>
          op.value(_.title, title).value(_.content, content).value(_.publishTime, publishTime)
            .value(_.autoStoredGraph, graph).valueMap(_.standardMetricsHourlyOld, standardHourlyMetrics.toMap)
      }
    })
    ArticleKey(url)
  }
}
