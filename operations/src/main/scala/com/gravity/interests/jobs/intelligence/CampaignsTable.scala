package com.gravity.interests.jobs.intelligence

import java.net.URL

import com.gravity.domain.articles.ArticlePixel
import com.gravity.events.EventLogWriter
import com.gravity.hbase.schema.{CommaSet, DeserializedResult, HRow, HbaseTable}
import com.gravity.interests.jobs.intelligence.Device.Type
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.blacklist.{BlacklistColumns, BlacklistRow}
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager
import com.gravity.interests.jobs.intelligence.operations.FieldConverters._
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.interests.jobs.loading.RssFeedIngestionSettings
import com.gravity.utilities.analytics.DateMidnightRange
import com.gravity.utilities.geo.GeoLocation
import com.gravity.utilities.grvcoll._
import com.gravity.utilities.grvstrings.emptyString
import com.gravity.utilities.thumby.ThumbyMode
import com.gravity.utilities.time.GrvDateMidnight
import com.gravity.utilities.{Counters, grvtime}
import org.joda.time.DateTime

import scala.collection.{Map, _}
import scalaz.Validation
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.std.option._
import scalaz.syntax.validation._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 9/17/12
 * Time: 4:49 PM
 */
class CampaignsTable extends HbaseTable[CampaignsTable, CampaignKey, CampaignRow](tableName = "campaigns", rowKeyClass = classOf[CampaignKey], tableConfig = defaultConf)
with SponsoredMetricsColumns[CampaignsTable, CampaignKey]
with BlacklistColumns[CampaignsTable, CampaignKey]
with InterestGraph[CampaignsTable, CampaignKey]
with Relationships[CampaignsTable, CampaignKey]
with ConnectionPoolingTableManager
with HasArticles[CampaignsTable, CampaignKey]
with ArticlePublishMetrics[CampaignsTable, CampaignKey]
with SponsoredPoolSponsorColumns[CampaignsTable, CampaignKey]
with CampaignBudgetsColumns[CampaignsTable, CampaignKey]
with ContentTaggingColumns[CampaignsTable, CampaignKey]
{

  override def rowBuilder(result: DeserializedResult): CampaignRow = new CampaignRow(result, this)

  val numFamilyVersions = 100

  override lazy val recentArticlesRowTtl: Int = 60 * 60 * 24 * 90 // 90 days

  // FAMILY :: meta
  val meta :Fam[String,Any]= family[String,Any]("meta", compressed = true, versions = numFamilyVersions)
  val siteGuid:Col[String] = column(meta, "sg", classOf[String])
  val name:Col[String] = column(meta, "n", classOf[String])
  val campaignType:Col[CampaignType.Type] = column(meta, "type", classOf[CampaignType.Type])
  val bid:Col[DollarValue] = column(meta, "b", classOf[DollarValue])
  val isGravityOptimized :Col[Boolean]= column(meta, "g", classOf[Boolean])
  val maxBid:Col[DollarValue] = column(meta, "mb", classOf[DollarValue])
  val maxBidFloor:Col[DollarValue] = column(meta, "mbf", classOf[DollarValue])
  @deprecated(message = "use budgetSettings instead", since = "4/15/14")
  val maxSpend:Col[DollarValue] = column(meta, "ms", classOf[DollarValue])
  @deprecated(message = "use budgetSettings instead", since = "4/15/14")
  val maxSpendType:Col[MaxSpendTypes.Type] = column(meta, "mst", classOf[MaxSpendTypes.Type])
  val startDate:Col[GrvDateMidnight] = column(meta, "sd", classOf[GrvDateMidnight])
  val endDate:Col[GrvDateMidnight] = column(meta, "ed", classOf[GrvDateMidnight])
  val isOngoing:Col[Boolean] = column(meta, "o", classOf[Boolean])
  val status:Col[CampaignStatus.Type] = column(meta, "s", classOf[CampaignStatus.Type])
  val countryRestrictions:Col[Seq[String]] = column(meta, "geocr", classOf[Seq[String]])
  val deviceRestrictions:Col[Seq[Int]] = column(meta, "devr", classOf[Seq[Int]])
  val ageGroupRestrictions:Col[Seq[AgeGroup.Type]] = column(meta, "ager", classOf[Seq[AgeGroup.Type]])
  val genderRestrictions:Col[Seq[Gender.Type]] = column(meta, "genr", classOf[Seq[Gender.Type]])
  val mobileOsRestrictions:Col[Seq[MobileOs.Type]] = column(meta, "mos", classOf[Seq[MobileOs.Type]])
  val campaignBudgetNotificationTimestamp:Col[DateTime] = column(meta, "cbnts", classOf[DateTime])
  val displayDomain:Col[String] = column(meta, "dd", classOf[String])

  val thumbyMode:Col[ThumbyMode.Type] = column(meta, "tm", classOf[ThumbyMode.Type])
  val useCachedImages:Col[Boolean] = column(meta, "uc", classOf[Boolean])
  val recentArticlesMaxAge:Col[Int] = column(meta, "rama", classOf[Int])
  val recentArticlesSizeSnap:Col[Int] = column(meta, "rasz", classOf[Int])
  val defaultClickUrl:Col[URL] = column(meta, "dcurl", classOf[URL])
  // This is a secondary source of truth for the current BudgetSettings, the primary source of truth is dateRangeToBudgetData
  val budgetSettings:Col[BudgetSettings] = column(meta, "bs", classOf[BudgetSettings])
  val campIngestRules:Col[CampaignIngestionRules] = column(meta, "cir", classOf[CampaignIngestionRules])
  val schedule:Col[WeekSchedule] = column(meta, "schedule", classOf[WeekSchedule])
  val enableCampaignTrackingParamsOverride:Col[Boolean] = column(meta, "ctpo", classOf[Boolean])
  val articleImpressionPixel:Col[String] = column(meta, "aip", classOf[String])
  val articleClickPixel:Col[String] = column(meta, "acp", classOf[String])
  val enableAuxiliaryClickEvent:Col[Boolean] = column(meta, "acc", classOf[Boolean])

  // "Insertion order" ID
  val ioId:Col[String] = column(meta, "ii", classOf[String])
  val enableComscorePixel:Col[Boolean] = column(meta, "ecp", classOf[Boolean])

  // CampaignRecoRequirement Columns
  val recoAuthorNonEmpty:Col[Boolean] = column(meta, "ran", classOf[Boolean])
  val recoImageNonEmpty:Col[Boolean] = column(meta, "rin", classOf[Boolean])
  val recoImageCachedOk:Col[Boolean] = column(meta, "ric", classOf[Boolean])
  val recoTagsNonEmpty:Col[Boolean] = column(meta, "rtn", classOf[Boolean])
  val recoTagsRequirement:Col[CommaSet] = column(meta, "rtr", classOf[CommaSet])
  val recoTitleRequirement:Col[CommaSet] = column(meta, "rtir", classOf[CommaSet])
  // CampaignRecoRequirement evaluation data for the Campaign
  val recoEvalDateTimeReq:Col[DateTime] = column(meta, "rer", classOf[DateTime])
  val recoEvalDateTimeOk:Col[DateTime] = column(meta, "reo", classOf[DateTime])

  // FAMILY :: trackingParams
  val trackingParams :Fam[String,String]= family[String, String]("trk", compressed = true, versions = numFamilyVersions)

  // FAMILY :: auditTrail
  val auditTrail:Fam[DateTime,AuditMessage] = family[DateTime, AuditMessage]("at", compressed = true)

  // FAMILY :: statusHistory
  val statusHistory:Fam[DateTime,CampaignStatusChange] = family[DateTime, CampaignStatusChange]("sh2", compressed = true)

  // FAMILY :: recentArticleSettings
  val recentArticleSettings:Fam[ArticleKey,CampaignArticleSettings] = family[ArticleKey, CampaignArticleSettings]("ras", rowTtlInSeconds = recentArticlesRowTtl, compressed = true, versions = numFamilyVersions)

  // FAMILY :: metrics
  val metrics:Fam[CampaignMetricsKey,Long] = family[CampaignMetricsKey, Long]("m", compressed = true, versions = numFamilyVersions)

  // FAMILY :: feeds
  val feeds:Fam[String,RssFeedSettings] = family[String, RssFeedSettings]("f", compressed = true, versions = numFamilyVersions)

  // FAMILY :: feedUpdateReceived
  val walkBackSessionInfo :Fam[Long,WalkBackSessionState]= family[Long, WalkBackSessionState]("fur", compressed = true, versions = numFamilyVersions)

  // for pemanent warehousing
  val existsInWarehouse:Col[Boolean] = column(meta, "wh", classOf[Boolean])
}

class CampaignRow(result: DeserializedResult, table: CampaignsTable) extends HRow[CampaignsTable, CampaignKey](result, table)
with SponsoredMetricsRow[CampaignsTable, CampaignKey, CampaignRow]
with BlacklistRow[CampaignsTable, CampaignKey, CampaignRow]
with InterestGraphedRow[CampaignsTable, CampaignKey, CampaignRow]
with RelationshipRow[CampaignsTable, CampaignKey, CampaignRow]
with HasArticlesRow[CampaignsTable, CampaignKey, CampaignRow]
with ArticlePublishMetricsRow[CampaignsTable, CampaignKey, CampaignRow]
with SponsoredPoolSponsorRow[CampaignsTable, CampaignKey, CampaignRow]
with CampaignBudgetsRow[CampaignsTable, CampaignKey, CampaignRow]
with ContentTaggingRow[CampaignsTable, CampaignKey, CampaignRow]
{
  import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._
  // lazy families
  lazy val auditTrail: Map[DateTime, AuditMessage] = family(_.auditTrail)
  lazy val statusHistory: Map[DateTime, CampaignStatusChange] = family(_.statusHistory)
  lazy val metrics: Map[CampaignMetricsKey, Long] = family(_.metrics)
  lazy val feeds:Map[String,RssFeedSettings] = family(_.feeds)
  lazy val walkBackSessionInfo:Map[Long,WalkBackSessionState] = family(_.walkBackSessionInfo)
  lazy val feedUrls:scala.collection.Set[String] = feeds.keySet
  lazy val trackingParams:Map[String,String] = family(_.trackingParams)
  // defaultable columns
  lazy val campaignTypeOption:Option[CampaignType.Type] = column(_.campaignType)
  lazy val campaignType:CampaignType.Type = campaignTypeOption.getOrElse(if (nameOrNotSet == "Organic" && maxBid == DollarValue.zero) CampaignType.organic else CampaignType.sponsored)
  lazy val isGravityOptimized:Boolean = column(_.isGravityOptimized).getOrElse(false)
  lazy val maxBid:DollarValue = column(_.maxBid).getOrElse(DollarValue.zero)
  lazy val maxBidFloorOpt:Option[DollarValue] = column(_.maxBidFloor)
  lazy val maxBidFloor:DollarValue = maxBidFloorOpt.getOrElse(DollarValue.zero)
  lazy val isOngoing:Boolean = column(_.isOngoing).getOrElse(false)
  lazy val status: CampaignStatus.Type = column(_.status).getOrElse(CampaignStatus.defaultValue)
  lazy val countryRestrictions:Seq[String] = column(_.countryRestrictions).getOrElse(Seq.empty)
  lazy val deviceRestrictions: Seq[Type] = column(_.deviceRestrictions).getOrElse(Seq.empty).flatMap(id => Device.get(id.toByte))
  lazy val ageGroupRestrictions:Seq[AgeGroup.Type] = column(_.ageGroupRestrictions).getOrElse(Seq.empty)
  lazy val genderRestrictions: Seq[Gender.Type] = column(_.genderRestrictions).getOrElse(Seq.empty)
  lazy val mobileOsRestrictions: Seq[MobileOs.Type] = column(_.mobileOsRestrictions).getOrElse(Seq.empty)
  lazy val displayDomain:Option[String] = column(_.displayDomain)
  lazy val thumbyMode:ThumbyMode.Type = column(_.thumbyMode).getOrElse(ThumbyMode.off)
  lazy val useCachedImages:Boolean = column(_.useCachedImages).getOrElse(false)
  lazy val enableCampaignTrackingParamsOverride:Boolean = column(_.enableCampaignTrackingParamsOverride).getOrElse(false)
  override lazy val recentArticlesMaxAge:Option[Int] = column(_.recentArticlesMaxAge)
  lazy val recentArticlesSizeSnap:Option[Int] = column(_.recentArticlesSizeSnap)
  lazy val defaultClickUrl:Option[URL] = column(_.defaultClickUrl)

  /** So named because it is injected per article in a given widget for articles recommended via this campaign. */
  lazy val articleImpressionPixel: Option[String] = column(_.articleImpressionPixel).noneForEmptyString
  lazy val articleImpressionPixelAfterMacros: Option[String] = articleImpressionPixel.map(ArticlePixel.populateMacros)

  lazy val articleClickPixel: Option[String] = column(_.articleClickPixel).noneForEmptyString
  lazy val articleClickPixelAfterMacros: Option[String] = articleClickPixel.map(ArticlePixel.populateMacros)

  lazy val enableAuxiliaryClickEvent:Boolean = column(_.enableAuxiliaryClickEvent).getOrElse(false)
  lazy val ioId:Option[String] = column(_.ioId).noneForEmptyString
  lazy val enableComscorePixel:Boolean = column(_.enableComscorePixel).getOrElse(false)

  lazy val budgetSettings:Option[BudgetSettings] = column(_.budgetSettings)
  lazy val campIngestRules:CampaignIngestionRules = column(_.campIngestRules).getOrElse(CampaignIngestionRules(Nil))
  lazy val schedule:Option[WeekSchedule] = column(_.schedule)
  lazy val activeBudgetSettings:Option[BudgetSettings] = budgetSettings.map(b => b.copy(budgets = b.budgets.filter(_.maxSpend.nonEmpty)))

  // used to track when the last notifcation was sent so that we only send one per day
  lazy val campaignBudgetNotificationTimestamp:Option[DateTime] = column(_.campaignBudgetNotificationTimestamp)

  lazy val recoAuthorNonEmpty:Option[Boolean] = column(_.recoAuthorNonEmpty)
  lazy val recoImageNonEmpty:Option[Boolean] = column(_.recoImageNonEmpty)
  lazy val recoImageCachedOk:Option[Boolean] = column(_.recoImageCachedOk)
  lazy val recoTagsNonEmpty:Option[Boolean] = column(_.recoTagsNonEmpty)
  lazy val recoTagsRequirement:Option[CommaSet] = column(_.recoTagsRequirement)
  lazy val recoTitleRequirement:Option[CommaSet] = column(_.recoTitleRequirement)

  lazy val campRecoRequirementsOpt:Option[CampaignRecoRequirements] = CampaignRecoRequirements.fromColumnValues(
    recoAuthorNonEmpty,
    recoImageNonEmpty,
    recoImageCachedOk,
    recoTagsNonEmpty,
    recoTagsRequirement,
    recoTitleRequirement
  )
  lazy val campRecoRequirementsOrEmptyReq:CampaignRecoRequirements = campRecoRequirementsOpt.getOrElse(CampaignRecoRequirements())

  lazy val recoEvalDateTimeReqOpt:Option[DateTime] = column(_.recoEvalDateTimeReq)
  lazy val recoEvalDateTimeOkOpt:Option[DateTime]  = column(_.recoEvalDateTimeOk)

  def isActive: Boolean = status == CampaignStatus.active

  def isOrganic: Boolean = CampaignType.isOrganicType(campaignType)

  def isSponsored: Boolean = CampaignType.isSponsoredType(campaignType)

  def hasTrackingParam(macroName: String):Boolean = trackingParams.values.exists(_ == macroName)

  def allowsMobile: Boolean = deviceRestrictions.contains(Device.mobile)

  def allowsDesktop: Boolean = deviceRestrictions.contains(Device.desktop)

  def allowsUS: Boolean = countryRestrictions.contains(GeoLocation.usCountryCode.raw)

  def todaysSpend:Long = spendForDay(grvtime.currentDay)

  def totalSpend:Long = SponsoredMetricsKey.groupMapgregate(sponsoredMetrics, (kv: (SponsoredMetricsKey, Long)) => kv._1.campaignKey).getOrElse(campaignKey, SponsoredRecommendationMetrics.empty).totalSpent

  def spendForDay(day: GrvDateMidnight):Long = SponsoredMetricsKey.groupMapgregate(metricsForDay(day), (kv: (SponsoredMetricsKey, Long)) => kv._1.campaignKey).getOrElse(campaignKey, SponsoredRecommendationMetrics.empty).totalSpent

  def spendForDayBySite(day: GrvDateMidnight, site: SiteKey):Long = SponsoredMetricsKey.groupMapgregate(metricsForDay(day), (kv: (SponsoredMetricsKey, Long)) => {
    kv._1.siteKey
  }).getOrElse(site, SponsoredRecommendationMetrics.empty).totalSpent

  // helpful lazy vals
  lazy val campaignKey:CampaignKey = rowid
  lazy val nameOption:Option[String] = column(_.name)

  lazy val siteCampaignName:String = SiteService.siteMeta(SiteKey(siteGuid)).fold("")(_.nameOrNoName + " -> ") + nameOrNotSet

  @transient private lazy val cachedRecentArticleSettings: Map[ArticleKey, CampaignArticleSettings] = {

    info("cachedRecentArticleSettings starting campaignKey: " + campaignKey)

    val temp = ArticleDataLiteCache.getMulti(recentArticleKeys).valueOr(fails => {
      warn(fails, "Article Data Lite Cache fetch failure")
      Map.empty[ArticleKey, ArticleRow]
    }).filter(kv => kv._1 != null && kv._2 != null).mapValues(_.campaignSettings.get(campaignKey)).collect { case (ak, Some(settings)) => ak -> settings }

    info("cachedRecentArticleSettings done campaignKey: " + campaignKey + " size: " + temp.size)

    temp
  }

  //lazy val activeArticles = (for ((ak, s) <- articles; if (!s.isBlacklisted && s.status == CampaignArticleStatus.active)) yield ak).toSet
  def activeRecentArticles(skipCache: Boolean = false): Predef.Set[ArticleKey] = (for ((ak, s) <- recentArticleSettings(skipCache); if s.isActive) yield ak).toSet

  def recentArticleSettings(skipCache: Boolean = false): Map[ArticleKey, CampaignArticleSettings] = {
    if (skipCache) {
      ArticleService.fetchMulti(recentArticleKeys, skipCache = true)(_.withColumn(_.campaignSettings, campaignKey)).valueOr(fails => {
        warn(fails, "Article Service fetch failure")
        Map.empty[ArticleKey, ArticleRow]
      }).filter(kv => kv._1 != null && kv._2 != null).mapValues(_.campaignSettings.get(campaignKey)).collect { case (ak, Some(settings)) => ak -> settings }
    } else {
      cachedRecentArticleSettings
    }
  }

  lazy val siteGuidOption:Option[String] = SiteService.siteMeta(campaignKey.siteKey) match {
    case Some(s) => s.siteGuid
    case None => None
  }

  lazy val siteGuid:String = siteGuidOption.getOrElse("NoSiteGuidFound")

  lazy val activeFeeds: scala.Seq[RssFeedIngestionSettings] = for {
    (feedUrl, feedSettings) <- feeds.toSeq
    if feedUrl.nonEmpty
    if feedSettings.feedStatus == CampaignArticleStatus.active
  } yield RssFeedIngestionSettings(siteGuid, campaignKey, feedUrl, feedSettings)

  lazy val dateRequirementsOption:Option[CampaignDateRequirements] = if (isOngoing) {
    column(_.startDate) match {
      case Some(sd) => CampaignDateRequirements.createOngoing(sd).some
      case None => None
    }
  } else {
    column(_.startDate) tuple column(_.endDate) match {
      case Some((sd, ed)) => CampaignDateRequirements.createNonOngoing(sd, ed).some
      case None => None
    }
  }

  // string representations
  lazy val nameOrNotSet:String = nameOption.getOrElse("NAME NOT SET")

  lazy val bidOrMaxBid:DollarValue = if (isGravityOptimized) {
    column(_.maxBid).getOrElse(DollarValue.zero)
  } else {
    column(_.bid).getOrElse(DollarValue.zero)
  }

  lazy val maxBidString:String = bidOrMaxBid.toString

  lazy val maxBidPennies:Long = bidOrMaxBid.pennies

  lazy val startDateString:String = column(_.startDate) match {
    case Some(sd) => sd.toString("M/d/yyyy")
    case None => emptyString
  }

  lazy val startDateMillis:Long = column(_.startDate) match {
    case Some(sd) => sd.getMillis
    case None => 0l
  }

  lazy val endDateString:String = column(_.endDate) match {
    case Some(ed) => ed.toString("M/d/yyyy")
    case None => emptyString
  }

  lazy val endDateMillis:Long = column(_.endDate) match {
    case Some(ed) => ed.getMillis
    case None => 0l
  }

  lazy val dateRange:String = column(_.startDate) match {
    case Some(sd) =>
      val end = if (isOngoing) {
        "ONGOING"
      } else {
        column(_.endDate) match {
          case Some(ed) => ed.toString("M/d/yyyy")
          case None => "NOT SET"
        }
      }
      "From: " + sd.toString("M/d/yyyy") + " to: " + end
    case None => "NOT SET"
  }

  // helpful methods
  def hasDatesInRange(range: DateMidnightRange): Boolean = {
    column(_.startDate) match {
      case Some(sd) =>
        if (range.contains(sd)) return true

        if (sd.isAfter(range.toInclusive)) return false

        if (isOngoing) {
          true
        } else {
          column(_.endDate) match {
            case Some(ed) => !ed.isBefore(range.fromInclusive)
            case None => false
          }
        }
      case None => false
    }
  }

  def isThisAnOrganicCampaign(implicit checker: (CampaignKey) => Boolean): Boolean = campaignTypeOption.map(_.id == CampaignType.organic.id).getOrElse(checker(campaignKey))

  def validateBudget(implicit checker: (CampaignKey) => Boolean, beingCheckedBy: String): Validation[String, BudgetResult] = {
    val category = CampaignService.managementCategory

    // Bail early if this is an organic campaign
    if (isThisAnOrganicCampaign(checker)) return BudgetResult.defaultWithinBudgetSuccess

    // If we have active budget settings, validate each possible spend type
    activeBudgetSettings.foreach(bs => {
      // all but `total` are based on a time period, so let's grab wht today is now
      val today = grvtime.currentDay

      // Convert the sequence of budgets into a map of type->pennies so that we may process each
      val budgetTypeToSpend = (for {
        budget <- bs.budgets
        if budget.notExplicitlyInfinite
      } yield {
        budget.maxSpendType -> budget.maxSpend.pennies
      }).toMap

      // --> TOTAL
      budgetTypeToSpend.get(MaxSpendTypes.total).foreach(budget => {
        countPerSecond(category, "Validating campaign budget: total")
        val totalSpent = SponsoredMetricsKey.groupMapgregate(sponsoredMetrics, (kv: (SponsoredMetricsKey, Long)) => kv._1.campaignKey).getOrElse(campaignKey, SponsoredRecommendationMetrics.empty).totalSpent
        info(s"validateBudget: campaign: $campaignKey, totalSpent: $totalSpent, budget: $budget / total, isWithinBudget? ${totalSpent < budget}")
        if (totalSpent >= budget) {
          val overage = totalSpent - budget
          EventLogWriter.writeLogEvent("CampaignBudgetOverage", new DateTime(), CampaignBudgetOverage(campaignKey, beingCheckedBy, overage, MaxSpendTypes.total))
          countPerSecond(category, "Budget Overage (pennies): MaxSpendTypes.total", overage.toInt)
          return BudgetResult(isWithinBudget = false, MaxSpendTypes.total).success[String]
        }
      })

      // --> MONTHLY
      budgetTypeToSpend.get(MaxSpendTypes.monthly).foreach(budget => {
        countPerSecond(category, "Validating campaign budget: monthly")
        val thisMonthsMetrics = sponsoredMetrics.filterKeys(k => today.getYear == k.dateHour.getYear && today.getMonthOfYear == k.dateHour.getMonthOfYear)
        val totalSpent = SponsoredMetricsKey.groupMapgregate(thisMonthsMetrics, (kv: (SponsoredMetricsKey, Long)) => kv._1.campaignKey).getOrElse(campaignKey, SponsoredRecommendationMetrics.empty).totalSpent
        info(s"validateBudget: campaign: $campaignKey, totalSpent: $totalSpent, budget: $budget / monthly, isWithinBudget? ${totalSpent < budget}")
        if (totalSpent >= budget) {
          val overage = totalSpent - budget
          EventLogWriter.writeLogEvent("CampaignBudgetOverage", new DateTime(), CampaignBudgetOverage(campaignKey, beingCheckedBy, overage, MaxSpendTypes.monthly))
          countPerSecond(category, "Budget Overage (pennies): MaxSpendTypes.monthly", overage.toInt)
          return BudgetResult(isWithinBudget = false, MaxSpendTypes.monthly).success[String]
        }
      })

      // --> WEEKLY
      budgetTypeToSpend.get(MaxSpendTypes.weekly).foreach(budget => {
        countPerSecond(category, "Validating campaign budget: weekly")
        val thisWeeksMetrics = sponsoredMetrics.filterKeys(k => today.getYear == k.dateHour.getYear && today.getWeekOfWeekyear == k.dateHour.toGrvDateMidnight.getWeekOfWeekyear)
        val totalSpent = SponsoredMetricsKey.groupMapgregate(thisWeeksMetrics, (kv: (SponsoredMetricsKey, Long)) => kv._1.campaignKey).getOrElse(campaignKey, SponsoredRecommendationMetrics.empty).totalSpent
        info(s"validateBudget: campaign: $campaignKey, totalSpent: $totalSpent, budget: $budget / weekly, isWithinBudget? ${totalSpent < budget}")
        if (totalSpent >= budget) {
          val overage = totalSpent - budget
          EventLogWriter.writeLogEvent("CampaignBudgetOverage", new DateTime(), CampaignBudgetOverage(campaignKey, beingCheckedBy, overage, MaxSpendTypes.weekly))
          countPerSecond(category, "Budget Overage (pennies): MaxSpendTypes.weekly", overage.toInt)
          return BudgetResult(isWithinBudget = false, MaxSpendTypes.weekly).success[String]
        }
      })

      // --> DAILY
      budgetTypeToSpend.get(MaxSpendTypes.daily).foreach(budget => {
        countPerSecond(category, "Validating campaign budget: daily")
        val todaysMetrics = sponsoredMetrics.filterKeys(k => today == k.dateHour.toGrvDateMidnight)
        val totalSpent = SponsoredMetricsKey.groupMapgregate(todaysMetrics, (kv: (SponsoredMetricsKey, Long)) => kv._1.campaignKey).getOrElse(campaignKey, SponsoredRecommendationMetrics.empty).totalSpent
        info(s"validateBudget: campaign: $campaignKey, totalSpent: $totalSpent, budget: $budget / daily, isWithinBudget? ${totalSpent < budget}")
        if (totalSpent >= budget) {
          val overage = totalSpent - budget
          EventLogWriter.writeLogEvent("CampaignBudgetOverage", new DateTime(), CampaignBudgetOverage(campaignKey, beingCheckedBy, overage, MaxSpendTypes.daily))
          countPerSecond(category, "Budget Overage (pennies): MaxSpendTypes.daily", overage.toInt)
          return BudgetResult(isWithinBudget = false, MaxSpendTypes.daily).success[String]
        }
      })
    })

    // If no active budget or none of them hit or exceeded their budgets, return within budget success
    BudgetResult.defaultWithinBudgetSuccess
  }

  def validateSchedule(): Validation[String, ScheduleResultCase] = {
    val today = grvtime.currentDay

    if (isOngoing) {
      column(_.startDate) match {
        case Some(sd) => if (sd.isAfter(today)) {
          TooEarly.success
        } else {
          Within.success
        }
        case None => "Ongoing campaign without startDate!".failure
      }
    } else {
      column(_.startDate) tuple column(_.endDate) match {
        case Some((sd, ed)) =>
          if (ed.isBefore(today)) {
            TooLate.success
          } else if (sd.isAfter(today)) {
            TooEarly.success
          } else {
            Within.success
          }
        case None => "Non-Ongoing campaign without start and/or end date(s)".failure
      }
    }
  }

  // default to no existence in warehouse
  lazy val existsInWarehouse:Boolean = column(_.existsInWarehouse).getOrElse(false)

}

sealed trait ScheduleResultCase

case object TooEarly extends ScheduleResultCase {
  override val toString: String = "TooEarly: Scheduled to run at a later time."
}

case object TooLate extends ScheduleResultCase {
  override val toString: String = "TooLate: Scheduled to end before now."
}

case object Within extends ScheduleResultCase {
  override val toString: String = "Within: Scheduled to run at this time."
}

@SerialVersionUID(1L)
case class BudgetResult(isWithinBudget: Boolean, maxSpendType: MaxSpendTypes.Type) {
  lazy val maxReachedMessage: Option[String] = if (isWithinBudget) {
    None
  } else {
    Some("spent " + maxSpendType + " budget")
  }
}

object BudgetResult {
  val defaultWithinBudget: BudgetResult = BudgetResult(isWithinBudget = true, MaxSpendTypes.total)
  val defaultWithinBudgetSuccess: Validation[String, BudgetResult] = defaultWithinBudget.success[String]
}














