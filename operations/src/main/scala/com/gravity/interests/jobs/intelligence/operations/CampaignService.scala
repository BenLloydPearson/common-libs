package com.gravity.interests.jobs.intelligence.operations

import java.net.URL
import java.util.concurrent.atomic.AtomicLong

import com.gravity.api.partnertagging.PartnerTagging._
import com.gravity.domain.gms.GmsArticleStatus
import com.gravity.hbase.schema.OpsResult
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.FieldConverters._
import com.gravity.interests.jobs.intelligence.operations.ImageCachingService.OptImgSaver
import com.gravity.interests.jobs.intelligence.operations.analytics.DashboardUserService
import com.gravity.interests.jobs.intelligence.operations.audit.AuditService
import com.gravity.interests.jobs.intelligence.operations.audit.OperationsAuditLoggingEvents._
import com.gravity.interests.jobs.intelligence.operations.recommendations.CampaignRecommendationData
import com.gravity.interests.jobs.intelligence.operations.sites.SiteGuidService
import com.gravity.interests.jobs.intelligence.schemas.{ArticleIngestionData, DollarValue}
import com.gravity.logging.Logstashable
import com.gravity.service.grvroles
import com.gravity.service.remoteoperations.{ProductionRemoteOperationsClient, RemoteOperationsClient, RemoteOperationsHelper}
import com.gravity.utilities._
import com.gravity.utilities.analytics.{DateHourRange, DateMidnightRange}
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.geo.GeoDatabase
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvfunc._
import com.gravity.utilities.grvtime._
import com.gravity.utilities.grvz._
import com.gravity.utilities.thumby.ThumbyMode
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}
import com.gravity.utilities.web.{ContentUtils, ParamValidationFailure, ValidationCategory}
import com.gravity.valueclasses.ValueClassesForDomain._
import org.joda.time.DateTime

import scala.collection._
import scala.concurrent.duration._
import scala.util.Try
import scalaz.Scalaz._
import scalaz._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 9/26/12
 * Time: 10:34 AM
 */
object CampaignService extends CampaignService with ProductionArticleServiceProxy {

}

trait CampaignService extends CampaignOperations
  with SponsoredPoolSponsorOperations[CampaignsTable, CampaignKey, CampaignRow] {
  this: ArticleServiceProxy =>

  import com.gravity.logging.Logging._

  private val hbaseConf = if(grvroles.isFrontEndRole) HBaseConfProvider.getConf.oltpConf else HBaseConfProvider.getConf.defaultConf

  override val counterCategory: String = "Campaign Service"

  val managementCategory = "CampaignManagement"

  val defaultTtlSeconds = 5 * 60

  /** @see [[ArticleService.exampleArticleRow]] a child article of this example campaign. */
  val exampleCampaignKey = CampaignKey(SiteKey(-5790131663773862499L), -7805491823054792056L)
  val exampleCampaignRow = fetch(exampleCampaignKey, skipCache = false)(_.withAllColumns)

  def getActiveCampaignsFromHbase: ValidationNel[FailureResult, Set[CampaignKey]] = {
    if (grvroles.isDevelopmentRole)
      filteredScan(hbaseConf = hbaseConf)(_.withColumns(_.siteGuid, _.status).filter(_.and(_.columnValueMustEqual(_.status, CampaignStatus.active)))).map(_.keySet)
    else
    // This is intentionally a full scan and not a filtered scan to avoid scan timeouts by keeping data coming back to
    // the server more frequently
      scanToSeqWithoutMaintenanceCheck(rowCache = 10, hbaseConf = hbaseConf)(_.withColumns(_.siteGuid, _.status)).map(_.filter(_.status == CampaignStatus.active).map(_.campaignKey).toSet)
  }

  def getCampaignsForSite(siteGuid: String): ValidationNel[FailureResult, Map[CampaignKey, CampaignRow]] = {
    filteredScan(hbaseConf = hbaseConf)(_.withColumns(_.siteGuid, _.status, _.name).filter(_.and(_.columnValueMustEqual(_.siteGuid, siteGuid))))
  }

  val activeCampaignCacheKey = "CampaignService.activeCampaignKeys"

  def activeCampaignKeys: Set[CampaignKey] = {
    def factoryFun() = {
      getActiveCampaignsFromHbase match {
        case Success(campMap) => campMap.some
        case Failure(fails) =>
          warn(fails,"Failed to load CampaignService.activeCampaignKeys due to the following:")
          None
      }
    }

    PermaCacher.getOrRegisterFactoryWithOption(activeCampaignCacheKey, 5 * 60 /*, resourceType = "campaign" -- commenting out so that this op gets bundled with the other HBase ops */) {
      PermaCacher.retryUntilNoThrow(factoryFun)
    }.getOrElse(Set.empty[CampaignKey])
  }

  /** @return The subset of campaign keys from the set passed for active campaigns. */
  def activeCampaignKeys(campaignKeys: Set[CampaignKey], skipCache: Boolean = false): Set[CampaignKey] = if (skipCache) {
    fetchMulti(campaignKeys, hbaseConf = hbaseConf)(_.withColumns(_.siteGuid, _.status).filter(_.and(_.columnValueMustEqual(_.status, CampaignStatus.active)))).fold(fails => {
      warn(fails,"Failed to load CampaignService.activeCampaignKeys(...) due to the following:")
      Set.empty
    }, _.keySet)
  }
  else
    campaignKeys intersect activeCampaignKeys

  val organicCampaignName = "Organic"
  //this might vary into "Organic Local" and "Organic Network" and other such things later
  val isGravityOptimizedDefault = false

  def isOrganic(campaignKey: CampaignKey): Boolean = campaignMeta(campaignKey, skipCache = Settings.ENABLE_SITESERVICE_META_CACHE).exists(_.isOrganic)

  def isSponsored(campaignKey: CampaignKey): Boolean = campaignMeta(campaignKey, skipCache = Settings.ENABLE_SITESERVICE_META_CACHE).exists(_.isSponsored)

  def createOrganicCampaign(siteGuid: String, campaignName: String, userId: Long,
                            displayDomain: Option[String] = None, recentArticlesMaxAge: Option[Int] = None,
                            trackingParams: Option[Map[String, String]] = None, thumbyMode: Option[ThumbyMode.Type] = None): ValidationNel[FailureResult, CampaignRow] = {
    for {
      campaignCreated <- createCampaign(siteGuid, campaignName,
        CampaignType.organic.some,
        CampaignDateRequirements.createOngoing(new GrvDateMidnight()),
        DollarValue.zero,
        None,
        isGravityOptimized = false,
        budgetSettings = BudgetSettings(maxSpend = DollarValue(1), maxSpendType = MaxSpendTypes.total).some,
        userId = userId,
        displayDomain = displayDomain,
        recentArticlesMaxAge = recentArticlesMaxAge,
        trackingParams = trackingParams,
        thumbyMode = thumbyMode)
      _ <- updateStatus(campaignCreated.campaignKey, CampaignStatus.active, userId)
      updatedCampaign <- fetch(campaignCreated.campaignKey)(_.withFamilies(_.meta, _.trackingParams))
    } yield updatedCampaign
  }

  def updateOrganicCampaign(campaignKey: CampaignKey, campaignName: String, userId: Long,
                            displayDomain: Option[String] = None, recentArticlesMaxAge: Option[Int] = None,
                            trackingParams: Option[Map[String, String]] = None, thumbyMode: Option[ThumbyMode.Type] = None): ValidationNel[FailureResult, CampaignRow] = {
    for {
      campaignUpdated <- updateCampaignMeta(campaignKey, campaignName,
        CampaignType.organic.some,
        CampaignDateRequirements.createOngoing(new GrvDateMidnight()),
        DollarValue.zero,
        None,
        isGravityOptimized = false,
        budgetSettings = BudgetSettings(maxSpend = DollarValue(1), maxSpendType = MaxSpendTypes.total).some,
        userId = userId,
        displayDomain = displayDomain,
        recentArticlesMaxAge = recentArticlesMaxAge,
        trackingParamsOption = trackingParams,
        thumbyMode = thumbyMode)
      //      _ <- updateStatus(campaignUpdated.campaignKey, CampaignStatus.active, userId)
      updatedCampaign <- fetch(campaignUpdated.campaignKey)(_.withFamilies(_.meta, _.trackingParams))
    } yield updatedCampaign
  }

  def createCampaign(
                      siteGuid: String,
                      campaignName: String,
                      campaignType: Option[CampaignType.Type] = None,
                      dateRequirements: CampaignDateRequirements,
                      bidOrMaxBid: DollarValue,
                      maxBidFloorOpt: Option[DollarValue],
                      isGravityOptimized: Boolean,
                      budgetSettings: Option[BudgetSettings] = None,
                      useCachedImages: Option[Boolean] = None,
                      thumbyMode: Option[ThumbyMode.Type] = None,
                      trackingParams: Option[Map[String, String]] = None,
                      userId: Long = -1l,
                      countryRestriction: Option[Seq[String]] = None,
                      deviceRestriction: Option[Seq[Device.Type]] = None,
                      displayDomain: Option[String] = None,
                      recentArticlesMaxAge: Option[Int] = None,
                      defaultClickUrl: Option[URL] = None,
                      campIngestRules: Option[CampaignIngestionRules] = None,
                      enableCampaignTrackingParamsOverride: Option[Boolean] = None,
                      articleImpressionPixel: Option[String] = None,
                      articleClickPixel: Option[String] = None,
                      enableAuxiliaryClickEvent: Boolean = false,
                      ioId: Option[String] = None,
                      enableComscorePixel: Boolean = false,
                      contentCategoriesOpt: Option[ContentCategoriesSetting] = None,
                      contentRatingOpt: Option[ContentRatingSetting] = None,
                      campRecoRequirements: Option[CampaignRecoRequirements] = None,
                      genderRestrictions: Option[Seq[Gender.Type]] = None,
                      ageGroupRestrictions: Option[Seq[AgeGroup.Type]] = None,
                      mobileOsRestrictions: Option[Seq[MobileOs.Type]] = None
                    ):
  ValidationNel[FailureResult, CampaignRow] = {
    if (ScalaMagic.isNullOrEmpty(siteGuid)) return FailureResult("siteGuid MUST be NON-Empty!").failureNel

    val campaignKey = CampaignKey.generate(siteGuid)
    updateCampaignMeta(campaignKey, campaignName, campaignType, dateRequirements, bidOrMaxBid, maxBidFloorOpt,
      isGravityOptimized, budgetSettings, None,

      // INTERESTS-8462: Make S3 Image Caching the default for all Sponsored campaigns (unless overridden here with non-None useCachedImages).
      useCachedImages.orElse(campaignType.map(CampaignType.isSponsoredType(_))).getOrElse(false).some,

      thumbyMode.orElse(ThumbyMode.off.some),
      trackingParams, None, siteGuid.some, userId,
      countryRestriction, deviceRestriction, displayDomain, recentArticlesMaxAge, defaultClickUrl, campIngestRules,
      enableCampaignTrackingParamsOverride, isCreate = true,
      articleImpressionPixel = articleImpressionPixel,
      articleClickPixel = articleClickPixel,
      enableAuxiliaryClickEvent = enableAuxiliaryClickEvent.some,
      ioId = ioId,
      enableComscorePixel = enableComscorePixel.some,
      contentCategoriesChange = contentCategoriesOpt,
      contentRatingChange = contentRatingOpt,
      campRecoRequirements = campRecoRequirements.map(Option(_)),
      genderRestrictions = genderRestrictions,
      ageGroupRestrictions = ageGroupRestrictions,
      mobileOsRestrictions = mobileOsRestrictions)
  }

  private val endDateColumnSet = Set(Schema.Campaigns.endDate.columnName)

  // The list of column families whose changes should be logged (do not include recentArticleSettings here, see loggingFetchQuerySpecWithRecentArticle)
  val campaignLoggingFamilies = List(
    (t: CampaignsTable) => t.meta,
    (t: CampaignsTable) => t.trackingParams,
    (t: CampaignsTable) => t.feeds,
    (t: CampaignsTable) => t.contentTagging
  )

  // Select only the basic column families to be logged, without the (possibly-huge) recentArticleSettings.
  val loggingFetchQuerySpecWithoutRecentArticles: QuerySpec = _.withFamilies(campaignLoggingFamilies.head, campaignLoggingFamilies.tail: _*)

  // Select the basic column families to be logged, plus a single ak->cas from recentArticleSettings.
  def loggingFetchQuerySpecWithRecentArticle(key: PublishDateAndArticleKey): QuerySpec = { builder =>
    loggingFetchQuerySpecWithoutRecentArticles(builder)
      .withColumn(_.recentArticles, key)
  }

  /**
    * @deprecated This is for testing only, and will probably soon be removed.
    */
  def oldLoggingFetchQuerySpecWithRecentArticleTestingOnlySlowSlow(key: PublishDateAndArticleKey): QuerySpec = { builder =>
    loggingFetchQuerySpecWithoutRecentArticles(builder)
      .withFamilies(_.recentArticles)
      .filter(
        _.or(
          _.allInFamilies(campaignLoggingFamilies: _*),
          _.betweenColumnKeys(_.recentArticles, key, key)
        )
      )
  }

  // Select the basic column families to be logged, plus ALL of the possibly-huge (e.g. 250,000 or more) recent articles.  You'll be sorry.
  val loggingFetchQuerySpecWithAllRecentArticlesExtremelyBigAndSlow: QuerySpec = { builder =>
    loggingFetchQuerySpecWithoutRecentArticles(builder)
      .withFamilies(_.recentArticles)
  }

  /**
    * @param maxBidFloorOpt       None = No change.
    * @param newSiteGuidOption    If performing a create, must specify Some(siteGuid). If not create, must be None.
    * @param countryRestriction   None = No change, Some(Seq(...)) = update country restrictions with new value
    * @param deviceRestriction    None = No change, Some(Seq(...)) = update device restrictions with new value
    * @param displayDomain        None = No change, Some(...) = update display domain with new value
    * @param recentArticlesMaxAge max article age (in days).  Pass Some(0) for 'no max age'
    * @param defaultClickUrl      None = No change, Some(URL(...)) = set the default click URL when ingesting articles (this is an advanced option!!)
    * @param isCreate             True if performing a create
    */
  def updateCampaignMeta(
                          campaignKey: CampaignKey,
                          campaignName: String,
                          campaignType: Option[CampaignType.Type],
                          dateRequirements: CampaignDateRequirements,
                          bidOrMaxBid: DollarValue,
                          maxBidFloorOpt: Option[DollarValue],
                          isGravityOptimized: Boolean,
                          budgetSettings: Option[BudgetSettings] = None,
                          budgetStartHour: Option[DateHour] = None,
                          useCachedImages: Option[Boolean] = None,
                          thumbyMode: Option[ThumbyMode.Type] = None,
                          trackingParamsOption: Option[Map[String, String]] = None,
                          statusOption: Option[CampaignStatus.Type] = None,
                          newSiteGuidOption: Option[String] = None,
                          userId: Long = -1l,
                          countryRestriction: Option[Seq[String]] = None,
                          deviceRestriction: Option[Seq[Device.Type]] = None,
                          displayDomain: Option[String] = None,
                          recentArticlesMaxAge: Option[Int] = None,
                          defaultClickUrl: Option[URL] = None,
                          campIngestRules: Option[CampaignIngestionRules] = None,
                          enableCampaignTrackingParamsOverride: Option[Boolean] = None,
                          articleImpressionPixel: Option[String] = None,
                          articleClickPixel: Option[String] = None,
                          isCreate: Boolean = false,
                          enableAuxiliaryClickEvent: Option[Boolean] = None,
                          ioId: Option[String] = None,
                          enableComscorePixel: Option[Boolean] = None,
                          contentCategoriesChange: Option[ContentCategoriesSetting] = None,
                          contentRatingChange: Option[ContentRatingSetting] = None,
                          campRecoRequirements: Option[Option[CampaignRecoRequirements]] = None,
                          genderRestrictions: Option[Seq[Gender.Type]] = None,
                          ageGroupRestrictions: Option[Seq[AgeGroup.Type]] = None,
                          mobileOsRestrictions: Option[Seq[MobileOs.Type]] = None
                        ):
  ValidationNel[FailureResult, CampaignRow] = {

    require(newSiteGuidOption.isDefined == isCreate, "Provide a siteGuid if performing campaign creation")

    //Get current CampaignRow before update, for logging
    val beforeRow = fetch(campaignKey)(loggingFetchQuerySpecWithoutRecentArticles)

    if (!isCreate && beforeRow.isFailure) return beforeRow //Failure: Could not load state of campaign before executing changes

    for {
      _ <- validateCampaignName(campaignKey, campaignName, isCreate)
      _ <- validateCampaignType(beforeRow.toOption, campaignType)
      _ <- validateComscorePixelConfig(beforeRow.toOption, enableComscorePixel, ioId)
      _ <- if (dateRequirements.isOngoing) {
        doModifyDelete(campaignKey)(del => {
          if (dateRequirements.isOngoing) del.values(_.meta, endDateColumnSet)

          del
        }) match {
          case Success(_) => Unit.successNel
          case Failure(fails) => fails.failure
        }
      }
      else {
        Unit.successNel
      }
      _ <- budgetSettings match {
        case Some(settings) =>
          setBudget(campaignKey, settings, userId = userId, startHour = budgetStartHour.getOrElse(grvtime.currentHour))
        case None => OpsResult(0, 0, 0).successNel
      }
      campaign <- modifyPutRefetch(campaignKey)(put => {
        newSiteGuidOption.foreach(siteGuid => put.value(_.siteGuid, siteGuid))
        put.value(_.name, campaignName)

        dateRequirements.addValuesToPut(put)

        put.value(_.isGravityOptimized, isGravityOptimized)

        put.value(if (isGravityOptimized) _.maxBid else _.bid, bidOrMaxBid)

        maxBidFloorOpt.foreach(maxBidFloor => put.value(_.maxBidFloor, maxBidFloor))

        // INTERESTS-8462: If the campaignType is going from a None => Some transition, then useCachedImages = isCampaignTypeSponsored.
        val updUseCachedImages = if (beforeRow.toOption.flatMap(_.campaignTypeOption).isEmpty && campaignType.isDefined)
          CampaignType.isSponsoredType(campaignType.get).some
        else
          useCachedImages

        updUseCachedImages.foreach(useCachedImagesValue => put.value(_.useCachedImages, useCachedImagesValue))

        thumbyMode.foreach(thumbyModeVal => {
          put.value(_.thumbyMode, thumbyModeVal)
        })

        enableCampaignTrackingParamsOverride.foreach(trackingOverrideValue => put.value(_.enableCampaignTrackingParamsOverride, trackingOverrideValue))

        articleImpressionPixel.foreach(value => put.value(_.articleImpressionPixel, value))
        articleClickPixel.foreach(value => put.value(_.articleClickPixel, value))

        contentCategoriesChange match {
          case Some(Some(seq)) =>
            put.value(_.contentCategories, seq.toSet)

          case Some(None) =>
            doModifyDelete(campaignKey)(_.values(_.contentTagging, Set(table.contentCategories.getQualifier)))

          case _ =>
        }

        contentRatingChange match {
          case Some(Some(enum)) =>
            put.value(_.contentRating, enum)

          case Some(None) =>
            doModifyDelete(campaignKey)(_.values(_.contentTagging, Set(table.contentRating.getQualifier)))

          case _ =>
        }

        campRecoRequirements.foreach {
          case (recoReqOpt) if beforeRow.isFailure || recoReqOpt != beforeRow.toOption.flatMap(_.campRecoRequirementsOpt) =>
            recoReqOpt match {
              case None =>
                doModifyDelete(campaignKey) { del => CampaignRecoRequirements().toPutAndDeleteSpec(put, del); del }
              case Some(recoRequirements) =>
                doModifyDelete(campaignKey) { del => recoRequirements.toPutAndDeleteSpec(put, del); del }
            }
            put.value(_.recoEvalDateTimeReq, grvtime.currentTime.plusMinutes(CampaignRecoRequirements.delayMinutesForEvaluation))
          case _ =>
        }

        trackingParamsOption.foreach(trackingParams => {
          doModifyDelete(campaignKey)(del => del.family(_.trackingParams))
          put.valueMap(_.trackingParams, trackingParams)
        })

        countryRestriction.foreach(countryCodes => {
          val resolved = countryCodes.flatMap(cc => {
            val negative = cc.startsWith("-")
            GeoDatabase.findById(cc.stripPrefix("-")).map(_.id.ifThen(negative)("-" + _))
          })
          put.value(_.countryRestrictions, resolved)
        })

        deviceRestriction.foreach(deviceList => {
          if (deviceList.groupBy(_.name.startsWith("-")).size > 1) {
            return FailureResult("Device restrictions must either be all-inclusive or all-exclusive, they cannot be mixed").failureNel
          }
          put.value(_.deviceRestrictions, deviceRestriction.get.map(_.id.toInt).sorted)
        })

        ageGroupRestrictions.foreach(ageGroup => {
          put.value(_.ageGroupRestrictions, ageGroup)
        })

        genderRestrictions.foreach(gender => {
          put.value(_.genderRestrictions, gender)
        })

        mobileOsRestrictions.foreach(mobileOs => {
          put.value(_.mobileOsRestrictions, mobileOs)
        })

        displayDomain.foreach(value => put.value(_.displayDomain, value))
        campaignType.foreach(value => put.value(_.campaignType, value))
        enableAuxiliaryClickEvent.foreach(value => put.value(_.enableAuxiliaryClickEvent, value))
        ioId.foreach(value => put.value(_.ioId, value))
        enableComscorePixel.foreach(value => put.value(_.enableComscorePixel, value))

        recentArticlesMaxAge.foreach(value => {
          // if value > 0, apply the value, otherwise remove the max age restriction
          if (value > 0) {
            put.value(_.recentArticlesMaxAge, value)
          } else {
            doModifyDelete(campaignKey)(del => del.values(_.meta, Set(Schema.Campaigns.recentArticlesMaxAge.getQualifier)))
          }
        })

        defaultClickUrl.foreach(value => {
          // if "empty" url is passed in, remove the default click URL
          if (value.toString.toLowerCase == "http:") {
            doModifyDelete(campaignKey)(del => del.values(_.meta, Set(Schema.Campaigns.defaultClickUrl.getQualifier)))
          } else {
            put.value(_.defaultClickUrl, value)
          }
        })

        if (campIngestRules.isDefined)
          put.value(_.campIngestRules, campIngestRules.get)

        put

      })(loggingFetchQuerySpecWithoutRecentArticles)
      _ <- SiteService.modifyPut(campaignKey.siteKey) { put =>
        updateSaveCachedImages(put, campaignKey, beforeRow.toOption.map(_.useCachedImages), useCachedImages)
          .valueMap(_.campaigns, Map(campaignKey -> campaign.status))
      }

      _ = AuditService.logAllChanges(campaignKey, userId, beforeRow.toOption, campaign, Seq("Campaign Service - update campaign")) valueOr { case f: NonEmptyList[FailureResult] => warn(f, "Campaign Service - update campaign") }
    }
      yield {
        statusOption.foreach(status => {
          updateStatus(campaignKey, status, userId) match {
            case Success(result) =>
            case Failure(fails) => warn(fails)
          }
        })
        campaign
      }
  }

  // If CampaignRow.useCachedImages is updated, make consequential update to SiteRow.saveCachedImages
  def updateSaveCachedImages(put: SiteService.PutSpec,
                             changingCk: CampaignKey,
                             optOldUseCachedImages: Option[Boolean],
                             optNewUseCachedImages: Option[Boolean]) = {
    (optOldUseCachedImages, optNewUseCachedImages) match {
      // If a campaign enables usedCachedImages, then enable saveCachedImages for the site.
      case (_, Some(true)) =>
        put.value(_.saveCachedImages, true)

      // Eh, it's fine.
      case (Some(false), Some(false)) =>

      // If we have disabled usedCachedImages for all the campaigns in a site, then disable saveCachedImages for the site.
      // Going from None to false, or true to false -- let's have a look at the other campaigns.
      case (_, Some(false)) =>
        // Let's try to get that information Live!
        val vLiveWantSiteSaveCachedImages = for {
          siteCks <- SiteService.fetchOrEmptyRow(changingCk.siteKey)(_.withFamilies(_.campaigns)).map(_.campaignKeys)
          otherCks = siteCks.filterNot(_ == changingCk)
          useCachedImagesExists <- CampaignService.fetchMulti(otherCks)(_.withColumns(_.useCachedImages)).map(_.values.exists(_.useCachedImages))
        } yield {
          useCachedImagesExists
        }

        // If getting it Live! didn't work, here's a cached way.
        lazy val cachedWantSiteSaveCachedImages = {
          val siteCampRows = allCampaignMetaBySiteKey.getOrElse(changingCk.siteKey, Seq())
          siteCampRows.filterNot(_.campaignKey == changingCk).exists(_.useCachedImages)
        }

        val wantSiteSaveCachedImages: Boolean = vLiveWantSiteSaveCachedImages getOrElse cachedWantSiteSaveCachedImages

        if (wantSiteSaveCachedImages == false)
          put.value(_.saveCachedImages, false)

      case _ =>
    }

    put
  }

  def validateStatusAgainstScheduleAndBudget(campaignKey: CampaignKey, preRetrievedRow: Option[CampaignRow] = None, checkedBy: String): ValidationNel[FailureResult, CampaignStatusValidationResponse] = {
    val campaign = preRetrievedRow match {
      case Some(preRow) => if (preRow.campaignKey != campaignKey) {
        return FailureResult("`preRetrievedRow` has a campaignKey (" + preRow.campaignKey + ") that did NOT equal `campaignKey` (" + campaignKey + ")!").failureNel
      }
      else {
        preRow
      }
      case None =>
        // let's calculate the maximum sponsoredMetrics history we need for this query based on the campaign's budget
        val startHour = campaignMeta(campaignKey, skipCache = true).flatMap(_.activeBudgetSettings).flatMap {
          budgetSettings =>
            // use the widest range possible (up to monthly, 'total' is checked by the job)
            if (budgetSettings.getBudget(MaxSpendTypes.monthly).isDefined) {
              grvtime.currentMonth.toDateHour.some
            }
            else if (budgetSettings.getBudget(MaxSpendTypes.weekly).isDefined) {
              grvtime.firstDayOfThisWeek(grvtime.currentDay).toDateHour.some
            }
            else if (budgetSettings.getBudget(MaxSpendTypes.daily).isDefined) {
              grvtime.currentDay.toDateHour.some
            }
            else {
              // we are already defaulting to current month, so no need to check further
              None
            }
        }.getOrElse(grvtime.dateToMonth(grvtime.currentDay).toDateHour).minusHours(1)

        fetch(campaignKey, skipCache = true)(_.withFamilies(_.meta, _.sponsoredMetrics)
          .filter(
            _.or(
              _.lessThanColumnKey(_.sponsoredMetrics, SponsoredMetricsKey.partialByStartDate(startHour)),
              _.allInFamilies(_.meta)
            )
          )
        ) match {
          case Success(camp) => camp
          case Failure(fails) => return fails.failure
        }
    }

    // If this is an organic campaign, make sure we ignore further check and return now
    if (campaign.isOrganic) return CampaignStatusValidationResponse.organicNoChangeRequiredSuccessNel

    val currentStatus = campaign.status

    def changeStatus(msg: String, toStatus: CampaignStatus.Type, budgetResOpt: Option[BudgetResult] = None): ValidationNel[FailureResult, CampaignStatusValidationResponse] = {
      if (currentStatus == toStatus) return CampaignStatusValidationResponse(msg, currentStatus, toStatus).successNel

      info("Changing campaign with key: " + campaignKey + " status from: " + currentStatus + " to: " + toStatus)

      for (budgetResult <- budgetResOpt) {
        try {
          info("Sending campaign budget alert email for campaign: " + campaignKey)
          CampaignNotifications.sendIfProduction(CampaignBudgetExceededNotification(campaignKey, budgetResult))
        } catch {
          case ex: Exception => critical("Campaign budget email exception", ex)
        }
      }

      modify(campaignKey)(_.value(_.status, toStatus)) match {
        case Success(putSpec) =>
          val auditKeyOption = performPostStatusChangeActions(campaignKey, currentStatus, toStatus, Settings2.INTEREST_SERVICE_USER_ID, checkedBy)
          CampaignStatusValidationResponse(msg, currentStatus, toStatus, putSpec.some, auditKeyOption).successNel
        case Failure(fails) => fails.failure
      }
    }

    val scheduleValidation = campaign.validateSchedule()

    val budgetValidation = campaign.validateBudget(isOrganic, checkedBy)

    currentStatus match {
      case CampaignStatus.active => scheduleValidation match {
        case Success(res) => res match {
          case TooEarly => changeStatus("Active Ongoing Campaigns Before Their Time", CampaignStatus.approved)
          case TooLate => changeStatus("Active Campaigns After Their Time", CampaignStatus.completed)
          case Within => budgetValidation match {
            case Success(budgetRes) => if (budgetRes.isWithinBudget) {
              changeStatus("Active campaigns left as-is", currentStatus)
            } else {
              val msg = "Active Campaigns that " + budgetRes.maxReachedMessage
              budgetRes.maxSpendType match {
                case MaxSpendTypes.daily => changeStatus(msg, CampaignStatus.dailyCapped, budgetRes.some)
                case MaxSpendTypes.weekly => changeStatus(msg, CampaignStatus.weeklyCapped, budgetRes.some)
                case MaxSpendTypes.monthly => changeStatus(msg, CampaignStatus.monthlyCapped, budgetRes.some)
                case MaxSpendTypes.total => changeStatus(msg, CampaignStatus.completed, budgetRes.some)
              }
            }
            case Failure(msg) => changeStatus(msg, CampaignStatus.pending)
          }
        }
        case Failure(msg) => changeStatus(msg, CampaignStatus.pending)
      }
      case CampaignStatus.approved => scheduleValidation match {
        case Success(res) => res match {
          case TooEarly => changeStatus("Approved Campaigns Before Their Time", currentStatus)
          case TooLate => changeStatus("Approved Campaigns After Their Time", CampaignStatus.completed)
          case Within => budgetValidation match {
            case Success(budgetRes) => if (budgetRes.isWithinBudget) {
              changeStatus("Approved Campaigns Within Their Time", CampaignStatus.active)
            } else {
              val msg = "Approved Campaigns that " + budgetRes.maxReachedMessage
              budgetRes.maxSpendType match {
                case MaxSpendTypes.daily => changeStatus(msg, CampaignStatus.dailyCapped, budgetRes.some)
                case MaxSpendTypes.weekly => changeStatus(msg, CampaignStatus.weeklyCapped, budgetRes.some)
                case MaxSpendTypes.monthly => changeStatus(msg, CampaignStatus.monthlyCapped, budgetRes.some)
                case MaxSpendTypes.total => changeStatus(msg, CampaignStatus.completed, budgetRes.some)
              }
            }
            case Failure(msg) => changeStatus(msg, CampaignStatus.pending)
          }
        }
        case Failure(msg) => changeStatus(msg, CampaignStatus.pending)
      }
      case CampaignStatus.dailyCapped | CampaignStatus.weeklyCapped | CampaignStatus.monthlyCapped => scheduleValidation match {
        case Success(res) => res match {
          case TooEarly => changeStatus("Capped Campaigns Before Their Time", CampaignStatus.approved)
          case TooLate => changeStatus("Capped Campaigns After Their Time", CampaignStatus.completed)
          case Within => budgetValidation match {
            case Success(budgetRes) => if (budgetRes.isWithinBudget) {
              changeStatus("Capped Campaigns Within Their Time", CampaignStatus.active)
            } else {
              budgetRes.maxSpendType match {
                case MaxSpendTypes.daily |
                     MaxSpendTypes.weekly |
                     MaxSpendTypes.monthly => changeStatus("Capped Campaigns left as-is", currentStatus)
                case MaxSpendTypes.total => changeStatus("Capped Campaigns that spent total budget", CampaignStatus.completed, budgetRes.some)
              }

            }
            case Failure(msg) => changeStatus(msg, CampaignStatus.pending)
          }
        }
        case Failure(msg) => changeStatus(msg, CampaignStatus.pending)
      }
      case CampaignStatus.paused => scheduleValidation match {
        case Success(res) => res match {
          case TooEarly => changeStatus("Paused Campaigns Before Their Time", currentStatus)
          case TooLate => changeStatus("Paused Campaigns After Their Time", CampaignStatus.completed)
          case Within => changeStatus("Paused Campaigns Within Their Time", currentStatus)
        }
        case Failure(msg) => changeStatus(msg, currentStatus)
      }
      case notHandled => changeStatus("CampaignStatus." + notHandled + " is not handled.", currentStatus)
    }
  }

  def verifyScheduleAndBudgetWithUpdate(campaignKey: CampaignKey): ValidationNel[FailureResult, Boolean] = {
    validateStatusAgainstScheduleAndBudget(campaignKey, checkedBy = "Role") match {
      case Success(result) => result.putSpecOption match {
        case Some(spec) => put(spec) match {
          case Success(_) =>
            result.auditKeyOption.foreach(auditKey => AuditService.modifyPut(auditKey)(_.value(_.status, AuditStatus.succeeded)))
            result.isStillActive.successNel
          case Failure(fails) =>
            result.auditKeyOption.foreach(auditKey => AuditService.modifyPut(auditKey)(_.value(_.status, AuditStatus.failed)))
            fails.failure
        }
        case None => result.isStillActive.successNel
      }
      case Failure(fails) => fails.failure
    }
  }

  def isActive(campaignKey: CampaignKey, skipCache: Boolean = false): Boolean = campaignMeta(campaignKey, skipCache) match {
    case Some(camp) =>
      if (camp.isActive) {
        true
      } else {
        camp.nameOption match {
          case Some(name) if name == organicCampaignName => true
          case _ => false
        }
      }
    case None => false
  }

  private def campaignMetaQuery = Schema.Campaigns.query2.withFamilies(_.meta, _.trackingParams, _.blacklistedSiteSettings,
    _.blacklistedCampaignSettings, _.blacklistedArticleSettings, _.blacklistedUrlSettings, _.blacklistedKeywordSettings, _.walkBackSessionInfo)

  private val shouldGetFromRemote: Boolean = (Settings2.getBooleanOrDefault("GetFromRemote.AllCampaignMeta", false) && !grvroles.isInRole(grvroles.REMOTE_RECOS) && !RemoteOperationsHelper.isUnitTest)
  if (shouldGetFromRemote) RemoteOperationsHelper.registerReplyConverter(CampaignMetaResponseConverter)

  def allCampaignMetaObj: AllCampaignMetaObj = {

    def factoryFun() = {
      val map =
        if (shouldGetFromRemote) {
          ProductionRemoteOperationsClient.requestResponse[CampaignMetaRequest, CampaignMetaResponse](CampaignMetaRequest(), 15.seconds, Some(CampaignMetaRequestConverter)) match {
            case Success(response) => response.rowMap
            case Failure(fails) =>
              warn("Failed to get campaign meta from remote: " + fails + ". Falling back to scan.")
              campaignMetaQuery.scanToIterable(row => (row.campaignKey, row)).toMap
          }
        }
        else
          campaignMetaQuery.scanToIterable(row => (row.campaignKey, row)).toMap

      AllCampaignMetaObj(map)
    }

    PermaCacher.getOrRegister(
      "CampaignService.campaignMeta", PermaCacher.retryUntilNoThrow(factoryFun), 5 * 60 /*, resourceType = "campaign"*/)
  }

  // Returns Some(newStamp, AllCampaignMetaObj) if a newer version than oldStamp is available, else None.
  def stampedAcmoIfNewer(oldStamp: Long): Option[(Long, AllCampaignMetaObj)] =
    allCampaignMetaObjIfNewStamp(oldStamp).map(acmo => (acmo.stamp, acmo))

  def allCampaignMetaObjIfNewStamp(stamp: Long) = {
    allCampaignMetaObj match {
      case acmo if acmo.stamp != stamp => Option(acmo)
      case _ => None
    }
  }

  def allCampaignMeta = allCampaignMetaObj.allCampaignMeta

  def allCampaignMetaSerialized = allCampaignMetaObj.serialized

  def allCampaignMetaBySiteKey = allCampaignMetaObj.bySiteKey

  def campaignMeta(campaignKey: CampaignKey, skipCache: Boolean = false): Option[CampaignRow] = {
    if (Settings.ENABLE_SITESERVICE_META_CACHE) {
      // In a production environment, cache the entire campaign meta as a map lookup
      allCampaignMeta.get(campaignKey)
    } else {
      // In other environments, cache single values at a time.  (Changed this from no cache because there are tight-loop lookups)
      campaignMetaQuery.withKey(campaignKey).singleOption(skipCache = skipCache)
    }
  }

  def campaignMeta(campaignKeys: Set[CampaignKey], skipCache: Boolean): Map[CampaignKey, CampaignRow] = {
    if (Settings.ENABLE_SITESERVICE_META_CACHE) {
      // In a production environment, cache the entire campaign meta as a map lookup
      campaignKeys.flatMap(ck => allCampaignMeta.get(ck) match {
        case Some(cr) => Some((ck, cr))
        case _ => None
      }).toMap
    } else {
      // In other environments, cache single values at a time.  (Changed this from no cache because there are tight-loop lookups)
      campaignMetaQuery.withKeys(campaignKeys).executeMap(skipCache = skipCache)
    }
  }

  def addRecentArticle(articleKey: ArticleKey, recentArticles: Map[PublishDateAndArticleKey, ArticleKey]): ValidationNel[FailureResult, Boolean] = {
    for {
      article <- ArticleService.fetch(articleKey, skipCache = false)(_.withColumns(_.url).withFamilies(_.campaigns))
      incompleteCampaignKeys = article.campaigns.filterNot(_._2.id == CampaignStatus.completed.id).keySet
      opsResult <- incompleteCampaignKeys.headOption match {
        case Some(first) =>
          val mod = Schema.Campaigns.put(first).valueMap(_.recentArticles, recentArticles)
          incompleteCampaignKeys.tail.foreach(next => mod.put(next).valueMap(_.recentArticles, recentArticles))
          put(mod)
        case None => emptyOpsResult.successNel
      }
    } yield {
      com.gravity.utilities.Counters.countPerSecond(counterCategory, "Recent Article Added to Campaign", opsResult.numPuts)
      opsResult.numPuts > 0
    }
  }

  //NOTE: this method does leaves references to the passed campaignKey in any articles it may have been ingested into.
  // you probably want to run CampaignRecentArticlesAndSettingsCleanseJob after running this method.
  // (first run without args to see the report, then use printReport:false to do the deletes).
  def youReallyShouldNotDeleteACampaignBut(campaignKey: CampaignKey): ValidationNel[FailureResult, OpsResult] = {
    println("WARNING!! Somebody called the method they really shouldn't and so now I'm going to delete campaign: " + campaignKey)

    // first retrieve meta so we know what we're dealing with
    val campaign = fetch(campaignKey)(_.withFamilies(_.meta)) valueOr {
      fails: NonEmptyList[FailureResult] =>
        println("Failed to fetch campaign meta for: " + campaignKey)
        return fails.failure
    }

    // going to need the site as well
    val site = SiteService.fetchSiteForManagement(campaignKey.siteKey) valueOr {
      fails: NonEmptyList[FailureResult] =>
        println("Failed to fetch the site for: " + campaignKey)
        return fails.failure
    }

    val (campName, siteName) = (campaign.nameOrNotSet, site.nameOrNoName)

    println("We'll now delete campaign '" + campName + "' (" + campaignKey + ") from site '" + siteName + "' (guid: " + site.siteGuidOrNoGuid + ", siteId: " + campaignKey.siteKey.siteId + ")...")

    try {
      val del = Schema.Sites.delete(campaignKey.siteKey).values(_.campaigns, Set(campaignKey))
      campaign.nameOption.foreach(name => del.values(_.campaignsByName, Set(name))) // only delete the name is it is actually present in the campaign
      del.execute()
    } catch {
      case ex: Exception =>
        val msg = "Failed to delete campaign " + campaignKey + " from sites table!"
        println(msg)
        return FailureResult(msg, ex).failureNel
    }

    println("We'll now delete campaign '" + campName + "' (" + campaignKey + ") from campaigns...")

    try {
      Schema.Campaigns.delete(campaignKey).execute().successNel
    } catch {
      case ex: Exception =>
        val msg = "Failed to delete campaign " + campaignKey + " from campaigns table!"
        println(msg)
        FailureResult(msg, ex).failureNel
    }
  }

  def performPostStatusChangeActions(campaignKey: CampaignKey, fromStatus: CampaignStatus.Type, toStatus: CampaignStatus.Type, userId: Long = -1l, checkedBy: String): Option[AuditKey2] = {
    if (fromStatus == toStatus) return None // nothing to see here. move along...

    //warn about any failures
    val auditKeyOption = AuditService.logEvent(campaignKey, userId, AuditEvents.campaignStatusChange, "status", fromStatus, toStatus, Seq("Campaign Service - status change by " + checkedBy)) match {
      case Success(auditRow) => auditRow.auditKey.some
      case Failure(fails) =>
        warn(fails)
        None
    }

    // if we have one of the following status transitions, we will send a campaign notification
    (fromStatus, toStatus) match {
      case (CampaignStatus.active, CampaignStatus.paused) |
           (CampaignStatus.active, CampaignStatus.completed) |
           (CampaignStatus.paused, CampaignStatus.active) => CampaignNotifications.sendIfProduction(CampaignStatusChangedNotification(campaignKey, fromStatus, toStatus, userId))
      case _ => // do nothing
    }

    SiteService.modifyPut(campaignKey.siteKey)(_.valueMap(_.campaigns, Map(campaignKey -> toStatus))) match {
      case Success(result) =>
      case Failure(fails) => warn(fails)
    }

    updateArticlesForCampaign(campaignKey, toStatus) match {
      case Success(result) =>
      case Failure(fails) => warn(fails)
    }

    if (toStatus == CampaignStatus.active || fromStatus == CampaignStatus.active) {
      PermaCacher.clearResultFromCache(activeCampaignCacheKey)
    }

    auditKeyOption
  }

  def updateStatus(campaignKey: CampaignKey, status: CampaignStatus.Type): ValidationNel[FailureResult, OpsResult] = updateStatus(campaignKey, status, -1l)

  def updateStatus(campaignKey: CampaignKey, status: CampaignStatus.Type, userId: Long): ValidationNel[FailureResult, OpsResult] = updateStatus(campaignKey, status, userId, None)

  def updateStatus(campaignKey: CampaignKey, status: CampaignStatus.Type, userId: Long, preLoadedCampaignMeta: Option[CampaignRow]): ValidationNel[FailureResult, OpsResult] = {
    val existing = preLoadedCampaignMeta.getOrElse {
      fetch(campaignKey)(_.withFamilies(_.meta)) match {
        case Success(camp) => camp
        case Failure(fails) => return NonEmptyList(FailureResult("Failed to check existing status before updating!"), fails.list: _*).failure
      }
    }

    // only go through with this if the status is actually changing.
    val existingStatus = if (existing.status == status) {
      return OpsResult(0, 0, 0).successNel
    } else {
      existing.status
    }

    for {
      ops <- modifyPut(campaignKey)(_.value(_.status, status).valueMap(_.statusHistory, Map(new DateTime -> CampaignStatusChange(userId, existingStatus, status))))
    } yield {
      performPostStatusChangeActions(campaignKey, existingStatus, status, userId, "Service")
      ops
    }

  }

  def validateCampaignName(ck: CampaignKey, name: String, isCreate: Boolean): ValidationNel[FailureResult, String] = if (ScalaMagic.isNullOrEmpty(name)) {
    FailureResult("campaignName MUST be NON-Empty!").failureNel
  } else {
    for {
      site <- SiteService.fetch(ck.siteKey, skipCache = true)(_.withColumns(_.guid).withFamilies(_.campaignsByName))
      testedName <- site.campaignsByName.get(name) match {
        case Some(existing) if existing == ck => name.successNel
        case Some(dupe) => CampaignNameAlreadyExistsFailure(name, ck, dupe).failureNel
        case None => SiteService.modifyPut(ck.siteKey)(_.valueMap(_.campaignsByName, Map(name -> ck))) match {
          case Success(_) => {
            if (!isCreate) {
              // Remove the old name from the campaignsByName map
              val existingName = site.campaignsByName.find { case (n, key) =>
                ck == key
              }
              existingName.foreach { case (n, ck) => // delete the existing name since we are replacing it.
                SiteService.deleteFromSite(ck.siteKey)(_.values(_.campaignsByName, Set(n)))
              }
              name.successNel
            }
            else
              name.successNel
          }
          case Failure(fails) => toNonEmptyList(List(FailureResult("Failed to verify and set campaign name: '" + name + "' for campaign: " + ck.toString)) ++ fails.list).failure
        }
      }
    } yield testedName
  }

  def validateCampaignType(campaignRowOption: Option[CampaignRow], campaignType: Option[CampaignType.Type]): ValidationNel[FailureResult, Unit] = {
    if (campaignType.isDefined && campaignRowOption.exists(_.campaignType != campaignType.get)) {
      FailureResult("Cannot change campaign type after campaign has been created").failureNel
    } else {
      ().successNel
    }
  }

  def validateComscorePixelConfig(beforeRow: Option[CampaignRow], enableComscorePixel: Option[Boolean],
                                  ioId: Option[String] = None): ValidationNel[FailureResult, Unit] = {
    val comscorePixelEffectivelyEnabled = enableComscorePixel.exists(_ == true) || (
      beforeRow.exists(_.enableComscorePixel) && enableComscorePixel.isEmpty
      )

    val ioIdEffectivelyEmpty = ioId.exists(_.isEmpty) || (ioId.isEmpty && beforeRow.exists(_.ioId.isEmpty))

    if (comscorePixelEffectivelyEnabled && ioIdEffectivelyEmpty)
      FailureResult("ioId is required if using comScore pixel.").failureNel
    else
      ().successNel
  }

  private def siteCampaignsByKey(siteKey: SiteKey, skipCache: Boolean = true, siteMetricsRange: Option[DateMidnightRange] = None)(query: QuerySpec): ValidationNel[FailureResult, CampaignsWithSponsoredMetrics] = {
    def filterMetrics(metrics: Map[SponsoredMetricsKey, Long]): Map[SponsoredMetricsKey, Long] = siteMetricsRange match {
      case Some(range) => metrics.filterKeys(k => range.contains(k.dateHour.toGrvDateMidnight))
      case None => metrics
    }

    for {
      site <- SiteService.fetch(siteKey)(qb => {
        val q = qb.withFamilies(_.meta, _.campaigns)

        siteMetricsRange.foreach(range => {
          val minKey = SponsoredMetricsKey.partialByStartDate(range.toHourRange.fromHour.minusHours(1))
          //          val maxKey = SponsoredMetricsKey.partialByEndDate(range.toHour)
          q.withFamilies(_.sponsoredDailyMetrics).filter(
            _.or(_.lessThanColumnKey(_.sponsoredDailyMetrics, minKey), _.allInFamilies(_.meta, _.campaigns))
          )
        })

        q
      })
      campaignKeys = {
        val cks = site.familyKeySet(_.campaigns)
        if (cks.isEmpty) {
          return CampaignsWithSponsoredMetrics(Map.empty[CampaignKey, CampaignRow], filterMetrics(site.sponsoredDailyMetrics)).successNel
        }
        cks
      }
      campaigns <- fetchMulti(campaignKeys)(query)
    } yield CampaignsWithSponsoredMetrics(campaigns, filterMetrics(site.sponsoredDailyMetrics))
  }

  def siteCampaigns(siteGuid: String, skipCache: Boolean = true, siteMetricsRange: Option[DateMidnightRange] = None)(query: QuerySpec): ValidationNel[FailureResult, CampaignsWithSponsoredMetrics] = {
    siteCampaignsByKey(SiteKey(siteGuid), skipCache, siteMetricsRange)(query)
  }

  def addArticleKey(ck: CampaignKey,
                    ak: ArticleKey,
                    userId: Long = -1l,
                    settings: Option[CampaignArticleSettings] = None
                   ): ValidationNel[FailureResult, CampaignRow] = {
    for {
      // Save the appropriate (human vs. S3) image, depending if S3-saving is enabled for the campaign's site.
      saver <- ImageCachingService.getOptCasSaver(ck.siteKey, settings, "When adding an ArticleKey to a Campaign")

      // Add the article to the campaign.
      campRow <- addArticleKeyWithoutImageFixUp(ck, ak, userId, saver.optCas)

      // Only request the image fix-up after the article and campaign have been updated (otherwise there's a race condition).
      fixupRequested <- ImageCachingService.schedOptCasSaverFixUp(saver, CampaignArticleKey(ck, ak))
    } yield {
      campRow
    }
  }

  def logCampaignArticleSettingsUpdate(caKey: CampaignArticleKey,
                                       oldSettingsIfAny: Option[CampaignArticleSettings],
                                       newSettings: CampaignArticleSettings,
                                       userId: Long = -1L
                                      ): ValidationNel[FailureResult, Option[AuditRow2]] = {
    if (oldSettingsIfAny != newSettings.some)
      AuditService.logEvent(caKey, userId, AuditEvents.campaignArticleSettingsUpdate, "campaignArticleSettings", oldSettingsIfAny, newSettings, Seq("Campaign Service - add/update article")).map(_.some)
    else
      None.successNel
  }

  private def addArticleKeyWithoutImageFixUp(ck: CampaignKey,
                                             ak: ArticleKey,
                                             userId: Long = -1l,
                                             settings: Option[CampaignArticleSettings] = None
                                            ): ValidationNel[FailureResult, CampaignRow] = {
    settings.foreach(cas => {
      validateUrl(cas.image) valueOr {
        failure => return ParamValidationFailure("image", ValidationCategory.Other, "Image Url is invalid").failureNel
      }
    })

    settings.foreach(cas => {
      validateUrl(cas.clickUrl) valueOr {
        failure => return ParamValidationFailure("clickUrl", ValidationCategory.Other, "Click Url is invalid").failureNel
      }
    })

    //title.foreach(t => if (t.trim.isEmpty) return ParamValidationFailure("title", ValidationCategory.Other, "Title is required").failureNel)

    // if we are creating a new campaign article, these will be the default initial settings for any parameters not passed in the request
    val initialSettings = new CampaignArticleSettings()

    for {
      article <- ArticleService.fetch(ak)(CampaignRecoRequirements.articleQuerySpecNoFilterAllowed(_.withFamilies(_.meta, _.campaignSettings)))
      existingCampaign <- fetchOrEmptyRow(ck)(loggingFetchQuerySpecWithRecentArticle(PublishDateAndArticleKey(article.publishTime, ak)))
      prevSettings = article.campaignSettings.get(ck)
      defaultSettings = prevSettings.getOrElse(initialSettings)
      newIsBlacklisted = settings.fold(defaultSettings.isBlacklisted)(_.isBlacklisted)
      newStatus = if (!newIsBlacklisted) settings.fold(defaultSettings.status)(_.status) else CampaignArticleStatus.inactive
      newClickUrl = {
        // campaign's default click url takes precedence (see Spencer), then the passed settings clickUrl (even if None) or lastly the defaultSettings clickUrl
        existingCampaign.defaultClickUrl.map(_.toString) <+> settings.map(_.clickUrl).getOrElse(defaultSettings.clickUrl)
      }
      newTitle = settings.flatMap(_.title).orElse(defaultSettings.title).filter(_.nonEmpty)
      newImage = settings.flatMap(_.image).orElse(defaultSettings.image).filter(_.nonEmpty)
      newDomain = settings.flatMap(_.displayDomain).orElse(defaultSettings.displayDomain).filter(_.nonEmpty)
      newArticleImpressionPixel = settings.flatMap(_.articleImpressionPixel).orElse(defaultSettings.articleImpressionPixel).filter(_.nonEmpty)
      newArticleClickPixel = settings.flatMap(_.articleClickPixel).orElse(defaultSettings.articleClickPixel).filter(_.nonEmpty)
      newTrackingParams = settings.fold(Map.empty[String, String])(_.trackingParams)
      casEmptyBlockedAndWhy = CampaignArticleSettings(status = newStatus, isBlacklisted = newIsBlacklisted,
        clickUrl = newClickUrl, title = newTitle, image = newImage, displayDomain = newDomain,
        articleImpressionPixel = newArticleImpressionPixel, articleClickPixel = newArticleClickPixel, trackingParams = newTrackingParams)
      newSettings = existingCampaign.campRecoRequirementsOpt.map(_.newCasWithBlockedAndWhy(article, casEmptyBlockedAndWhy)).getOrElse(casEmptyBlockedAndWhy)
      _ <- articleService.modifyPut(ak)(_.valueMap(_.campaigns, Map(ck -> existingCampaign.status)).valueMap(_.campaignSettings, Map(ck -> newSettings)))
      _ <- logCampaignArticleSettingsUpdate(CampaignArticleKey(ck, ak), prevSettings, newSettings, userId)
      _ <- if (newIsBlacklisted && defaultSettings.status == CampaignArticleStatus.inactive && settings.exists(_.status == CampaignArticleStatus.active)) {
        return ParamValidationFailure("status", ValidationCategory.Other, "Cannot make blacklisted article active.").failureNel
      } else {
        true.successNel
      }
      _ <- {
        if (CampaignService.pubTimeIsWithinTtl(existingCampaign.recentArticlesMaxAge, article.publishTime)) {
          modifyPut(ck)(_.valueMap(_.recentArticles, Map(PublishDateAndArticleKey(article.publishTime, ak) -> ak)).valueMap(_.recentArticleSettings, Map(ak -> newSettings)))
        } else {
          true.successNel
        }
      }
      updated <- fetch(ck)(loggingFetchQuerySpecWithRecentArticle(PublishDateAndArticleKey(article.publishTime, ak)))
    } yield {
      updated
    }

  }


  def addArticleUrl(ck: CampaignKey,
                    url: String,
                    userId: Long = -1l,
                    settings: Option[CampaignArticleSettings] = None,
                    flags: Seq[AddArticleFlags.Type] = Seq(),
                    scrubUrl: Boolean = true)

  : ValidationNel[FailureResult, CampaignArticleAddResult] = {

    for {
    // Save the appropriate (human vs. S3) image, depending if S3-saving is enabled for the campaign's site.
      saver <- ImageCachingService.getOptCasSaver(ck.siteKey, settings, "When adding a Article URL to a Campaign")

      scrubbedUrl <- if (scrubUrl) UrlScrubber.scrubUrl(url, ScrubberEnum.canonicalize, (_ => true), settings.flatMap(_.title), settings.flatMap(_.image), None) else url.successNel

      // Add the article to the campaign.
      addResult <- addArticleUrlWithoutImageFixUp(ck, scrubbedUrl, userId, saver.optCas, flags)

      // Only request the image fix-up after the article and campaign have been updated (otherwise there's a race condition).
      fixupRequested <- ImageCachingService.schedOptCasSaverFixUp(saver, CampaignArticleKey(ck, addResult.article.articleKey))
    } yield {
      addResult
    }
  }

  private def addArticleUrlWithoutImageFixUp(ck: CampaignKey,
                                             articleUrl: String,
                                             userId: Long = -1l,
                                             casSettings: Option[CampaignArticleSettings] = None,
                                             flags: Seq[AddArticleFlags.Type] = Seq.empty): ValidationNel[FailureResult, CampaignArticleAddResult] = {
    validateUrl(articleUrl.some) valueOr {
      failure => return ParamValidationFailure("url", ValidationCategory.Other, "Url was invalid.").failureNel
    }

    def crawlAndSaveArticle: ValidationNel[FailureResult, (ArticleRow, Boolean)] = {
      var artSaver = OptImgSaver(false, None)

      def skipCrawlAndSaveEmptyArticle(): ValidationNel[FailureResult, ArticleRow] = {
        for {
          articleSiteGuid <- SiteGuidService.findOrGenerateSiteGuidByUrl(articleUrl)
          _ <- casSettings.find(_.title.isDefined).toValidationNel(FailureResult("The article crawl cannot be skipped if there is no title given."))

          putSpec <- ArticleIngestionFields(articleUrl, articleSiteGuid, new DateTime(), ArticleTypes.content, OptionalArticleFields.empty)
            .generatePut(IngestionTypes.fromAccountManager)

          ar <- ArticleService.saveArticle(url = articleUrl,
            sourceSiteGuid = articleSiteGuid,
            ingestionSource = IngestionTypes.fromAccountManager,
            publishDateOption = new DateTime().some
          )(_ => putSpec.spec, query = _.withFamilies(_.meta).withColumn(_.content))
        } yield ar
      }

      def forceCrawlAndSaveArticle(): ValidationNel[FailureResult, ArticleRow] = {
        for {
          articleSiteGuid <- SiteGuidService.findOrGenerateSiteGuidByUrl(articleUrl)
          _ = trace(s"AddArticleUrl crawling $articleUrl")
          crawlArticle <- Try(ContentUtils.fetchAndExtractMetadata(articleUrl, withImages = true, publishTimeOverride = new DateTime().some)).toValidationNel[FailureResult]((ex: Throwable) => FailureResult(ex.getMessage, ex))
          ingestedArticle <- ArticleIngestionFields.fromGoose(crawlArticle, articleSiteGuid, ArticleTypes.content)

          // Save the appropriate (human vs. S3) image, depending if S3-saving is enabled for the campaign's site.
          saver <- ImageCachingService.getOptImgSaver(ck.siteKey, ingestedArticle.optionalFields.image, "When adding a Goose-crawled Article URL to a Campaign")
          _ = artSaver = saver

          ingestedArticleModified <- ingestedArticle.copy(
            url = articleUrl,
            optionalFields = ingestedArticle.optionalFields.copy(image = saver.optImgStr)
          ).successNel

          putSpec <- ingestedArticleModified.generatePut(IngestionTypes.fromAccountManager)

          ar <- ArticleService.saveArticle(url = articleUrl,
            sourceSiteGuid = articleSiteGuid,
            ingestionSource = IngestionTypes.fromAccountManager,
            publishDateOption = new DateTime().some
          )(_ => putSpec.spec, query = _.withFamilies(_.meta).withColumn(_.content))
        } yield ar
      }

      // See if we have an "adequate" article (which currenlty just means that it already exists).
      def fetchAdequateArticle() = {
        for {
          articleRow <- ArticleService.fetch(ArticleKey(articleUrl))(_.withFamilies(_.meta).withColumn(_.content))
        //          _ <- casSettings.find(_.title.isDefined).toValidationNel(FailureResult("The article does not have a saved title."))
        } yield {
          articleRow
        }
      }

      var failedCrawlButAdded = false

      for {
        articleRow <- if (flags.contains(AddArticleFlags.forceCrawl)) {
          // Re-crawl the article, whether or not it already exists.
          forceCrawlAndSaveArticle()
        } else {
          // No force-crawl requested. Use the existing article if possible.
          fetchAdequateArticle() orElse {
            // No existing article. If skipCrawl is specified, we'll just create an empty article, otherwise that's an error.
            if (flags.contains(AddArticleFlags.skipCrawl)) {
              skipCrawlAndSaveEmptyArticle()
            } else {
              // No skipCrawl, so try to crawl the article.
              forceCrawlAndSaveArticle() match {
                case yay@Success(_) => yay

                case boo@Failure(_) =>
                  // Crawl Failed. If requested, force the creation of the article by saving an empty article, else complain.
                  if (flags.contains(AddArticleFlags.failCrawlOk)) {
                    failedCrawlButAdded = true
                    skipCrawlAndSaveEmptyArticle()
                  } else {
                    boo
                  }
              }
            }
          }
        }

        // Only request the image fix-up after the article has been updated (otherwise there's a race condition).
        artFixUpRequested <- ImageCachingService.schedOptImgSaverFixUp(artSaver, articleRow.articleKey)
      } yield (articleRow, failedCrawlButAdded)
    }

    crawlAndSaveArticle match {
      case Success((articleRow, failedCrawlButAdded)) =>
        val beforeCampaignRow = fetch(ck)(loggingFetchQuerySpecWithRecentArticle(PublishDateAndArticleKey(articleRow.publishTime, articleRow.articleKey)))

        // Let's figure out what we're going to use for the CAS's clickUrl override.
        val finalClickUrl = {
          // Get the clickUrl override that came in on the casSettings.
          val casClickUrl = casSettings.flatMap(_.clickUrl)

          // If an override was defined in the CAS, we take it as gospel...
          casClickUrl.orElse {
            // ...otherwise, only set the clickUrl if the raw and scrubbed URLs were different.
            if (articleUrl.trim != articleRow.url) articleUrl.some else None
          }
        }

        for {
          opsResult <- addArticleKey(ck, articleRow.articleKey, userId, casSettings.map(_.copy(clickUrl = finalClickUrl)))
          campaign <- fetch(ck)(loggingFetchQuerySpecWithRecentArticle(PublishDateAndArticleKey(articleRow.publishTime, articleRow.articleKey)))
          artSettings <- ArticleService.fetch(articleRow.articleKey)(_.withFamilies(_.campaignSettings))
            .flatMap(_.campaignSettings.get(ck).toValidationNel(FailureResult("unable to refetch campaign article settings")))
        } yield {
          AuditService.logChangeLossy(ck, AuditEvents.articleUrlListAdd, userId, beforeCampaignRow, campaign)
          CampaignArticleAddResult(articleRow, ck, artSettings, articleRow.siteGuid, failedCrawlButAdded)
        }

      case Failure(failure) => ParamValidationFailure("url", ValidationCategory.Other, failure.list.mkString(", ")).failureNel
    }
  }

  def validateUrl(url: Option[String]): ValidationNel[FailureResult, Option[String]] = {
    import com.gravity.utilities.grvstrings._

    url match {
      case Some(mt) if mt.isEmpty => None.success
      case Some(u) => u.tryToURL match {
        case Some(_) => url.success
        case None => FailureResult("Invalid url: `" + u + "`!").failureNel
      }
      case None => None.success
    }
  }

  def addArticleOnlyIfNew(ck: CampaignKey, key: PublishDateAndArticleKey, settings: Option[CampaignArticleSettings]): ValidationNel[FailureResult, Boolean] = withMaintenance {
    val campaignVNel = try {
      fetch(ck) {
        _.withColumns(_.siteGuid, _.status)
          .withColumn(_.recentArticles, key)
      }
    } catch {
      // Seeing an infrequent but peculiar Exception when calling fetch above. Collect info on what we were doing.
      case ex: Exception =>
        throw new Exception(s"Exception in addArticleOnlyIfNew (ck=`$ck`, key=`$key`) calling fetch.", ex)
    }

    for {
      campaign <- campaignVNel

      isNew = !campaign.recentArticleKeys.contains(key.articleKey)

      opsResultVNel = try {
        addArticleKey(ck, key.articleKey, settings = settings)
      } catch {
        case ex: Exception =>
          throw new Exception(s"Exception in addArticleOnlyIfNew (ck=`$ck`, key=`$key`) calling addArticleKey.", ex)
      }

      opsResult <- opsResultVNel
    } yield {
      true
    }
  }

  /**
    * @note cr needs to at least have the family 'meta'
    */
  def updateCampaignArticleSettings(cr: CampaignRow,
                                    articleKeys: Set[ArticleKey],
                                    updateFunction: (CampaignArticleSettings) => CampaignArticleSettings
                                   ): ValidationNel[FailureResult, OpsResult] = {

    for {
      articleRows <- ArticleService.fetchMulti(articleKeys)(CampaignRecoRequirements.articleQuerySpecNoFilterAllowed(_.withFamilies(_.campaignSettings)))
      ck = cr.campaignKey
      campRecoRequirements = cr.campRecoRequirementsOrEmptyReq
      // map over each article row, extract the existing campaign article settings, map each over our update functions
      newArticleSettings <- (for {
        (ak, ar) <- articleRows
        oldCas <- ar.campaignSettings.get(ck)
      } yield {
        val updatedCas = updateFunction(oldCas)
        val newCas = campRecoRequirements.newCasWithBlockedAndWhy(ar, updatedCas)
        ak -> newCas
      }).toMap.successNel
      // update each article with the new settings. Do articles first in case we go down while updating, articles is the expected source of truth.
      // Here we map the update calls to PutOps and combine them into a single PutOp for execution.
      articlesResult <- ArticleService.seqToCombinedPutOp(newArticleSettings.toSeq)( _._1, { case (ak, newCas) => Schema.Articles.put(ak).valueMap(_.campaignSettings, Map(ck -> newCas)) })
        .map(v => ArticleService.put(v)).getOrElse(OpsResult(0, 0, 0).successNel)
      // update the campaign's recent articles with the new settings
      campaignArticlesResult <- modifyPut(ck)(_.valueMap(_.recentArticleSettings, newArticleSettings))
    } yield {
      articlesResult
    }

  }

  // This parses well-formed RSS Feed ingestion notes written by RssArticleIngestionActor.saveRssArticle()
  private val feedViaRegex =
    """feed: (\S*)\s*via (\w*): (\S*).*""".r("feedUrl", "viaKeyType", "valKeyVal")

  def toOptNotesInfo(tup: (ArticleIngestionKey, ArticleIngestionData)): (Option[String], Option[String], Option[String]) = {
    //AolContentFeedService notes are just the URL
    def feedViaHttpPrefixOrRegex(ingestionNotes: String) = {
      if (ingestionNotes.startsWith("http"))
        (Option(ingestionNotes), None, None)
      else {
        feedViaRegex findPrefixOf ingestionNotes match {
          case Some(feedViaRegex(parsedFeedUrl, parsedWhereType, parsedWhereVal)) =>
            (Option(parsedFeedUrl), Option(parsedWhereType), Option(parsedWhereVal))
          case _ => (None, None, None)
        }
      }
    }

    val (artIngKey, artIngData) = tup

    if (artIngKey.ingestionType == IngestionTypes.fromRss)
      feedViaHttpPrefixOrRegex(artIngData.ingestionNotes)
    else
      (None, None, None)
  }

  /**
    * For the given CampaignRow whose RSS Feed's active/inactive status is changing TO newStatus,
    * return the Set of ArticleKeys whose status in the campaign should also be changed to newStatus.
    *
    * Detail: An existing RSS Feed's active/inactive RssFeedSettings.feedStatus is being changed, and the user has requested
    * that the feed's articles in the campaign have their active/inactive CampaignArticleSettings.status updated similarly.
    *
    * If newStatus=active, then set the CampaignArticleSettings.status to 'active' on all of the Campaign's articles
    * that were ever ingested by the given RSS Feed via the given campaign.
    *
    * If newStatus=inactive, then, in a manner analogous to garbage collection,
    * set the CampaignArticleSettings.status to 'inactive' for any article
    * that was ever ingested by the given RSS Feed via the given campaign,
    * and which was never ingested by any other RSS Feed that is still active in the given campaign.
    */
  def findArticlesToChange(campRow: CampaignRow, wantFeedUrl: String, newStatus: CampaignArticleStatus.Type): ValidationNel[FailureResult, Set[ArticleKey]] = {

    // If we're deactivating an RSS Feed, we won't deactivate articles that are linked to another active RSS Feed.
    val stopSetFeedUrls = if (newStatus == CampaignArticleStatus.inactive) campRow.activeFeeds.map(_.feedUrl).toSet - wantFeedUrl
    else Set[String]()

    // A candidate article needs to be changed if is associated with the changing feed, but not associated with any active stopSet feeds.
    def needsChange(ar: ArticleRow) = {
      // From the ArticleRow, get the Set of URL Strings of the RSS Feeds that have ingested that Article.
      val artUrls = ar.ingestedData.toList.flatMap(data => toOptNotesInfo(data)._1).toSet

      artUrls.contains(wantFeedUrl) && artUrls.intersect(stopSetFeedUrls).isEmpty
    }

    // Filter a set of ArticleKeys down to a ValidatationNEL containing the keys of Articles whose active/inactive status must be changed.
    def onesNeedingChange(artKeys: Set[ArticleKey]): ValidationNel[FailureResult, Set[ArticleKey]] =
      ArticleService.fetchMulti(artKeys)(_.withFamilies(_.ingestedData)).map(_.filter(artKeyRow => needsChange(artKeyRow._2)).keySet.toSet)

    // We have to look at non-blacklisted articles that don't already have the newStatus that we're moving to.
    val candidateArtKeys = (for ((ak, s) <- campRow.recentArticleSettings(skipCache = true); if !s.isBlacklisted && s.status != newStatus) yield ak).toSet

    val emptyRes: ValidationNel[FailureResult, Set[ArticleKey]] = Set[ArticleKey]().successNel
    val maxRows = 512 // Lower is less likely to run out of memory -- higher is faster.

    // This works, but is just a bit ugly.
    candidateArtKeys.grouped(maxRows)
      .foldLeft(emptyRes)((nel, artKeys) => if (nel.isFailure) nel
      else {
        nel |+| onesNeedingChange(artKeys)
      })

    //    // Something like this might work, but surely there's already an easy way to do this?
    //    candidateArtKeys.grouped(maxRows)
    //      .foldLeftFailFast (emptyRes) (_ |+| onesNeedingChange(_))
  }

  /**
    * Get the campaign row including the columns used in logging. Useful for passing to AuditService.
    *
    * @param ck CampaignKey to fetch
    * @return CampaignRow with relevant AuditTable columns
    */
  def attachRss(ck: CampaignKey, rss: String, feedSettings: RssFeedSettings = RssFeedSettings.default, userId: Long = -1l, updateLinkedArticleStatus: Boolean = false): ValidationNel[FailureResult, CampaignRow] = {
    def appropriateLoggingSpec = if (updateLinkedArticleStatus)
      loggingFetchQuerySpecWithAllRecentArticlesExtremelyBigAndSlow
    else
      loggingFetchQuerySpecWithoutRecentArticles

    //Get current CampaignRow before update, for logging
    val beforeRow = fetch(ck)(appropriateLoggingSpec)

    for {
    // if user has requested the linked articles to be updated, then find and update the articles to the new feed status
      _ <- if (updateLinkedArticleStatus) {
        for {
          cr <- beforeRow
          aks <- findArticlesToChange(cr, rss, feedSettings.feedStatus)
          result <- updateCampaignArticleSettings(cr, aks, _.copy(status = feedSettings.feedStatus))
        } yield {
          result
        }
      } else {
        OpsResult(0, 0, 0).successNel
      }
      //Attempt to add or update the feed.
      result <- modifyPutRefetch(ck)(_.valueMap(_.feeds, Map(rss -> feedSettings)))(appropriateLoggingSpec).map(
        updatedRow => {
          //Log some kind of change even if beforeRow failed
          //Will wrongly log a modification as a rssFeedListAdd in the case that beforeRow is FailureResult (impossible to distinguish)
          AuditService.logChangeLossy(ck, AuditEvents.rssFeedListAdd, userId, beforeRow, updatedRow)
          updatedRow
        }
      )
    } yield {
      result
    }
  }

  def updateArticlesForCampaign(ck: CampaignKey, status: CampaignStatus.Type): ValidationNel[FailureResult, Int] = {
    for {
      campaign <- fetch(ck)(_.withFamilies(_.recentArticles))
    } yield {
      // try to update the status for each of the articles currently bound to this campaign
      val articles = campaign.recentArticleKeys

      if (articles.nonEmpty) {
        var count = 0
        val failures = mutable.Buffer[FailureResult]()
        val map = Map(ck -> status)
        for (ak <- articles) {
          articleService.modifyPut(ak)(_.valueMap(_.campaigns, map)).fold((fails: NonEmptyList[FailureResult]) => failures ++= fails.list, (_: OpsResult) => count += 1)
        }

        if (failures.isEmpty) {
          count
        } else {
          return toNonEmptyList(failures.toList).failure
        }
      } else {
        0
      }
    }
  }

  def effectiveTitle(articleTitle: Title, camp: CampaignRecommendationData): Title = {
    val titleO = for {
      someSettings <- camp.settings
    } yield someSettings.effectiveTitle(articleTitle)

    titleO.getOrElse(articleTitle)
  }

  def effectiveImage(articleImage: ImageUrl, settings: Option[CampaignArticleSettings]): ImageUrl = {
    settings.map(_.effectiveImage(articleImage)).getOrElse(articleImage)
  }

  private def auditBudgetChange(oldBudget: Option[BudgetSettings], newBudget: BudgetSettings, campaignKey: CampaignKey, userId: Long) = {
    def auditBudget(budgetType: String, oldBudget: Option[Budget], newBudget: Option[Budget]) = {
      (oldBudget, newBudget) match {
        case (Some(oldB), None) => AuditService.logEvent[CampaignKey, Budget, Option[Budget]](campaignKey, userId,
          AuditEvents.budgetSettingsUpdate, "budgetSettings", oldB, None, Seq(s"Deleted $budgetType budget"))
        case (None, Some(newB)) => AuditService.logEvent[CampaignKey, Option[Budget], Budget](campaignKey, userId,
          AuditEvents.budgetSettingsUpdate, "budgetSettings", None, newB, Seq(s"Added $budgetType budget"))
        case (Some(oldB), Some(newB)) if oldB != newB =>
          val action =
            if (oldB.maxSpend == DollarValue.infinite) "Added"
            else if (newB.maxSpend == DollarValue.infinite) "Deleted"
            else "Modified"
          AuditService.logEvent(campaignKey, userId, AuditEvents.budgetSettingsUpdate, "budgetSettings", oldB, newB, Seq(s"$action $budgetType budget"))
        case _ =>
      }
    }

    oldBudget match {
      case Some(ob) =>
        auditBudget("primary", ob.budgets.headOption, newBudget.budgets.headOption)
        auditBudget("secondary", ob.budgets.lift(1), newBudget.budgets.lift(1))
      case None =>
        auditBudget("primary", None, newBudget.budgets.headOption)
        auditBudget("secondary", None, newBudget.budgets.lift(1))
    }
  }

  def getNextHour(dt: DateTime): DateHour = {
    // By converting to a DateHour we'll get implicit down rounding. To give the user more time (but not too much time)
    // before the new budget start time happens, add an extra hour when the time window would be less than 30 minutes.
    val moh = dt.minuteOfHour()
    val minute = moh.get()
    val roundUp = minute > 30
    val dh = dt.toDateHour
    if (roundUp) dh.plusHours(1)
    else dh
  }

  case class InvalidBudget(campaignKey: CampaignKey, userId: Long, budget: BudgetSettings, msg: String) extends Logstashable {

    import com.gravity.logging.Logstashable._

    override def getKVs: Seq[(String, String)] = {
      Seq(CampaignKey -> campaignKey.toString, UserGuid -> userId.toString, Message -> msg)
    }
  }

  /* operations for the new budget history stuff */
  def setBudget(campaignKey: CampaignKey, budget: BudgetSettings, startHour: DateHour = grvtime.currentHour, endHour: DateHour = BudgetSettings.endOfTime, overwriteCurrent: Boolean = true, userId: Long = -1L): ValidationNel[FailureResult, OpsResult] = {
    val actualBudget = budget.withExplicitInfinite()

    if (!actualBudget.isValid) {
      val msg = "Invalid budget, unwilling to save: " + actualBudget.toPrettyString()
      warn(InvalidBudget(campaignKey, userId, actualBudget, msg))
      return FailureResult(msg).failureNel
    }

    getBudgets(campaignKey) match {
      case Success(budgets) =>
        val budgetsContainingStartHour = budgets.filter { case (range, data) => range.contains(startHour) }.toSeq.sortBy(_._1.toHour)
        if (budgetsContainingStartHour.isEmpty) {
          val range = DateHourRange(startHour, endHour)
          val putOp =
            if (overwriteCurrent) Schema.Campaigns.put(campaignKey).value(_.budgetSettings, actualBudget).valueMap(_.dateRangeToBudgetData, Map(range -> actualBudget))
            else Schema.Campaigns.put(campaignKey).valueMap(_.dateRangeToBudgetData, Map(range -> actualBudget))
          val result = put(putOp)
          result.foreach(_ => auditBudgetChange(None, actualBudget, campaignKey, userId))
          result
        }
        else {
          val lastExisting = budgetsContainingStartHour.last
          val lastExistingRange = lastExisting._1
          val lastExistingBudget = lastExisting._2
          if (lastExistingBudget == actualBudget) {
            OpsResult(0, 0, 0).successNel
          }
          else {
            val sameStart = budgetsContainingStartHour.map(_._1).filter(_.fromHour == startHour)
            val isExistingFutureDate = lastExistingRange.fromHour.isAfter(grvtime.currentTime)
            if (sameStart.isEmpty && !isExistingFutureDate) {
              // Check if the last budget has started yet
              // check if the TO date is in the past, if it is leave it alone, if it isn't then modify it.
              val isFutureEndDate = lastExistingRange.toHour.isAfter(grvtime.currentTime)
              val putOp = if (isFutureEndDate) {
                // Go ahead and change the existing
                val newRangeForExisting = DateHourRange(lastExistingRange.fromInclusive, startHour)
                val rangeForNew = DateHourRange(startHour, endHour)
                if (lastExistingRange != rangeForNew) {
                  // otherwise sometimes the new one ends up deleted
                  doModifyDelete(campaignKey)(_.values(_.dateRangeToBudgetData, Set(lastExistingRange)))
                }

                if (overwriteCurrent) Schema.Campaigns.put(campaignKey).value(_.budgetSettings, actualBudget).valueMap(
                  _.dateRangeToBudgetData, Map(newRangeForExisting -> lastExistingBudget, rangeForNew -> actualBudget))
                else Schema.Campaigns.put(campaignKey).valueMap(_.dateRangeToBudgetData,
                  Map(newRangeForExisting -> lastExistingBudget, rangeForNew -> actualBudget))
              }
              else {
                // The lastExistingRange end date is in the past
                val rangeForNew = if (startHour.isBefore(lastExistingRange.toHour)) {
                  DateHourRange(lastExistingRange.toHour, endHour)
                }
                else {
                  DateHourRange(startHour, endHour)
                }
                // Leave the existing alone and just add the new
                if (overwriteCurrent) Schema.Campaigns.put(campaignKey).value(_.budgetSettings, actualBudget).valueMap(
                  _.dateRangeToBudgetData, Map(rangeForNew -> actualBudget))
                else Schema.Campaigns.put(campaignKey).valueMap(_.dateRangeToBudgetData, Map(rangeForNew -> actualBudget))
              }

              val result = put(putOp)
              result.foreach(_ => auditBudgetChange(Some(lastExistingBudget), actualBudget, campaignKey, userId))
              result
            }
            else {
              if (sameStart.size <= 1 && isExistingFutureDate) {
                // then overwrite the last existing budget that is in the future
                doModifyDelete(campaignKey)(_.values(_.dateRangeToBudgetData, Set(lastExistingRange)))

                val rangeForNew = DateHourRange(startHour, endHour)
                val putOp = Schema.Campaigns.put(campaignKey).value(_.budgetSettings, actualBudget).valueMap(
                  _.dateRangeToBudgetData, Map(rangeForNew -> actualBudget))

                val result = put(putOp)
                result.foreach(_ => auditBudgetChange(Some(lastExistingBudget), actualBudget, campaignKey, userId))
                result
              }
              else {
                FailureResult("Campaign " + campaignKey + " already has a budget with start " + startHour + ". Use changeBudgetEndDate to modify an existing range.").failureNel
              }
            }
          }
        }
      case Failure(fails) => fails.failure
    }
  }

  def deleteBudgetHistory(campaignKey: CampaignKey): OpsResult = {
    Schema.Campaigns.delete(campaignKey).family(_.dateRangeToBudgetData).execute()
  }

  def changeBudgetEndDate(campaignKey: CampaignKey, existingStartHour: DateHour, newEndHour: DateHour): ValidationNel[FailureResult, OpsResult] = {
    getBudgets(campaignKey) match {
      case Success(map) =>
        val existing = map.filter { case (range, data) => range.fromHour == existingStartHour }
        if (existing.size > 1) throw new Exception("Campaign " + campaignKey + " has more than one budget starting on " + existingStartHour + ". This should never happen!")
        if (existing.isEmpty) FailureResult("There is no budget set for " + campaignKey).failureNel
        else {
          val existingRange = existing.head._1
          val existingData = existing.head._2
          val newRange = DateHourRange(existingStartHour, newEndHour)
          doModifyDelete(campaignKey)(_.values(_.dateRangeToBudgetData, Set(existingRange)))
          putSingle(campaignKey)(_.valueMap(_.dateRangeToBudgetData, Map(newRange -> existingData)))
        }
      case Failure(fails) => FailureResult("There is no budget set for " + campaignKey).failureNel
    }
  }

  def getCurrentBudget(campaignKey: CampaignKey): ValidationNel[FailureResult, BudgetSettings] = {
    fetch(campaignKey)(_.withFamilies(_.dateRangeToBudgetData).withColumn(_.budgetSettings)) match {
      case Success(row) =>
        if (row.dateRangeToBudgetData.isEmpty) {
          if (row.budgetSettings.isDefined)
            row.budgetSettings.get.successNel
          else
            FailureResult("No budget settings found in current or history for " + campaignKey).failureNel
        }
        else {
          row.dateRangeToBudgetData.toSeq.sortBy(_._1.toHour).last._2.successNel
        }

      case Failure(fails) => fails.failure
    }
  }

  def getBudgetsWithOld(campaignKey: CampaignKey, skipCache: Boolean = true): ValidationNel[FailureResult, scala.collection.Map[DateHourRange, BudgetSettings]] = {
    val startHour = DateHour(new DateTime(2000, 1, 1, 0, 0, 0, 0))
    fetch(campaignKey, skipCache)(_.withFamilies(_.dateRangeToBudgetData).withColumn(_.budgetSettings)) match {
      case Success(row) =>
        if (row.dateRangeToBudgetData.isEmpty) {
          if (row.budgetSettings.isDefined)
            Map(DateHourRange(startHour, BudgetSettings.endOfTime) -> row.budgetSettings.get).successNel
          else
            FailureResult("No budget settings found in current or history for " + campaignKey).failureNel
        }
        else {
          if (row.budgetSettings.isDefined) {
            val oldStart = row.dateRangeToBudgetData.keys.toSeq.sortBy(_.fromInclusive.getMillis).head.fromInclusive
            val oldRange = DateHourRange(startHour, oldStart)
            (row.dateRangeToBudgetData + (oldRange -> row.budgetSettings.get)).successNel
          }
          else
            row.dateRangeToBudgetData.successNel
        }

      case Failure(fails) => fails.failure
    }
  }

  def getBudget(campaignKey: CampaignKey, dateHour: DateHour, skipCache: Boolean = true): ValidationNel[FailureResult, Seq[(DateHourRange, BudgetSettings)]] = {
    getBudgets(campaignKey, skipCache) match {
      case Success(map) =>
        val existing = map.filter { case (range, data) => range.contains(dateHour) }
        if (existing.isEmpty) {
          FailureResult("There is no budget set for " + campaignKey + " at " + dateHour).failureNel
        }
        else {
          existing.toSeq.sortBy(_._1.toHour).successNel
        }
      case Failure(fails) => fails.failure
    }
  }

  def getBudgetWithOld(campaignKey: CampaignKey, dateHour: DateHour, skipCache: Boolean = true): ValidationNel[FailureResult, Seq[(DateHourRange, BudgetSettings)]] = {
    getBudgetsWithOld(campaignKey, skipCache) match {
      case Success(map) =>
        val existing = map.filter { case (range, data) => range.contains(dateHour) }
        if (existing.isEmpty) {
          FailureResult("There is no budget set for " + campaignKey + " at " + dateHour).failureNel
        }
        else {
          existing.toSeq.sortBy(_._1.toHour).successNel
        }
      case Failure(fails) => fails.failure
    }
  }


  def getBudgetsForDay(campaignKey: CampaignKey, day: DateTime, skipCache: Boolean = true): ValidationNel[FailureResult, Seq[(DateHourRange, BudgetSettings)]] = {
    getBudgets(campaignKey, skipCache) match {
      case Success(map) =>
        val existing = map.filter { case (range, data) => range.toMidnightRange.contains(day) }
        if (existing.isEmpty) {
          FailureResult("There is no budget set for " + campaignKey + " at " + day).failureNel
        }
        else {
          existing.toSeq.sortBy(_._1.toHour).successNel
        }
      case Failure(fails) => fails.failure
    }
  }

  case class SpendTypeBudgets(private val budgets: Map[MaxSpendTypes.Type, Budget]) {
    def getSpendTypeBudget(f: (MaxSpendTypes.type) => MaxSpendTypes.Type) = budgets.get(f(MaxSpendTypes))
  }

  def getMaxBudgetsForDay(campaignKey: CampaignKey, day: DateTime, skipCache: Boolean = true): ValidationNel[FailureResult, SpendTypeBudgets] = {
    // return the max budget for the spend type for that day
    CampaignService.getBudgetsForDay(campaignKey, day, skipCache) match {
      case Success(data) =>
        SpendTypeBudgets(
          (for (spendType <- MaxSpendTypes.values) yield {
            data.map { case (_, budgetSetting) => budgetSetting.getBudget(spendType) }.maxBy(_.map(_.maxSpend).getOrElse(DollarValue(-1l)))
          }).flatten.map(budget => (budget.maxSpendType, budget)).toMap
        ).successNel
      case Failure(fails) => fails.failure
    }
  }

  def getBudgets(campaignKey: CampaignKey, skipCache: Boolean = true): ValidationNel[FailureResult, scala.collection.Map[DateHourRange, BudgetSettings]] = {
    fetchOrEmptyRow(campaignKey, skipCache)(_.withFamilies(_.dateRangeToBudgetData)).map(_.dateRangeToBudgetData)
  }

  def getAllBudgets: scala.collection.Map[CampaignKey, scala.collection.Map[DateHourRange, BudgetSettings]] = {
    Schema.Campaigns.query2.withFamilies(_.dateRangeToBudgetData).scanToIterable(row => (row.campaignKey, row.dateRangeToBudgetData)).toMap
  }

  def enableCampaignTrackingParamsOverride(ck: CampaignKey): Boolean = campaignMeta(ck).fold(false)(_.enableCampaignTrackingParamsOverride)

  def pubTimeIsWithinTtl(maxAgeOpt: Option[Int], pubTime: DateTime) = {
    // If the article's pubDate is newer than articlePublishCutoff, then it's ok, otherwise it's too old.
    val oldestAllowed = maxAgeOpt.map(grvtime.currentDay.minusDays).getOrElse(grvtime.epochDateTime)

    // Return true if the article is new enough to be within the TTL.
    pubTime.isEqual(oldestAllowed) || pubTime.isAfter(oldestAllowed)
  }

  // Return true if this is a DLUG/GMS article with a loved gmsStatus, or if the article is new enough to be within the TTL.
  def gmsStatusIsToBeKeptOrPubTimeIsWithinTtl(gmsStatusOpt: Option[GmsArticleStatus.Type], maxAgeOpt: Option[Int], pubTime: DateTime): Boolean =
    gmsStatusOpt.map(_.isToBeKept) getOrElse pubTimeIsWithinTtl(maxAgeOpt, pubTime)
}

case class CampaignMetaRequest()

case class CampaignMetaResponse(rowMap : Map[CampaignKey, CampaignRow])

case class CampaignsWithSponsoredMetrics(campaignMap: Map[CampaignKey, CampaignRow], sponsoredMetrics: Map[SponsoredMetricsKey, Long])

case class CampaignDateRequirements private(startDate: GrvDateMidnight, isOngoing: Boolean, endDateOpt: Option[GrvDateMidnight]) {
  require(isOngoing == endDateOpt.isEmpty, "If the campaign is ongoing, there cannot be an end date set!")
  require(if (isOngoing) true
  else {
    endDateOpt match {
      case Some(endDate) => !endDate.isBefore(startDate)
      case None => true // not really, but we covered this in the previous require ;-)
    }
  }, "If the campaign is NOT ongoing, then the end date MUST be AFTER the start date!")

  def addValuesToPut(put: CampaignService.PutSpec): CampaignService.PutSpec = {
    put.value(_.startDate, startDate).value(_.isOngoing, isOngoing)
    endDateOpt.foreach(endDate => put.value(_.endDate, endDate))

    put
  }
}

object CampaignDateRequirements {
  val empty = CampaignDateRequirements(grvtime.emptyDateTime.toGrvDateMidnight, isOngoing = true, endDateOpt = None)

  def createOngoing(startDate: GrvDateMidnight): CampaignDateRequirements = CampaignDateRequirements(startDate, isOngoing = true, None)

  def createNonOngoing(startDate: GrvDateMidnight, endDate: GrvDateMidnight): CampaignDateRequirements = CampaignDateRequirements(startDate, isOngoing = false, endDate.some)

  def validate(startDate: GrvDateMidnight, endDateOpt: Option[GrvDateMidnight]): ValidationNel[FailureResult, CampaignDateRequirements] = {
    endDateOpt match {
      case Some(endDate) =>
        if (!endDate.isBefore(startDate)) {
          CampaignDateRequirements.createNonOngoing(startDate, endDate).successNel
        } else {
          FailureResult("If the campaign is NOT ongoing, then the end date MUST NOT be BEFORE the start date!").failureNel
        }
      case None => CampaignDateRequirements.createOngoing(startDate).successNel
    }
  }
}

object AddArticleFlags extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String) = Type(id, name)

  val noOp = Value(0, "no-op")

  val forceCrawl  = Value(30, "forceCrawl")
  val skipCrawl   = Value(40, "skipCrawl")
  val failCrawlOk = Value(50, "failCrawlOk")

  val defaultValue = noOp
  implicit val defaultValueWriter = makeDefaultValueWriter[Type]
}

case class CampaignNameAlreadyExistsFailure(name: String, attemptedCampaignKey: CampaignKey, existingCampaignKey: CampaignKey)
  extends FailureResult("Cannot set the campaign name: '" + name + "' for campaign: '" + attemptedCampaignKey.toString + "' as it is a duplicate of existing campaign: " + existingCampaignKey.toString, None)

case class CampaignArticleAddResult(article: ArticleRow, campaignKey: CampaignKey, campaignArticleSettings: CampaignArticleSettings, siteGuid: String, failedCrawl: Boolean) {
  override lazy val toString: String = {
    s"""{ "url":${article.url}, "campaignKey":$campaignKey, "campaignArticleSettings":$campaignArticleSettings, "siteGuid":$siteGuid, "failedCrawl":$failedCrawl }"""
  }
}

case class CampaignStatusValidationResponse(description: String, currentStatus: CampaignStatus.Type, toStatus: CampaignStatus.Type, putSpecOption: Option[CampaignService.PutSpec] = None, auditKeyOption: Option[AuditKey2] = None) {
  def statusChangeMessge: Option[String] = if (currentStatus != toStatus) {
    Some(currentStatus.toString + " => " + toStatus.toString)
  } else {
    None
  }

  def isStillActive: Boolean = toStatus == CampaignStatus.active
}

object CampaignStatusValidationResponse {
  val organicNoChangeRequired = CampaignStatusValidationResponse("Organic Campaign. Schedule and Budget not used.", CampaignStatus.active, CampaignStatus.active)
  val organicNoChangeRequiredSuccessNel: ValidationNel[FailureResult, CampaignStatusValidationResponse] = organicNoChangeRequired.successNel
}

sealed trait UpdateStatusWorthyResult
case class ScheduleFailureResult(reason: String) extends FailureResult(reason, None) with UpdateStatusWorthyResult
case class BudgetFailureResult(reason: String) extends FailureResult(reason, None) with UpdateStatusWorthyResult

trait ArticleServiceProxy {
  def articleService: ArticleService
}

trait ProductionArticleServiceProxy extends ArticleServiceProxy {
  def articleService: ArticleService = ArticleService
}

@SerialVersionUID(1L)
case class CampaignBudgetExceededNotification(campaignKey: CampaignKey, budgetResult: BudgetResult)

@SerialVersionUID(1L)
case class CampaignStatusChangedNotification(campaignKey: CampaignKey, oldStatus: CampaignStatus.Type, newStatus: CampaignStatus.Type, userId: Long)

@SerialVersionUID(1L)
case class ValidateCampaignBudgetAndSchedule(campaignKey: CampaignKey, name: String)

@SerialVersionUID(1L)
case class ValidateCampaigns()

@SerialVersionUID(1L)
case class ValidateExchanges()

@SerialVersionUID(1L)
case class ValidateExchangeMetricsAndGoalsAndSchedule(exchangeKey: ExchangeKey, name: String)


object CampaignNotifications {
 import com.gravity.logging.Logging._

  val counter = (name: String) => com.gravity.utilities.Counters.countPerSecond("CampaignNotifications", name)

  val distributionAddress = "campaignsalerts@gravity.com"
  val fromAddress = "alerts@gravity.com"

  val campaignBudgetNotificationLock = new Object
  val campaignStatusNotificationLock = new Object

  def sendIfProduction(message: CampaignBudgetExceededNotification) {
    if (Settings.isProductionServer) {
      RemoteOperationsClient.clientInstance.send(message)
    }
  }

  def sendIfProduction(message: CampaignStatusChangedNotification) {
    if (Settings.isProductionServer) {
      RemoteOperationsClient.clientInstance.send(message)
    }
  }

  def sendCampaignBudgetExceededNotification(notification: CampaignBudgetExceededNotification) {

    campaignBudgetNotificationLock.synchronized {
      try {
        val message = notification.budgetResult.maxReachedMessage.getOrElse("Budget exceeded")

        val campaignKey = notification.campaignKey

        // get the campaign's last notification date rounded to midnight... or default to long ago if not set
        val campaign = CampaignService.fetch(campaignKey, skipCache = true)(_.withFamilies(_.meta)).toOption

        val lastNotification = campaign.flatMap(_.campaignBudgetNotificationTimestamp).getOrElse(grvtime.epochDateTime)

        val today = grvtime.currentTime

        // if a day has passed since last notification date, then send a notification, and update the last notification date to today
        if (today.toGrvDateMidnight.isAfter(lastNotification.toGrvDateMidnight)) {
          val siteMeta = SiteService.siteMeta(campaignKey.siteKey)
          val siteName = siteMeta.map(_.nameOrNoName).getOrElse("(Unknown)")
          val accountManager = campaign.flatMap(c => DashboardUserService.getManagerForSite(c.siteGuid.asSiteGuid))
          val campaignName = campaign.map(_.nameOrNotSet).getOrElse("(Unknown)")

          val body = {
            new StringBuilder()
              .append("Site: ").append(siteName)
              .append("\nCampaign: ").append(campaignName)
              .append("\nAccount Manager: ").append(accountManager.map(_.toEmailAddress).getOrElse("(Unknown)"))
              .append("\n\nReason: ").append(message).append("\n\n").append(campaignLink(campaignKey))
              .toString()
          }

          counter("sendCampaignBudgetExceededNotification (email)")

          CampaignService.modifyPut(campaignKey)(_.value(_.campaignBudgetNotificationTimestamp, today)) match {
            case Success(_) =>
              val subject = "Campaign Reached Budget Cap: " + siteName + " -> " + campaignName
              accountManager.map(_.toEmailAddress) match {
                case Some(acctMgrEmail) =>
                  EmailUtility.send(acctMgrEmail, fromAddress, subject, body, Some(distributionAddress))

                case None =>
                  EmailUtility.send(distributionAddress, fromAddress, subject, body)
              }
            case Failure(fails) =>
              warn(fails, "Campaign Service put failure")
          }
        }
      } catch {
        case ex: Exception => warn(ex, "Error while sending campaign budget notification!")
      }
    }

  }


  def sendCampaignStatusChangedNotification(notification: CampaignStatusChangedNotification) {

    campaignStatusNotificationLock.synchronized {
      try {
        val campaignKey = notification.campaignKey
        val campaign = CampaignService.fetch(campaignKey, skipCache = true)(_.withFamilies(_.meta)).toOption

        campaign.map(cr => {
          val siteName = SiteService.siteMeta(campaignKey.siteKey).map(_.nameOrNoName).getOrElse("(Unknown)")
          val campaignName = cr.nameOrNotSet
          val changedBy = campaign.flatMap(c => DashboardUserService.getUser(notification.userId))
          val accountManager = campaign.flatMap(c => DashboardUserService.getManagerForSite(c.siteGuid.asSiteGuid))

          val message = changedBy.flatMap(_.firstName).getOrElse("(Unknown)") + " changed status from " + notification.oldStatus.toString.toUpperCase + " to " + notification.newStatus.toString.toUpperCase + ": " + siteName + " -> " + campaignName

          val body = {
            new StringBuilder()
              .append("Site: ").append(siteName)
              .append("\nCampaign: ").append(campaignName)
              .append("\nUser: ").append(changedBy.map(_.toEmailAddress).getOrElse("(Unknown)"))
              .append("\nAccount Manager: ").append(accountManager.map(_.toEmailAddress).getOrElse("(Unknown)"))
              .append("\n\nReason: ").append(message).append("\n\n").append(campaignLink(campaignKey))
              .toString()
          }

          accountManager.map(_.toEmailAddress) match {
            case Some(acctMgrEmail) =>
              EmailUtility.send(acctMgrEmail, fromAddress, message, body, Some(distributionAddress))

            case None =>
              EmailUtility.send(distributionAddress, fromAddress, message, body)
          }

          counter("sendCampaignStatusChangedNotification (email)")
        })
      } catch {
        case ex: Exception => warn(ex, "Error while sending campaign status changed notification!")
      }
    }

  }

  def campaignLink(campaignKey: CampaignKey): String = {
    val siteGuid = SiteService.siteMeta(campaignKey.siteKey).map(_.siteGuidOrNoGuid).getOrElse("NO_GUID")
    "https://dashboard.gravity.com/campaigns/campaign/edit/id/" + campaignKey + "?sg=" + siteGuid
  }

 }

object AllCampaignMetaObj {
  val nextStamp = new AtomicLong(1)
}

case class AllCampaignMetaObj(allCampaignMeta: Map[CampaignKey, CampaignRow]) {
  lazy val stamp = AllCampaignMetaObj.nextStamp.getAndIncrement
  lazy val serialized = CampaignMetaResponseConverter.toBytes(CampaignMetaResponse(allCampaignMeta))
  lazy val bySiteKey: Map[SiteKey, Seq[CampaignRow]] = {
    val asMonoid: List[Map[SiteKey, List[CampaignRow]]] =
      allCampaignMeta.toList.map(kv => Map(kv._1.siteKey -> List(kv._2)))

    asMonoid.foldLeft(Map[SiteKey, List[CampaignRow]]())(_ |+| _)
  }
}

case class CampaignBudgetOverage(campaignKey: CampaignKey, foundBy: String, amount: Long, maxSpendType: MaxSpendTypes.Type)

