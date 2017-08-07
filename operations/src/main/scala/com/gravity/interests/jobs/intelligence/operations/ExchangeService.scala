package com.gravity.interests.jobs.intelligence.operations

import java.util.concurrent.atomic.AtomicLong

import com.gravity.data.configuration.ConfigurationQueryService
import com.gravity.hbase.schema.{ColumnFamily, OpsResult}
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase._
import com.gravity.interests.jobs.intelligence.operations.audit.AuditService
import com.gravity.interests.jobs.intelligence.operations.audit.OperationsAuditLoggingEvents._
import com.gravity.service.grvroles
import com.gravity.utilities._
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvtime._
import com.gravity.utilities.grvz._
import com.gravity.valueclasses.ValueClassesForDomain.{ExchangeGuid, SiteGuid}
import org.joda.time.DateTime

import scala.collection._
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, NonEmptyList, Success, ValidationNel}

/*

         ▒▒
         ██
       ██████
     ██████████
     ██░░██░░██
     ██████████
       ██████
       ░░░░░░      It's gonna be fun on the bun!
       ██████

*/

object ExchangeService extends ExchangeService {

}

trait ExchangeService extends ExchangeOperations {
  import com.gravity.utilities.Counters._
  import com.gravity.logging.Logging._

  private val hbaseConf = if(grvroles.isFrontEndRole) HBaseConfProvider.getConf.oltpConf else HBaseConfProvider.getConf.defaultConf

  def allExchangeMeta = allExchangeMetaObj.allExchangeMeta

  val exchangeMetaQuerySpec: QuerySpec = _.withFamilies(_.meta, _.exchangeSites, _.contentSources, _.disabledSites, _.trackingParamsForAllSites)

  private def exchangeMetaQuery = exchangeMetaQuerySpec(Schema.Exchanges.query2)

 def exchangeMeta(exchangeKey: ExchangeKey, skipCache: Boolean = false): Option[ExchangeRow] = {
    if (Settings.ENABLE_SITESERVICE_META_CACHE) {
      // In a production environment, cache the entire campaign meta as a map lookup
      allExchangeMeta.get(exchangeKey)
    } else {
      // In other environments, cache single values at a time.  (Changed this from no cache because there are tight-loop lookups)
      exchangeMetaQuery.withKey(exchangeKey).singleOption(skipCache = skipCache)
    }
  }

 def exchangeMeta(exchangeKeys: Set[ExchangeKey], skipCache: Boolean): Map[ExchangeKey, ExchangeRow] = {
    if (Settings.ENABLE_SITESERVICE_META_CACHE) {
      // In a production environment, cache the entire campaign meta as a map lookup
      exchangeKeys.flatMap(exk => allExchangeMeta.get(exk) match {
        case Some(exr) => Some((exk, exr))
        case _ => None
      }).toMap
    } else {
      // In other environments, cache single values at a time.  (Changed this from no cache because there are tight-loop lookups)
      exchangeMetaQuery.withKeys(exchangeKeys).executeMap(skipCache = skipCache)
    }
  }

  def allExchangeMetaObj: AllExchangeMetaObj = {

    def factoryFun() = {
      val map = exchangeMetaQuery.scanToIterable(row => (row.exchangeKey, row)).toMap

      AllExchangeMetaObj(map)
    }

    PermaCacher.getOrRegister(
      "ExchangeService.exchangeMeta", PermaCacher.retryUntilNoThrow(factoryFun), 5 * 60)
  }

  object AllExchangeMetaObj {
    val nextStamp = new AtomicLong(1)
  }

  case class AllExchangeMetaObj(allExchangeMeta: Map[ExchangeKey, ExchangeRow]) {


  }

  def getActiveAndAwaitingStartExchangesFromHbase: ValidationNel[FailureResult, Set[ExchangeKey]] = {
    val wantedExchangeStatuses = Set(ExchangeStatus.active, ExchangeStatus.awaitingStart)
    if (grvroles.isDevelopmentRole)
      filteredScan(hbaseConf = hbaseConf)(_.withColumns(_.exchangeGuid, _.exchangeStatus).filter(_.and(_.columnValueMustBeIn(_.exchangeStatus, wantedExchangeStatuses)))).map(_.keySet)
    else
    // This is intentionally a full scan and not a filtered scan to avoid scan timeouts by keeping data coming back to
    // the server more frequently
      scanToSeqWithoutMaintenanceCheck(rowCache = 10, hbaseConf = hbaseConf)(_.withColumns(_.exchangeGuid, _.exchangeStatus)).map(_.filter(row => wantedExchangeStatuses.contains(row.exchangeStatus)).map(_.exchangeKey).toSet)
  }

  case class ExchangeNameAlreadyExistsFailure(name: String, attemptedExchangeKey: ExchangeKey, existingExchangeKey: ExchangeKey)
    extends FailureResult("Cannot set the exchange name: '" + name + "' for exchange: '" + attemptedExchangeKey.toString + "' as it is a duplicate of existing exchange: " + existingExchangeKey.toString, None)

  def createExchange(hostGuid: SiteGuid,
                     exchangeName: String,
                     exchangeType: ExchangeType.Type,
                     userId: Long = -1l,
                     scheduledStartTime: DateTime,
                     scheduledEndTime: DateTime,
                     exchangeStatus: Option[ExchangeStatus.Type] = None,
                     exchangeGoal: Option[ExchangeGoal] = None,
                     exchangeThrottle: Option[ExchangeThrottle] = None,
                     exchangeSites: Option[Set[SiteKey]] = None,
                     contentSources: Option[Set[ContentGroupKey]] = None,
                     enableOmnitureTrackingParamsForAllSites: Option[Boolean] = None,
                     trackingParamsForAllSites: Option[Map[String,String]] = None
                    ):
  ValidationNel[FailureResult, ExchangeRow] = {
    if (ScalaMagic.isNullOrEmpty(hostGuid.raw)) return FailureResult("hostGuid MUST be NON-Empty!").failureNel
    if (ScalaMagic.isNullOrEmpty(exchangeName)) return FailureResult("exchangeName MUST be NON-Empty!").failureNel

    val exchangeGuid = ExchangeGuid.generateGuid
    updateExchange(exchangeGuid.exchangeKey, exchangeType, userId, isCreate = true, Option(exchangeGuid), Option(hostGuid), Option(exchangeName), exchangeStatus,
      Option(scheduledStartTime), Option(scheduledEndTime), exchangeGoal, exchangeThrottle, exchangeSites.getOrElse(Set(hostGuid.siteKey)).some, contentSources,
      enableOmnitureTrackingParamsForAllSites, trackingParamsForAllSites
    )
  }

  def validateHostGuid(exchangeRowOption: Option[ExchangeRow], hostGuid: Option[SiteGuid]): ValidationNel[FailureResult, Unit] = {
    if (hostGuid.isDefined && exchangeRowOption.isDefined) {
      FailureResult("Cannot change exchange host after exchange has been created").failureNel
    } else {
      ().successNel
    }
  }

  def validateExchangeName(exchangeRowOption: Option[ExchangeRow], exchangeKey: ExchangeKey, exchangeNameOpt: Option[String]): ValidationNel[FailureResult, Unit] = {
    (exchangeRowOption, exchangeNameOpt) match {
      case (None, None) =>  FailureResult("Name must be included when creating an exchange.").failureNel
      case (None, Some(name)) if ScalaMagic.isNullOrEmpty(name) =>  FailureResult("Name must be included when creating an exchange.").failureNel
      case (None, Some(name)) =>
        scanToSeq()(_.withColumn(_.exchangeName).filter(_.and(_.columnValueMustEqual(_.exchangeName, name)))) match {
          case Failure(f) => f.failure
          case Success(exchangeRow +: _) => ExchangeNameAlreadyExistsFailure(name, exchangeKey, exchangeRow.exchangeKey).failureNel
          case Success(Seq()) => ().successNel
        }
      case (Some(_), None) => ().successNel
      case (Some(_), Some(name)) if ScalaMagic.isNullOrEmpty(name) =>  FailureResult("Name must not be empty or null.").failureNel
      case (Some(_), Some(name)) =>
        scanToSeq()(_.withColumn(_.exchangeName).filter(_.and(_.columnValueMustEqual(_.exchangeName, name)))) match {
          case Failure(f) => f.failure
          case Success(exchangeRow +: Seq()) if exchangeKey == exchangeRow.exchangeKey => ().successNel
          case Success(Seq()) => ().successNel
          case Success(exchangeRow +: _) => ExchangeNameAlreadyExistsFailure(name, exchangeKey, exchangeRow.exchangeKey).failureNel
        }
    }
  }

  def validateExchangeTimes(exchangeRowOption: Option[ExchangeRow], startOpt: Option[DateTime], endOpt: Option[DateTime]): ValidationNel[FailureResult, Unit] = {
    (exchangeRowOption, startOpt, endOpt) match {
      case (None, Some(start),Some(end)) =>
        if (start.isBefore(end)) ().successNel
        else FailureResult("Scheduled start must be before end.").failureNel
      case (None,_,_) => FailureResult("Scheduled start and end must be included when creating an exchange.").failureNel

      case (Some(_), None, None) => ().successNel
      case (Some(row), Some(start), None) =>
        if (start.isBefore(row.scheduledEndTime)) ().successNel
        else FailureResult("Scheduled start must be before currently scheduled end.").failureNel
      case (Some(row), None, Some(end)) =>
        if (row.scheduledStartTime.isBefore(end)) ().successNel
        else FailureResult("Scheduled end must be after currently scheduled start.").failureNel
      case (Some(_), Some(start),Some(end)) =>
        if (start.isBefore(end)) ().successNel
        else FailureResult("Scheduled start must be before end.").failureNel

    }
  }

  def validateExchangeType(exchangeRowOption: Option[ExchangeRow], exchangeType: ExchangeType.Type): ValidationNel[FailureResult, Unit] = {
    if (exchangeRowOption.exists(_.exchangeType != exchangeType)) {
      FailureResult("Cannot change exchange type after exchange has been created").failureNel
    } else {
      ().successNel
    }
  }

  def validateExchangeSites(exchangeRowOption: Option[ExchangeRow], exchangeSitesOption: Option[Set[SiteKey]], hostGuidOption: Option[SiteGuid]): ValidationNel[FailureResult, Unit] = {

    (hostGuidOption, exchangeSitesOption, exchangeRowOption) match {
      case (Some(hostGuid), None, _) => FailureResult("Exchange sites must always be included when creating an exchange").failureNel
      case (Some(hostGuid), Some(sites), _) =>
        if (!sites.contains(hostGuid.siteKey)) FailureResult("Exchange sites must always include the host when creating an exchange").failureNel
        else ().successNel
      case (None, None, Some(_)) => ().successNel
      case (_, Some(sites), Some(exchange)) =>
        if (!sites.contains(exchange.hostGuid.siteKey)) FailureResult("Exchange sites must always include the host site").failureNel
        else ().successNel
      case (None, _, None) => FailureResult("hostGuid required when creating an exchange.").failureNel
    }
  }

  def validateContentSources(exchangeRowOption: Option[ExchangeRow], exchangeSitesOption: Option[Set[SiteKey]], contentSourcesOption: Option[Set[ContentGroupKey]]): ValidationNel[FailureResult, Unit] = {

    def anyContentSourcesInvalidForSites(sites: Set[SiteKey], contentSources: Set[ContentGroupKey]): Boolean = {
      val contentSourceGroups = ConfigurationQueryService.queryRunner.getContentGroupsWithoutCaching(contentSources.map(_.contentGroupId))
      contentSourceGroups.size != contentSources.size || contentSourceGroups.exists(cgr => !sites.contains(SiteKey(cgr.forSiteGuid)))
    }

    (exchangeSitesOption, contentSourcesOption, exchangeRowOption) match {
      case (Some(sites), Some(contentSources), _) =>
        if (anyContentSourcesInvalidForSites(sites, contentSources)) FailureResult("All content sources must belong to a site in the exchange.").failureNel
        else ().successNel
      case (Some(sites), None, Some(exchangeRow)) =>
        //Question: just remove the content sources rather than complain? hrrrm...
        if (anyContentSourcesInvalidForSites(sites, exchangeRow.contentSources)) FailureResult("Exchange sites cannot be removed without removing their content sources.").failureNel
        else ().successNel
      case (None, Some(contentSources), Some(exchangeRow)) =>
        if (anyContentSourcesInvalidForSites(exchangeRow.exchangeSites, contentSources)) FailureResult("All content sources must belong to a site in the exchange.").failureNel
        else ().successNel
      case (None, None, Some(_)) => ().successNel
      case (Some(_), None, None) => ().successNel
      case (None, _, None) => FailureResult("Exchange sites must include the sponsor site when creating an exchange.").failureNel
    }
  }



  def updateExchange(exchangeKey: ExchangeKey,
                     exchangeType: ExchangeType.Type,
                     userId: Long = -1l,
                     isCreate: Boolean = false,
                     newExchangeGuidOption: Option[ExchangeGuid] = None,
                     hostGuid: Option[SiteGuid] = None,
                     exchangeName: Option[String] = None,
                     exchangeStatus: Option[ExchangeStatus.Type] = None,
                     scheduledStartTime: Option[DateTime] = None,
                     scheduledEndTime: Option[DateTime] = None,
                     exchangeGoal: Option[ExchangeGoal] = None,
                     exchangeThrottle: Option[ExchangeThrottle] = None,
                     exchangeSites: Option[Set[SiteKey]] = None,
                     contentSources: Option[Set[ContentGroupKey]] = None,
                     enableOmnitureTrackingParamsForAllSites: Option[Boolean] = None,
                     trackingParamsForAllSites: Option[Map[String,String]] = None
                    ):
  ValidationNel[FailureResult, ExchangeRow] = {
    require(newExchangeGuidOption.isDefined == isCreate, "Provide an exchangeGuid if performing exchange creation")
    require(hostGuid.isDefined == isCreate, "Provide a hostGuid if performing exchange creation")

    val beforeRow = fetch(exchangeKey)(exchangeMetaQuerySpec)

    if (!isCreate && beforeRow.isFailure) return beforeRow //Failure: Could not load state of exchange before executing changes

    val beforeRowOpt = beforeRow.toOption
    for {
      _ <- validateExchangeType(beforeRowOpt, exchangeType)
      _ <- validateHostGuid(beforeRowOpt, hostGuid)
      _ <- validateExchangeName(beforeRowOpt, exchangeKey, exchangeName)
      _ <- validateExchangeTimes(beforeRowOpt, scheduledStartTime, scheduledEndTime)
      _ <- validateExchangeSites(beforeRowOpt, exchangeSites, hostGuid)
      _ <- validateContentSources(beforeRowOpt, exchangeSites, contentSources)
      exchange <- modifyPutRefetch(exchangeKey)(put => {
        newExchangeGuidOption.foreach(exchangeGuid => put.value(_.exchangeGuid, exchangeGuid))

        put.value(_.exchangeType, exchangeType)

        hostGuid.foreach(host => put.value(_.hostGuid, host))

        exchangeName.foreach(name => put.value(_.exchangeName, name))

        exchangeStatus.foreach(status => put.value(_.exchangeStatus, status))

        scheduledStartTime.foreach(start => put.value(_.scheduledStartTime, start))

        scheduledEndTime.foreach(end => put.value(_.scheduledEndTime, end))

        exchangeGoal.foreach(goal => put.value(_.goal, goal))

        exchangeThrottle.foreach(throttle => put.value(_.throttle, throttle))

        enableOmnitureTrackingParamsForAllSites.foreach(bool => put.value(_.enableOmnitureTrackingParamsForAllSites, bool))

        val currentSites = beforeRowOpt.map(_.exchangeSites).getOrElse(Set())
        val currentContentSources = beforeRowOpt.map(_.contentSources).getOrElse(Set())
        val currentTrackingParamsForAllSites = beforeRowOpt.map(_.trackingParamsForAllSites).getOrElse(Map())

        val putAndDelteOps = List[PutSpec => Option[PutSpec]](
          passedPut => exchangeSites.map(wantSites => familyKeySetModify(currentSites, wantSites)(exchangeKey, passedPut, _.exchangeSites)),
          passedPut => contentSources.map(wantContentSources => familyKeySetModify(currentContentSources, wantContentSources)(exchangeKey, passedPut, _.contentSources)),
          passedPut => trackingParamsForAllSites.map(wantTrackingParamsForAllSites => familyMapModify(currentTrackingParamsForAllSites, wantTrackingParamsForAllSites)(exchangeKey, passedPut, _.trackingParamsForAllSites))
        )

        // !!!  ------------------------------ From here down, we should be using the result of this foldLeft since it may produce a different put than was passed in ------------------------------
        val newPut = putAndDelteOps.foldLeft(put){ (currentPut,putAndDeleteFn) => putAndDeleteFn(put).getOrElse(currentPut) }

        newPut
      })(exchangeMetaQuerySpec)
      _ = AuditService.logAllChanges(exchangeKey, userId, beforeRowOpt, exchange, Seq("Exchange Service - update exchange")) valueOr  { case f: NonEmptyList[FailureResult] => warn(f, "Campaign Service - update campaign") }
    } yield exchange
  }

  private def monthlyClickMetricsQuery: ScopedMetricsService.QuerySpec = (query) => {
    val monthColumnKey = ScopedMetricsKey(grvtime.currentTime.toDateMonth.getMillis, RecommendationMetricCountBy.click)
    query.withFamilies(_.scopedMetricsByMonth).filter(_.and(_.betweenColumnKeys(_.scopedMetricsByMonth, monthColumnKey, monthColumnKey)))
  }

  def validateExchangeMetricsAndGoalsAndSchedule(exchangeKey: ExchangeKey, checkedBy: String): ValidationNel[FailureResult, (Int, ExchangeStatus.Type)] = {

    def metricsRowToExchangeMetrics(scopedFromToKey: ScopedFromToKey, metricsRow: ScopedMetricsRow): ValidationNel[FailureResult, ExchangeMonthMetric] = {
      for {
        exchangeSiteKey <- scopedFromToKey.from.tryToTypedKey[ExchangeSiteKey].liftFailNel
        toSiteKey <- scopedFromToKey.to.tryToTypedKey[SiteKey].liftFailNel
        fromSiteKey = exchangeSiteKey.siteKey
        key = ScopedMetricsKey(grvtime.currentTime.toDateMonth.getMillis, RecommendationMetricCountBy.click)
      } yield ExchangeMonthMetric(fromSiteKey, toSiteKey, key, metricsRow.scopedMetricsByMonth.getOrElse(key, 0L))
    }

    def getExpectedStatusFromSchedule(currentStatus: ExchangeStatus.Type, start: DateTime, end: DateTime): ExchangeStatus.Type = {
      val now = grvtime.currentTime
      currentStatus match {
        case ExchangeStatus.awaitingStart =>
          if (now.isOnOrAfter(start) && now.isBefore(end)) ExchangeStatus.active
          else if (now.isOnOrAfter(start)) ExchangeStatus.completed
          else currentStatus
        case ExchangeStatus.active =>
          if (now.isBefore(start)) ExchangeStatus.awaitingStart
          else if (now.isOnOrAfter(end)) ExchangeStatus.completed
          else currentStatus
        case other =>
          warn(s"Unexpected status while evaluating exchange ${exchangeKey.exchangeId} status and schedule: ${other}")
          currentStatus
      }
    }

    def fromToScopedKey(fromSiteKey: SiteKey, toSiteKey: SiteKey): ScopedFromToKey = ExchangeSiteKey(exchangeKey, fromSiteKey).toScopedKey.fromToKey(toSiteKey.toScopedKey)

    for {
      exchangeRow <- fetch(exchangeKey)(_.withFamilies(_.meta, _.exchangeSites, _.disabledSites))
      wantStatus = getExpectedStatusFromSchedule(exchangeRow.exchangeStatus, exchangeRow.scheduledStartTime, exchangeRow.scheduledEndTime)
      fromToKeys = exchangeRow.exchangeSites.toList.combinations(2).toList.flatMap {
        case siteA :: siteB :: List() =>
          //combinations will give us each site pair but not the permutations, include both ways (ex: (1,2,3).combinations(2) gives us (1, 2), (1, 3), (2, 3) but we also need (2, 1), (3, 1), (3, 2) )
          fromToScopedKey(siteA, siteB) :: fromToScopedKey(siteB, siteA) :: List()
        case _ => List() //we did combinations(2) so we should never get here... even in the single exchange site case!
      }.toSet
      metrics <- ScopedMetricsService.fetchMulti(fromToKeys, returnEmptyResults = true)(monthlyClickMetricsQuery)
      exchangeMetrics <- metrics.map{ case ((sk, mr)) => metricsRowToExchangeMetrics(sk, mr) }.extrude
      goalCompleteSites = if(exchangeMetrics.nonEmpty) exchangeRow.goal.goalCompletedSitesFromMetrics(exchangeMetrics) else Set()
      throttledSites = if(exchangeMetrics.nonEmpty) exchangeRow.throttle.throttledSitesFromMetrics(exchangeMetrics) else Set()
      disabledSites = goalCompleteSites ++ throttledSites
      opsResult <- setDisabledSitesAndStatusWithAuditLog(exchangeKey, exchangeRow.disabledSites, disabledSites, exchangeRow.exchangeStatus, wantStatus, Settings2.INTEREST_SERVICE_USER_ID, checkedBy)
    } yield {
      val disabledSitesSize = disabledSites.size
      val enabledSize = exchangeRow.exchangeSites.size - disabledSitesSize
      trace(s"Exchange: ${exchangeKey} start disabledSites: ${exchangeRow.disabledSites.size}, end disabledSites: $disabledSitesSize remaining enabled: $enabledSize opsResult: $opsResult")
      (enabledSize, wantStatus)
    }
  }

  //NOTE: this may return a (chained) put other than the one passed in but the returned put will always be for the same key
  def familyKeySetModify[S <: CanBeScopedKey](currentKeySet: Set[S], wantKeySet: Set[S])(key: ExchangeKey, put: PutSpec, family: ExchangesTable => ColumnFamily[ExchangesTable,ExchangeKey,String,ScopedKey, HbaseNull]): PutSpec = {

    val toAddSet = wantKeySet diff currentKeySet
    val toAddMap: Map[ScopedKey, HbaseNull] = toAddSet.map(sk => sk.toScopedKey -> HbaseNull())(breakOut)
    val toDeleteSet = (currentKeySet diff wantKeySet).map(_.toScopedKey)

    //first add the puts if we have any
    if(toAddMap.nonEmpty)
      put.valueMap(t => family(t), toAddMap)


    if(toDeleteSet.nonEmpty) {
      //if we have anything to delete, we have to chain a delete on the put so it can be done with one operation, then return a new put
      put.delete(key).values(t => family(t), toDeleteSet).put(key)
    }
    else
      put
  }
  //  def familyKeySetModify[S <: CanBeScopedKey, T <: HbaseTable[T, K, R], K, R <: HRow[T, K]]
  //  (currentKeySet: Set[S], wantKeySet: Set[S])(key: K, put: PutSpec, family: T => ColumnFamily[T,K,String,ScopedKey, HbaseNull]): PutOp[T, K]] = {

  def familyMapModify[A,B](currentMap: Map[A,B], wantMap: Map[A,B])(key: ExchangeKey, put: PutSpec, family: ExchangesTable => ColumnFamily[ExchangesTable,ExchangeKey,String,A,B]): PutSpec = {

    val putMap = wantMap.filter{ case ((key, value)) => !currentMap.get(key).contains(value) }
    val deleteKeys = currentMap.keySet diff wantMap.keySet

    //first add the puts if we have any
    if(putMap.nonEmpty)
      put.valueMap(t => family(t), putMap)


    if(deleteKeys.nonEmpty) {
      //if we have anything to delete, we have to chain a delete on the put so it can be done with one operation, then return a new put
      put.delete(key).values(t => family(t), deleteKeys).put(key)
    }
    else
      put
  }

  def setExchangeSitesWithAuditLog(exchangeKey: ExchangeKey, currentSites: Set[SiteKey], wantSites: Set[SiteKey], userId: Long = -1l, changedBy: String): ValidationNel[FailureResult,OpsResult] = {

    val opsResultNel = put(familyKeySetModify(currentSites, wantSites)(exchangeKey, Schema.Exchanges.put(exchangeKey), _.exchangeSites))

    val auditStatus = if (opsResultNel.isSuccess) AuditStatus.succeeded else AuditStatus.failed
    AuditService.logEvent(exchangeKey, userId, AuditEvents.exchangeSitesChanged, "exchangeSites", currentSites, wantSites, Seq("Exchange Row - exchange sites change by " + changedBy)) match {
      case Success(auditRow) => AuditService.modifyPut(auditRow.auditKey)(_.value(_.status, auditStatus))
      case Failure(fails) =>
        warn(fails)
    }

    opsResultNel
  }

  def setContentSourcesWithAuditLog(exchangeKey: ExchangeKey,  currentContentSources: Set[ContentGroupKey], wantContentSources: Set[ContentGroupKey], userId: Long = -1l, changedBy: String): ValidationNel[FailureResult,OpsResult] = {

    val opsResultNel = put(familyKeySetModify(currentContentSources, wantContentSources)(exchangeKey, Schema.Exchanges.put(exchangeKey), _.contentSources))

    val auditStatus = if (opsResultNel.isSuccess) AuditStatus.succeeded else AuditStatus.failed
    AuditService.logEvent(exchangeKey, userId, AuditEvents.exchangeContentSourcesChanged, "contentSources", currentContentSources, wantContentSources, Seq("Exchange Row - content sources change by " + changedBy)) match {
      case Success(auditRow) => AuditService.modifyPut(auditRow.auditKey)(_.value(_.status,auditStatus))
      case Failure(fails) =>
        warn(fails)
    }

    opsResultNel
  }

  def setDisabledSitesAndStatusWithAuditLog(exchangeKey: ExchangeKey, currentDisabledSites: Set[SiteKey], wantDisabledSites: Set[SiteKey], currentStatus: ExchangeStatus.Type, wantStatus: ExchangeStatus.Type, userId: Long = -1l, changedBy: String): ValidationNel[FailureResult,OpsResult] = {

    val sitesPut: PutSpec =  familyKeySetModify(currentDisabledSites, wantDisabledSites)(exchangeKey, Schema.Exchanges.put(exchangeKey), _.disabledSites)
    val finalPut = if(currentStatus != wantStatus) sitesPut.value(_.exchangeStatus, wantStatus) else sitesPut
    val opsResultNel = put(finalPut)

    val auditStatus = if (opsResultNel.isSuccess) AuditStatus.succeeded else AuditStatus.failed
    AuditService.logAllChanges(exchangeKey, userId, Option((currentDisabledSites, currentStatus)), (wantDisabledSites, wantStatus), Seq("Exchange Row - disabled sites and/or status change by " + changedBy)) match {
      case Success(auditRowSeq) =>
        AuditService.seqToCombinedPutOp(auditRowSeq.map(_.auditKey))(identity, Schema.Audits2.put(_).value(_.status, auditStatus)).foreach(AuditService.put(_))
      case Failure(fails) =>
        warn(fails)
    }

    opsResultNel
  }

  def siteExchanges(siteGuid: SiteGuid, skipCache: Boolean = true)(query: QuerySpec): ValidationNel[FailureResult, Map[ExchangeKey, ExchangeRow]] = {
    siteExchangesByKey(siteGuid.siteKey, skipCache)(query)
  }

  def siteExchangesByKey(siteKey: SiteKey, skipCache: Boolean = true)(query: QuerySpec): ValidationNel[FailureResult, Map[ExchangeKey, ExchangeRow]] = {
    for {
      //Question: how to differentiate empty vs error?
      //NOTE: if we add a ExchangeSites table, we might want to scan from that since it's major key is the site.... otherwise we might want to store these exchanges in the sites themselves rather than doing a scan...
      exchangeKeys <- ExchangeService.scanToSeq(skipCache)(_.withColumn(_.exchangeSites, siteKey.toScopedKey)).map(_.map(_.exchangeKey))
      exchangeMap <- ExchangeService.fetchMulti(exchangeKeys.toSet)(query)
    } yield exchangeMap
  }
}