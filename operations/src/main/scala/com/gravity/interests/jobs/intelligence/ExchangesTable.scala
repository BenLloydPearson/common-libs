package com.gravity.interests.jobs.intelligence

import com.gravity.data.configuration.ConfigurationQueryService
import com.gravity.domain.articles.{ContentGroupSourceTypes, ContentGroupStatus}
import com.gravity.domain.recommendations.ContentGroup
import com.gravity.hbase.schema.{DeserializedResult, HRow, HbaseTable}
import com.gravity.interests.jobs.intelligence.ExchangeGoalConverters._
import com.gravity.interests.jobs.intelligence.ExchangeThrottleConverters._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase.{ConnectionPoolingTableManager, ScopedKey}
import com.gravity.interests.jobs.intelligence.operations.ContentGroupService
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._
import com.gravity.utilities.{grvmath, grvtime}
import com.gravity.valueclasses.ValueClassesForDomain._
import org.joda.time.DateTime

import scala.collection._
import scalaz.syntax.validation._
import scalaz.{NonEmptyList, ValidationNel}



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

class ExchangesTable extends HbaseTable[ExchangesTable, ExchangeKey, ExchangeRow] ("exchanges", rowKeyClass = classOf[ExchangeKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
  with ConnectionPoolingTableManager {

  override def rowBuilder(result: DeserializedResult) = new ExchangeRow(result, this)

  val meta: Fam[String, Any] = family[String,Any]("meta",compressed=true)
  val exchangeGuid: Col[ExchangeGuid] = column(meta, "exchangeGuid", classOf[ExchangeGuid])
  val hostGuid: Col[SiteGuid] = column(meta, "hostGuid", classOf[SiteGuid])
  val exchangeName: Col[String] = column(meta, "name", classOf[String])
  val exchangeType: Col[ExchangeType.Type] = column(meta, "type", classOf[ExchangeType.Type])
  val exchangeStatus: Col[ExchangeStatus.Type] = column(meta, "status", classOf[ExchangeStatus.Type])
  val goal: Col[ExchangeGoal] = column(meta, "goal", classOf[ExchangeGoal])
  val throttle: Col[ExchangeThrottle] = column(meta, "throttle", classOf[ExchangeThrottle])
  val scheduledStartTime: Col[DateTime] = column(meta, "start", classOf[DateTime])
  val scheduledEndTime: Col[DateTime] = column(meta, "end", classOf[DateTime])
  val enableOmnitureTrackingParamsForAllSites: Col[Boolean] = column(meta, "omnitureAllSites", classOf[Boolean])

  val exchangeSites: Fam[ScopedKey, HbaseNull] =  family[ScopedKey,HbaseNull]("ets",compressed=true)
  val disabledSites: Fam[ScopedKey, HbaseNull] = family[ScopedKey,HbaseNull]("etds",compressed=true)

  val contentSources: Fam[ScopedKey, HbaseNull] = family[ScopedKey,HbaseNull]("etcs",compressed=true)

  val trackingParamsForAllSites: Fam[String,String] = family[String,String]("tpas",compressed=true)
}

class ExchangeRow (result: DeserializedResult, table: ExchangesTable) extends HRow[ExchangesTable, ExchangeKey](result, table) {

  lazy val exchangeKey: ExchangeKey = rowid

  // FAMILY :: meta
  lazy val exchangeGuid: ExchangeGuid = column(_.exchangeGuid).getOrElse("NO_GUID".asExchangeGuid)

  lazy val hostGuid: SiteGuid = column(_.hostGuid).getOrElse("NO_GUID".asSiteGuid)

  lazy val exchangeName: String = column(_.exchangeName).getOrElse("NO NAME")

  lazy val exchangeType: ExchangeType.Type = column(_.exchangeType).getOrElse(ExchangeType.defaultValue)

  lazy val exchangeStatus: ExchangeStatus.Type = column(_.exchangeStatus).getOrElse(ExchangeStatus.defaultValue)

  lazy val goal: ExchangeGoal = column(_.goal).getOrElse(new ClicksUnlimitedGoal)

  lazy val throttle: ExchangeThrottle = column(_.throttle).getOrElse(new NoExchangeThrottle)

  lazy val scheduledStartTime: DateTime = column(_.scheduledStartTime).getOrElse(grvtime.emptyDateTime)

  lazy val scheduledEndTime: DateTime = column(_.scheduledEndTime).getOrElse(new DateTime(Long.MaxValue))

  lazy val enableOmnitureTrackingParamsForAllSites: Boolean = column(_.enableOmnitureTrackingParamsForAllSites).getOrElse(false)

  // FAMILY :: exchangeSites
  lazy val exchangeSites: Set[SiteKey] = family(_.exchangeSites).flatMap(_._1.tryToTypedKey[SiteKey].toOption).toSet

  // FAMILY :: disabledSites
  lazy val disabledSites: Set[SiteKey] = family(_.disabledSites).flatMap(_._1.tryToTypedKey[SiteKey].toOption).toSet

  // FAMILY :: contentSources
  lazy val contentSources: Set[ContentGroupKey] = family(_.contentSources).flatMap(_._1.tryToTypedKey[ContentGroupKey].toOption).toSet

  // FAMILY :: trackingParamsForAllSites
  lazy val trackingParamsForAllSites: Map[String,String] = family(_.trackingParamsForAllSites)

  def permaCachedContentGroups(failurePrefix: String): ValidationNel[FailureResult,NonEmptyList[ContentGroup]] = PermaCacher.getOrRegister("exchangeContentGroups:" + exchangeKey.toScopedKey.keyString, (7 + grvmath.rand.nextInt(5)) * 60, mayBeEvicted = false) {
    val contentGroupsSet = for {
      contentSource <- contentSources
      contentGroupRow <- ConfigurationQueryService.queryRunner.getContentGroup(contentSource.contentGroupId)
      if contentGroupRow.status == ContentGroupStatus.active && contentGroupRow.sourceType != ContentGroupSourceTypes.exchange
      contentGroup = contentGroupRow.asContentGroup
      siteKey <- ContentGroupService.siteKeyForContentGroup(contentGroup)
      if !disabledSites.contains(siteKey)
    } yield contentGroup

    if(contentGroupsSet.isEmpty)
      FailureResult(failurePrefix + "has no active content sources").failureNel
    else
      toNonEmptyList(contentGroupsSet.toList).successNel
  }
}