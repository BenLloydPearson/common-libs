package com.gravity.algorithms.model

import com.gravity.data.configuration.ConfigurationQueryService
import com.gravity.hbase.schema.OpsResult
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase._
import com.gravity.interests.jobs.intelligence.operations.audit.AuditService
import com.gravity.interests.jobs.intelligence.operations.{ProbabilityDistribution, SiteService, TableOperations}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvcoll._
import com.gravity.utilities.grvmath
import com.gravity.valueclasses.ValueClassesForDomain.{SiteGuid, SitePlacementId}
import org.apache.hadoop.conf.Configuration

import scala.collection._
import scalaz.Scalaz._
import scalaz._

object FeatureSettings extends AlgoSettingsRegistry

object SettingOverrides {
  import com.gravity.logging.Logging._

  /**
   * Fetches setting overrides into a Seq.
   */
  def fetchSettingOverrides(): Seq[(AlgoSetting[_], scala.Seq[(ScopedKey, String)])] = {
    for {
      setting <- FeatureSettings.registry.sortBy(_.name)
      settingOverrides = FeatureSettings.get(setting)
    } yield {
      val overrides = settingOverrides match {
        case Success(overrideRow) =>
          val overrideRows = setting match {
            case switch: Switch =>
              overrideRow.family(_.switches).toSeq
            case variable: Variable =>
              overrideRow.family(_.variables).toSeq
            case strsetting: Setting =>
              overrideRow.family(_.settings).toSeq
            case probs: ProbabilityDistributionSetting =>
              overrideRow.family(_.probabilitySettings).toSeq
            case _ => Seq.empty[(ScopedKey, String)]
          }
          overrideRows
        case Failure(fails) =>
          Seq[(ScopedKey, Any)]()
          //no overrides
      }
      val overridesMapped = overrides.map { case (key: ScopedKey, value: Any) =>

        val valStr = value.toString
        key -> valStr
      }
      (setting, overridesMapped)
    }
  }

  def fromSetting(finder:(AlgoSettingsRegistry) => Setting,value:String): SettingOverrides = SettingOverrides(settingOverrides=mutable.Map(finder(FeatureSettings) -> value))

  def fromSetting(setting: Setting, value: String, finder:(AlgoSettingsRegistry) => Setting, finderValue: String): SettingOverrides = {

    SettingOverrides(settingOverrides=mutable.Map(setting -> value, finder(FeatureSettings) -> finderValue))
  }

  def fromPDS(pdsSeq: Seq[(ProbabilityDistributionSetting, ProbabilityDistribution)]): SettingOverrides = {
    val pdsToStr = pdsSeq.map(e => (e._1, e._2.toString))
    val pdsOverrideMap = mutable.Map(pdsToStr :_*)
    SettingOverrides(probabilityDistributionSettingOverrides = pdsOverrideMap)
  }

  def fromSettingAndPDS(finder: (AlgoSettingsRegistry) => Setting, value: String, pdsSeq: Seq[(ProbabilityDistributionSetting, ProbabilityDistribution)]): SettingOverrides = {
    val overrides = fromSetting(finder, value)
    overrides.copy(probabilityDistributionSettingOverrides = fromPDS(pdsSeq).probabilityDistributionSettingOverrides)
  }

  def fromSettingAndPDS(setting: Setting, value: String, pdsSeq: Seq[(ProbabilityDistributionSetting, ProbabilityDistribution)]): SettingOverrides = {
    val overrides = SettingOverrides(settingOverrides=mutable.Map(setting -> value))
    overrides.copy(probabilityDistributionSettingOverrides = fromPDS(pdsSeq).probabilityDistributionSettingOverrides)
  }

  def fromQueryStringParam(param: String): (Seq[String], SettingOverrides) = {
    val settingOverrides = new SettingOverrides()
    val failureBuffer = mutable.Buffer[String]()
    val overrideArr = param.split('|')
    if (overrideArr.length == 0) failureBuffer += "Splitting param by | produced no results"

    overrideArr.foreach((overrideStr) => {
      val overrideStrArr = overrideStr.split(':')

      try {

        if (overrideStrArr.length != 2) failureBuffer += "Splitting " + overrideStr + " by : produced incorrect results"
        else {
          val settingName = overrideStrArr(0)
          val settingVal = overrideStrArr(1)

          FeatureSettings.settingByName(settingName) match {
            case Some(setting: Setting) =>
              settingOverrides.settingOverrides.put(setting, settingVal)
            case Some(setting: Switch) =>
              settingOverrides.switchOverrides.put(setting, settingVal.toBoolean)
            case Some(setting: Variable) =>
              settingOverrides.variableOverrides.put(setting, settingVal.toDouble)
            case Some(setting: ProbabilityDistributionSetting) =>
              settingOverrides.probabilityDistributionSettingOverrides.put(setting, settingVal)
            case _ =>
              failureBuffer += "Unable to find setting " + settingName
          }
        }
      } catch {
        case ex: Exception =>
          failureBuffer += "Using override " + overrideStr + " produced exception (logged)"
          warn(ex, "Unable to override with string {0}", overrideStr)
      }
    })

    (failureBuffer, settingOverrides)
  }
}

case class SettingOverrides(
                             switchOverrides: mutable.Map[Switch, Boolean] = mutable.Map(),
                             settingOverrides: mutable.Map[Setting, String] = mutable.Map(),
                             variableOverrides: mutable.Map[Variable, Double] = mutable.Map(),
                             probabilityDistributionSettingOverrides: mutable.Map[ProbabilityDistributionSetting, String] = mutable.Map()
                             )

case class AlgoSettingNamespace(variablesMap: Map[ScopedKey, Double] = Map.empty[ScopedKey, Double],
                                switchesMap: Map[ScopedKey, Boolean] = Map.empty[ScopedKey, Boolean],
                                settingsMap: Map[ScopedKey, String] = Map.empty[ScopedKey, String],
                                probabilitySettingsMap: Map[ScopedKey, ProbabilityDistribution] = Map.empty[ScopedKey, ProbabilityDistribution])


trait AlgoSetting[V] { //extends StoredSetting[V] {
  def name: String

  def default: V
  def group: String

  def desc : String

  def loggingEnabled: Boolean

  def key: AlgoSettingKey = AlgoSettingKey(name)

}

trait AlgoSettingResult[V] {
  def algoSetting: AlgoSetting[V]

  def value: V

  def finalScope: Option[ScopedKey]

}

case class Switch(name: String, group:String,default: Boolean, desc:String, loggingEnabled: Boolean = false) extends AlgoSetting[Boolean]

case class Variable(name: String, group:String, default: Double, desc:String, loggingEnabled: Boolean = false) extends AlgoSetting[Double]

case class Setting(name: String, group:String, default: String, desc:String, loggingEnabled: Boolean = false) extends AlgoSetting[String]

case class TransientSetting(name: String, group:String, default: String, desc:String, loggingEnabled: Boolean = false) extends AlgoSetting[String]

case class ProbabilityDistributionSetting(name: String, group: String, default: ProbabilityDistribution, desc: String, loggingEnabled: Boolean = false) extends AlgoSetting[ProbabilityDistribution]

case class VariableResult(value: Double, algoSetting: Variable, finalScope: Option[ScopedKey]) extends AlgoSettingResult[Double]

case class SettingResult(value: String, algoSetting: Setting, finalScope: Option[ScopedKey]) extends AlgoSettingResult[String]

case class SwitchResult(value: Boolean, algoSetting: Switch, finalScope: Option[ScopedKey]) extends AlgoSettingResult[Boolean]

case class ProbabilityDistributionSettingResult(value: ProbabilityDistribution, algoSetting: ProbabilityDistributionSetting, finalScope: Option[ScopedKey]) extends AlgoSettingResult[ProbabilityDistribution]

object PDSUtility {
  // one pds implies multiple possible values for a given variable:
  //  pds1 = n possibilities
  //  pds2 = m possibilities
  //  pds3 = ...
  // total possibilities = n * m * ...
  // this functions returns all possible combinations for multiple pds's
  def toAllPossiblePDSCombinations(pdsResSeq: IndexedSeq[ProbabilityDistributionSettingResult]) : Seq[Seq[(ProbabilityDistributionSetting, ProbabilityDistribution)]] = {
    val listOfVpList = pdsResSeq.map(pdsRes =>  pdsRes.value.valueProbabilityList.toList).toList
    val listProduct = listOfVpList.listProduct

    for (fullListValueProb <- listProduct) yield {
      for {(vp, index) <- fullListValueProb.zipWithIndex
           pds = pdsResSeq(index).algoSetting
           vpList = List(vp.copy(cumulativeProbability = 100))} yield
        (pds, ProbabilityDistribution(vpList))
    }
  }

  def selectOnePDSCombinations(pdsResSeq: IndexedSeq[ProbabilityDistributionSettingResult]) : Seq[(ProbabilityDistributionSetting, ProbabilityDistribution)] = {
    val res = pdsResSeq.map(pdsRes =>    {
      val r = grvmath.randomInt(1, 101)
      val pds = pdsRes.algoSetting
      val vpList = List(pdsRes.value.select(r).copy(cumulativeProbability = 100))
      (pds, ProbabilityDistribution(vpList))
    })

    res
  }

}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
trait AlgoSettingsOps extends TableOperations[AlgoSettingsTable, AlgoSettingKey, AlgoSettingsRow] with AlgoSettingsValidations with AlgoSettingsCache {
  val table: AlgoSettingsTable = Schema.AlgoSettings

  def printOverrides(setting: AlgoSetting[_]): Unit = {
    (for {
      settingRow <- fetch(AlgoSettingKey(setting.name))(_.withAllColumns)
    } yield settingRow) match {
      case Success(row) =>
        row.prettyPrint()
      case Failure(fails) => println("No overrides")
    }
  }

  def clearScopedSwitch(switch: Switch, scopes: Seq[ScopedKey] = universalKeyScopeSeq, userId: Long = -1L, updateZooKeeper: Boolean = true): OpsResult = {
    scopes.foreach { scope =>
      val oldValue = getScopedSwitch(switch, getThisPlusParentScopedKeys(scope).reverse, skipCache = true).value
      AuditService.logEventForScopedKey(scope, userId, AuditEvents.algoOverrideDeleted, switch.name, oldValue, "")
    }
    val res = Schema.AlgoSettings.delete(AlgoSettingKey(switch.name)).values(_.switches, scopes.toSet).execute()

    if(updateZooKeeper) AlgoSettingsZooKeeperStore.reloadSetting(switch.name)

    res
  }

  def clearScopedSetting(setting: Setting, scopes: Seq[ScopedKey] = universalKeyScopeSeq, userId: Long, updateZooKeeper: Boolean = true): OpsResult = {
    scopes.foreach { scope =>
      val oldValue = getScopedSetting(setting, getThisPlusParentScopedKeys(scope).reverse, skipCache = true).value
      AuditService.logEventForScopedKey(scope, userId, AuditEvents.algoOverrideDeleted, setting.name, oldValue, "")
    }

    val res = Schema.AlgoSettings.delete(AlgoSettingKey(setting.name)).values(_.settings, scopes.toSet).execute()

    if(updateZooKeeper) AlgoSettingsZooKeeperStore.reloadSetting(setting.name)

    res
  }

  def clearScopedVariable(variable: Variable, scopes: Seq[ScopedKey] = universalKeyScopeSeq, userId: Long = -1L, updateZooKeeper: Boolean = true): OpsResult = {
    scopes.foreach { scope =>
      val oldValue = getScopedVariable(variable, getThisPlusParentScopedKeys(scope).reverse, skipCache = true).value
      AuditService.logEventForScopedKey(scope, userId, AuditEvents.algoOverrideDeleted, variable.name, oldValue, "")
    }

    val res = Schema.AlgoSettings.delete(AlgoSettingKey(variable.name)).values(_.variables, scopes.toSet).execute()

    if(updateZooKeeper) AlgoSettingsZooKeeperStore.reloadSetting(variable.name)

    res
  }

  def clearScopedVariable(variable: Variable, scope: ScopedKey, userId: Long): OpsResult = clearScopedVariable(variable, Seq(scope), userId)

  def clearScopedProbabilityDistributionSetting(pds: ProbabilityDistributionSetting, scopes: Seq[ScopedKey] = universalKeyScopeSeq, userId: Long = -1L, updateZooKeeper: Boolean = true): OpsResult = {
    scopes.foreach { scope =>
      val oldValue = getScopedProbabilityDistributionSetting(pds, getThisPlusParentScopedKeys(scope).reverse, skipCache = true).value
      AuditService.logEventForScopedKey(scope, userId, AuditEvents.algoOverrideDeleted, pds.name, oldValue, "")
    }

    val res = Schema.AlgoSettings.delete(AlgoSettingKey(pds.name)).values(_.probabilitySettings, scopes.toSet).execute()

    if(updateZooKeeper) AlgoSettingsZooKeeperStore.reloadSetting(pds.name)

    res
  }

  def clearSetting(setting: AlgoSetting[_]): ValidationNel[FailureResult, OpsResult] = {
    val res = delete(Schema.AlgoSettings.delete(AlgoSettingKey(setting.name)))
    AlgoSettingsZooKeeperStore.removeSetting(setting.name)
    res
  }

  def get(setting: AlgoSetting[_], hbaseConf: Configuration = HBaseConfProvider.getConf.defaultConf): Validation[NonEmptyList[FailureResult], AlgoSettingsRow] = for {
    setting <- fetchOrEmptyRow(AlgoSettingKey(setting.name), skipCache = true, hbaseConf = hbaseConf)(_.withAllColumns.filter(_.or(
      _.betweenColumnKeys(_.variables, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
      _.betweenColumnKeys(_.settings, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
      _.betweenColumnKeys(_.switches, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
      _.betweenColumnKeys(_.probabilitySettings, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType))
    )))
  } yield setting

  def overrideInheritedMessage(scopedKey: ScopedKey): String = {
    s"Overriding inherited value from $scopedKey"
  }
  val overrideDefaultMessage = "Override default value"
  val changeExistingMessage = "Changing existing override value"

  private def notesForAudit(scopedKeyOpt: Option[ScopedKey], scope: ScopedKey): Seq[String] = {
    scopedKeyOpt match {
      case Some(oldValueScopedKey) if oldValueScopedKey != scope => Seq(overrideInheritedMessage(oldValueScopedKey))
      case Some(oldValueScopedKey) => Seq(changeExistingMessage)
      case None => Seq(overrideDefaultMessage)
    }
  }

  def setScopedSwitch(switch: Switch, scope: ScopedKey = universalKeyScope, value: Boolean, userId: Long = -1L, updateZooKeeper: Boolean = true): ValidationNel[FailureResult, OpsResult] = {
    val oldValue = getScopedSwitch(switch, getThisPlusParentScopedKeys(scope).reverse, skipCache = true)
    val result = put(Schema.AlgoSettings.put(AlgoSettingKey(switch.name)).valueMap(_.switches, Map(scope -> value)))
    AuditService.logEventForScopedKey(scope, userId, AuditEvents.algoChanges, switch.name, oldValue.value, value,
      notesForAudit(oldValue.finalScope, scope))
    if(updateZooKeeper) AlgoSettingsZooKeeperStore.reloadSetting(switch.name)
    result
  }

  def setScopedSetting(setting: Setting, scope: ScopedKey = universalKeyScope, value: String, userId: Long = -1L, updateZooKeeper: Boolean = true): ValidationNel[FailureResult, OpsResult] = {
    val oldValue = getScopedSetting(setting, getThisPlusParentScopedKeys(scope).reverse, skipCache = true)
    val result = put(Schema.AlgoSettings.put(AlgoSettingKey(setting.name)).valueMap(_.settings, Map(scope -> value)))
    AuditService.logEventForScopedKey(scope, userId, AuditEvents.algoChanges, setting.name, oldValue.value, value,
      notesForAudit(oldValue.finalScope, scope))
    if(updateZooKeeper) AlgoSettingsZooKeeperStore.reloadSetting(setting.name)
    result
  }

  def setScopedVariable(variable: Variable, scope: ScopedKey = universalKeyScope, value: Double, userId: Long = -1L, updateZooKeeper: Boolean = true): ValidationNel[FailureResult, OpsResult] = {
    val oldValue = getScopedVariable(variable, getThisPlusParentScopedKeys(scope).reverse, skipCache = true)
    val result = put(Schema.AlgoSettings.put(AlgoSettingKey(variable.name)).valueMap(_.variables, Map(scope -> value)))
    AuditService.logEventForScopedKey(scope, userId, AuditEvents.algoChanges, variable.name, oldValue.value, value,
      notesForAudit(oldValue.finalScope, scope))
    if(updateZooKeeper) AlgoSettingsZooKeeperStore.reloadSetting(variable.name)
    result
  }

  def setScopedProbabilityDistributionSetting(setting: ProbabilityDistributionSetting, scope: ScopedKey = universalKeyScope, value: ProbabilityDistribution,
                                              logAudit:Boolean = true, userId: Long = -1L, updateZooKeeper: Boolean = true): ValidationNel[FailureResult, OpsResult] = {
    if(logAudit) {
      val oldValue = getScopedProbabilityDistributionSetting(setting, getThisPlusParentScopedKeys(scope).reverse, skipCache = true)
      val result = put(Schema.AlgoSettings.put(AlgoSettingKey(setting.name)).valueMap(_.probabilitySettings, Map(scope -> value.toString)))
      AuditService.logEventForScopedKey(scope, userId, AuditEvents.algoChanges, setting.name, oldValue.value, value,
        notesForAudit(oldValue.finalScope, scope))
      if(updateZooKeeper) AlgoSettingsZooKeeperStore.reloadSetting(setting.name)
      result
    }
    else {
      val result = put(Schema.AlgoSettings.put(AlgoSettingKey(setting.name)).valueMap(_.probabilitySettings, Map(scope -> value.toString)))
      if(updateZooKeeper) AlgoSettingsZooKeeperStore.reloadSetting(setting.name)
      result
    }
  }

  def updateZooKeeper(settingName: String): Unit = {
    AlgoSettingsZooKeeperStore.reloadSetting(settingName)
  }

  def getScopedSetting(setting: Setting, scopes: Seq[ScopedKey] = universalKeyScopeSeq, skipCache: Boolean = false, hbaseConf: Configuration = HBaseConfProvider.getConf.defaultConf): SettingResult = {

    if(skipCache) {

      get(setting, hbaseConf) match {
        case Success(row) =>
          scopes.view.map {
            scope => row.family(_.settings).get(scope).map(str => scope -> str)
          } collectFirst {
            case Some(x) => x
          } match {
            case Some((scopedKey, settingRes)) => SettingResult(settingRes, setting, Some(scopedKey))
            case None => SettingResult(setting.default, setting, None)
          }
        case Failure(fails) =>
          SettingResult(setting.default, setting, None)
      }

    } else {

      getScopedSettingFromCache(setting, scopes)
    }
  }

  def getScopedVariable(variable: Variable, scopes: Seq[ScopedKey] = universalKeyScopeSeq, skipCache: Boolean = false): VariableResult = {

    if(skipCache) {
      get(variable) match {
        case Success(row) =>
          scopes.view.map {
            scope => row.family(_.variables).get(scope).map(str => scope -> str)
          } collectFirst {
            case Some(x) => x
          } match {
            case Some((scopedKey, variableRes)) => VariableResult(variableRes, variable, Some(scopedKey))
            case None => VariableResult(variable.default, variable, None)
          }
        case Failure(fails) =>
          VariableResult(variable.default, variable, None)
      }
    } else {

      getScopedVariableFromCache(variable, scopes)
    }
  }

  def getScopedProbabilityDistributionSetting(setting: ProbabilityDistributionSetting, scopes: Seq[ScopedKey] = universalKeyScopeSeq, skipCache: Boolean = false): ProbabilityDistributionSettingResult = {

    if(skipCache) {
      get(setting) match {
        case Success(row) =>
          scopes.view.map {
            scope => row.family(_.probabilitySettings).get(scope).map(probDistribStr => scope -> ProbabilityDistribution.deserializeFromString(probDistribStr))
          } collectFirst {
            case Some(x) => x
          } match {
            case Some((scopedKey, probDistribution)) => ProbabilityDistributionSettingResult(probDistribution, setting, Some(scopedKey))
            case None => ProbabilityDistributionSettingResult(setting.default, setting, None)
          }
        case Failure(fails) =>
          ProbabilityDistributionSettingResult(setting.default, setting, None)
      }
    } else {

      getScopedProbabilityDistributionSettingFromCache(setting, scopes)
    }
  }

  /**
   * Gets a variable for a single scope not traversing the scoped hierarchy.

  *def getVariableForScope(variable: Variable, scope: ScopedKey,
                          *skipCache: Boolean = false): ValidationNel[FailureResult, Option[Double]] = for {
    *row <- fetchOrEmptyRow(variable.key, skipCache)(_.withColumnsInFamily(_.variables, scope))
  *} yield row.columnFromFamily(_.variables, scope)
   */
  def getVariableForScope(variable: Variable, scope: ScopedKey, skipCache: Boolean = false): ValidationNel[FailureResult, Option[Double]] = {

    if(skipCache) {

      for {
        row <- fetchOrEmptyRow(variable.key, skipCache)(_.withColumnsInFamily(_.variables, scope))
      } yield row.columnFromFamily(_.variables, scope)

    } else {

      getFromCache(variable) match {

        case Some(algoSettingNamespace) =>
          algoSettingNamespace.variablesMap.get(scope).successNel
        case None =>
          None.successNel
      }

    }
  }

  /**
   * Like [[getScopedVariable()]] but for many scopes at once, and not traversing the scoped hierarchy.
   *
   * @return Success is map of ScopedKey to value with keys existing only for values that were actually set for that
   *         specific scoped key.
   */
  def getVariablesForScope(variable: Variable, scopes: Seq[ScopedKey], skipCache: Boolean = false): ValidationNel[FailureResult, Map[ScopedKey, Double]] = {

    if(skipCache) {

      for {
        row <- get(variable)
      } yield row.family(_.variables).filterKeys(k => scopes.contains(k))

    } else {

      getFromCache(variable) match {

        case Some(algoSettingNamespace) =>
          algoSettingNamespace.variablesMap.filterKeys(k => scopes.contains(k)).successNel
        case None =>
          Map.empty[ScopedKey, Double].successNel
      }
    }

  }

  // Ordered seq of keys to find the switch from the first one that works, otherwise fallback to a default value
  def getScopedSwitch(switch: Switch, scopes: Seq[ScopedKey] = universalKeyScopeSeq, skipCache: Boolean = false): SwitchResult = {

    if (skipCache) {
      get(switch) match {
        case Success(row) =>
          scopes.view.map {
            scope => row.family(_.switches).get(scope).map(str => scope -> str)
          } collectFirst {
            case Some(x) => x
          } match {
            case Some((scopedKey, switchRes)) => SwitchResult(switchRes, switch, Some(scopedKey))
            case None => SwitchResult(switch.default, switch, None)
          }
        case Failure(fails) =>
          SwitchResult(switch.default, switch, None)
      }
    } else {

      getScopedSwitchFromCache(switch, scopes)
    }
  }

  def getThisPlusParentScopedKeys(key: ScopedKey): Seq[ScopedKey] = {
    val result = mutable.ArrayBuffer[ScopedKey]()
    key.objectKey match {
      case s@SitePlacementBucketSlotKey(slotIndex, bucketId, placementId, siteId) =>
        val siteKey = SiteKey(siteId)
        val siteGuid = SiteService.siteGuid(siteKey).getOrElse("NO SITE GUID")
        result.append(siteKey.toScopedKey)

        // Convert the placementId (which is non-unique) into the actual unique SP id
        val sprOpt = ConfigurationQueryService.queryRunner.getSitePlacement(siteGuid, placementId.toInt)
        sprOpt.foreach { spr =>
          result.append(SitePlacementKey(placementId, siteId).toScopedKey)
          result.append(SitePlacementIdKey(SitePlacementId(spr.id)).toScopedKey)
        }

        result.append(SitePlacementBucketKey(bucketId, placementId, siteId).toScopedKey)

        result.append(s.toScopedKey)
      case s@SitePlacementBucketKey(_, placementId, siteId) =>
        val siteKey = SiteKey(siteId)
        val siteGuid = SiteService.siteGuid(siteKey).getOrElse("NO SITE GUID")
        result.append(siteKey.toScopedKey)

        val sprOpt = ConfigurationQueryService.queryRunner.getSitePlacement(siteGuid, placementId.toInt)
        sprOpt.foreach { spr =>
          result.append(SitePlacementKey(placementId, siteId).toScopedKey)
          result.append(SitePlacementIdKey(SitePlacementId(spr.id)).toScopedKey)
        }
        result.append(s.toScopedKey)
      case s@SitePlacementKey(placementId, siteId) => // Should only use non-unique placement id
      val siteKey = SiteKey(siteId)
        val siteGuid = SiteService.siteGuid(siteKey).getOrElse("NO SITE GUID")
        result.append(siteKey.toScopedKey)
        result.append(s.toScopedKey)
        val sprOpt = ConfigurationQueryService.queryRunner.getSitePlacement(siteGuid, placementId.toInt)
        sprOpt.foreach { spr =>
          result.append(SitePlacementIdKey(SitePlacementId(spr.id)).toScopedKey)
        }
      case s@SitePlacementIdKey(spid) => // Uses the unique placement id
        val sprOpt = ConfigurationQueryService.queryRunner.getSitePlacements(List(spid.raw))
        sprOpt.foreach { spr =>
          val siteKey = SiteGuid(spr.siteGuid).siteKey
          result.append(siteKey.toScopedKey)
          result.append(SitePlacementKey(spr.placementId.toLong, siteKey.siteId).toScopedKey)
        }
        result.append(s.toScopedKey)
      case s@CampaignKey(siteKey, _) =>
        result.append(siteKey.toScopedKey)
        result.append(s.toScopedKey)
      case s@SiteKey(siteId) =>
        result.append(s.toScopedKey)
      case s@RecommenderIdKey(_, scopedKey) =>
        result.append(s.toScopedKey)
      case _ => result.append(key)
    }

    result
  }
}

