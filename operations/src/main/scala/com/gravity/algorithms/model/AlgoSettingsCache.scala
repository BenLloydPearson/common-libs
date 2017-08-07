package com.gravity.algorithms.model

import akka.actor.Cancellable
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase._
import com.gravity.interests.jobs.intelligence.operations.FieldConverters.AlgoSettingNamespaceConverter
import com.gravity.interests.jobs.intelligence.operations.ProbabilityDistribution
import com.gravity.service.{ZooCommon, grvroles}
import com.gravity.utilities._
import com.gravity.utilities.cache.{BackgroundReloadedField, PermaCacher}
import com.gravity.utilities.components.FailureResult
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, Seq}
import scala.io.StdIn
import scalaz.Scalaz._
import scalaz._

/**
  * Created by agrealish14 on 5/13/15.
  */

object AlgoSettingsCache {
  val useZookeeper: Boolean = Settings2.getBooleanOrDefault("algosettings.use.zookeper", default = true)

  def forceReload() : Unit = {
    if(useZookeeper) {
      AlgoSettingsZooKeeperStore.loadSettings(force = true)
    }
    else {
      AlgoSettingsReloadingStore.forceReload
      AlgoSettingByItemReloadingStore.forceReload()
    }

  }
}

trait AlgoSettingsCache {

  val universalKeyScope: ScopedKey = EverythingKey.toScopedKey
  val universalKeyScopeSeq: scala.Seq[ScopedKey] = Seq(universalKeyScope)

  if(AlgoSettingsCache.useZookeeper) {
    AlgoSettingsZooKeeperStore.start()
  }

  def getScopedSettingFromCache(setting: Setting, scopes: Seq[ScopedKey] = universalKeyScopeSeq): SettingResult = {

    getFromCache(setting) match {

      case Some(algoSettingNamespace) =>
        scopes.view.map {
          scope => {
            algoSettingNamespace.settingsMap.get(scope).map(str => scope -> str)
          }
        } collectFirst {
          case Some(x) => x
        } match {
          case Some((scopedKey, settingRes)) => SettingResult(settingRes, setting, Some(scopedKey))
          case None => SettingResult(setting.default, setting, None)
        }
      case None =>
        SettingResult(setting.default, setting, None)
    }
  }

  def getScopedVariableFromCache(variable: Variable, scopes: Seq[ScopedKey] = universalKeyScopeSeq): VariableResult = {

    getFromCache(variable) match {

      case Some(algoSettingNamespace) =>
        scopes.view.map {
          scope => algoSettingNamespace.variablesMap.get(scope).map(str => scope -> str)
        } collectFirst {
          case Some(x) => x
        } match {
          case Some((scopedKey, variableRes)) => VariableResult(variableRes, variable, Some(scopedKey))
          case None => VariableResult(variable.default, variable, None)
        }
      case None =>
        VariableResult(variable.default, variable, None)
    }
  }

  def getScopedProbabilityDistributionSettingFromCache(setting: ProbabilityDistributionSetting, scopes: Seq[ScopedKey] = universalKeyScopeSeq): ProbabilityDistributionSettingResult = {

    getFromCache(setting) match {

      case Some(algoSettingNamespace) =>
        scopes.view.map {
          scope => algoSettingNamespace.probabilitySettingsMap.get(scope).map(str => scope -> str)
        } collectFirst {
          case Some(x) => x
        } match {
          case Some((scopedKey, probabilityDistributionSettingRes)) => ProbabilityDistributionSettingResult(probabilityDistributionSettingRes, setting, Some(scopedKey))
          case None => ProbabilityDistributionSettingResult(setting.default, setting, None)
        }
      case None =>
        ProbabilityDistributionSettingResult(setting.default, setting, None)
    }
  }

  def getVariableForScopeFromCache(variable: Variable, scope: ScopedKey): ValidationNel[FailureResult, Option[Double]] = {

    getFromCache(variable) match {

      case Some(algoSettingNamespace) =>
        algoSettingNamespace.variablesMap.get(scope).successNel
      case None =>
        None.successNel
    }
  }

  def getVariablesForScopeFromCache(variable: Variable, scopes: Seq[ScopedKey],
                                    skipCache: Boolean = false): ValidationNel[FailureResult, Map[ScopedKey, Double]] = {

    getFromCache(variable) match {

      case Some(algoSettingNamespace) =>
        algoSettingNamespace.variablesMap.filterKeys(k => scopes.contains(k)).successNel
      case None =>
        Map.empty[ScopedKey, Double].successNel
    }
  }

  def getScopedSwitchFromCache(switch: Switch, scopes: Seq[ScopedKey] = universalKeyScopeSeq): SwitchResult = {

    getFromCache(switch) match {

      case Some(algoSettingNamespace) =>
        scopes.view.map {
          scope => algoSettingNamespace.switchesMap.get(scope).map(str => scope -> str)
        } collectFirst {
          case Some(x) => x
        } match {
          case Some((scopedKey, switchRes)) => SwitchResult(switchRes, switch, Some(scopedKey))
          case None => SwitchResult(switch.default, switch, None)
        }
      case None =>
        SwitchResult(switch.default, switch, None)
    }
  }

  def getFromCache(setting: AlgoSetting[_]): Option[AlgoSettingNamespace] = {
    if(AlgoSettingsCache.useZookeeper) {
      AlgoSettingsZooKeeperStore.getSetting(setting.name)
    }
    else if(!grvroles.isInApiRole && !grvroles.isInOneOfRoles(grvroles.WIDGETS_FAILOVER)) {
      AlgoSettingByItemReloadingStore.getSetting(setting.name)
    }
    else {
      cacheMap() match {
        case Some(cacheMap) => cacheMap.get(setting.name)
        case None => None
      }
    }
  }

  private def cacheMap(): Option[GrvConcurrentMap[String, AlgoSettingNamespace]] = {
    AlgoSettingsReloadingStore.cachedItem
  }
}

object Testthis extends App {

  while(true) {
    println(AlgoSettingsReloadingStore.cachedItem.size)
    StdIn.readLine("Press enter")
  }
}

object GranularTestThis extends App {
  com.gravity.algorithms.model.FeatureSettings.registry.foreach(setting => {
    val s = AlgoSettingByItemReloadingStore.getSetting(setting.name)
    println(setting.name + ": " + s.toString)
  })
}

object AllTestThis extends App {
  com.gravity.algorithms.model.FeatureSettings.registry.foreach(setting => {
    val s = AlgoSettingsReloadingStore.cachedItem.get.get(setting.name)
    println(setting.name + ": " +  s.toString)
  })
}

object AlgoSettingsReloadingStore extends BackgroundReloadedField[Option[GrvConcurrentMap[String, AlgoSettingNamespace]]]("AlgoSettings", 60 * 5) {
  implicit val conf: Configuration = HBaseConfProvider.getConf.defaultConf

  override protected def getItem: Option[GrvConcurrentMap[String, AlgoSettingNamespace]] = {
    val algoSettingMap = new GrvConcurrentMap[String, AlgoSettingNamespace]()
    Schema.AlgoSettings.query2.withFamilies(_.variables, _.switches, _.settings, _.probabilitySettings)
      .filter(_.or(
        _.betweenColumnKeys(_.variables, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
        _.betweenColumnKeys(_.settings, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
        _.betweenColumnKeys(_.switches, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
        _.betweenColumnKeys(_.probabilitySettings, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType))
      )).scanToIterable((algoSettingsRow: AlgoSettingsRow) => {
      algoSettingMap.put(algoSettingsRow.rowid.key,
        AlgoSettingNamespace(
          algoSettingsRow.family(_.variables),
          algoSettingsRow.family(_.switches),
          algoSettingsRow.family(_.settings),
          algoSettingsRow.family(_.probabilitySettings).map(probDistrib => probDistrib._1 -> ProbabilityDistribution.deserializeFromString(probDistrib._2))
        )
      )

    })

    Some(algoSettingMap)
  }
}

object AlgoSettingByItemReloadingStore {
 import com.gravity.logging.Logging._
  implicit val conf: Configuration = HBaseConfProvider.getConf.defaultConf

  info("Using per item algo settings cache")

  PermaCacher.addRestartCallback(() => settings.clear())

  private val settings = new GrvConcurrentMap[String, Option[AlgoSettingNamespace]]

  def forceReload(): Unit = {
    settings.clear()
  }

  def getSettingFromHbase(name: String): Boolean = {
    try {
      val settingOpt = Schema.AlgoSettings.query2.withKey(AlgoSettingKey(name)).withFamilies(_.variables, _.switches, _.settings, _.probabilitySettings)
        .filter(_.or(
          _.betweenColumnKeys(_.variables, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
          _.betweenColumnKeys(_.settings, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
          _.betweenColumnKeys(_.switches, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
          _.betweenColumnKeys(_.probabilitySettings, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType))
        )).singleOption(noneOnEmpty = true).map(algoSettingsRow => {
        AlgoSettingNamespace(
          algoSettingsRow.family(_.variables),
          algoSettingsRow.family(_.switches),
          algoSettingsRow.family(_.settings),
          algoSettingsRow.family(_.probabilitySettings).map(probDistrib => probDistrib._1 -> ProbabilityDistribution.deserializeFromString(probDistrib._2))
        )
      })
      settings.update(name, settingOpt)
      true
    }
    catch {
      case e:Exception =>
        warn(e, "Exception getting algo setting " + name)
        false
    }

  }

  private def getSettingPermaCached(name: String) = {
    PermaCacher.getOrRegister("AlgoSetting-" + name, getSettingFromHbase(name), 60 * 5, mayBeEvicted = false, resourceType = "AlgoSetting")
  }

  def getSetting(name: String): Option[AlgoSettingNamespace] = {
    //if it's in our map, return it.
    settings.get(name) match {
      case Some(settingOpt) =>
        settingOpt
      case None =>
        this.synchronized { //on startup we might have a bunch of simultaneous requests for the same setting; avoid flooding hbase and confusing permacacher
          settings.get(name) match { //might have been filled in while we were waiting for the lock
            case Some(settingOpt) => settingOpt
            case None => //okay NOW we're sure we're the only one trying
              if (getSettingPermaCached(name: String))
                settings.get(name).get
              else
                None
          }
        }
    }
  }

}

object AlgoSettingsZooKeeperStore extends ZooKeeperWatcher[Option[AlgoSettingNamespace]] {
 import com.gravity.logging.Logging._
  implicit def conf: Configuration = HBaseConfProvider.getConf.defaultConf

  info("Using zoo kept settings cache")

  private val backFillFromHbase = true

  val instantUpdateExclusionList: Set[String] = Set("yo-churner-prob")

  def getSetting(name: String) : Option[AlgoSettingNamespace] = {
    zooKeptDataMap.get(name) match {
      case Some(opt) => opt
      case None =>
        if(backFillFromHbase) {
          info("Requested algo setting " + name + " not found in zookeeper, getting from Hbase.")
          val fromHBase = getSettingFromHbase(name)
          writeToZookeeper(name, fromHBase)
          zooKeptDataMap.update(name, fromHBase)
          fromHBase
        }
        else {
          zooKeptDataMap.update(name, None)
          None
        }
    }
  }

  override def childUpdated(name: String, data: Option[AlgoSettingNamespace]): Unit = {
    info("Got updated data for setting " + name)
  }

  override def childRemoved(name : String): Unit = {
    info("Setting " + name + " removed.")
  }

  override def childAdded(name: String, data: Option[AlgoSettingNamespace]): Unit = {
    info("Setting " + name + " added.")
  }

  private def getSettingFromHbase(name: String) = {
    try {
      val settingOpt = Schema.AlgoSettings.query2.withKey(AlgoSettingKey(name)).withFamilies(_.variables, _.switches, _.settings, _.probabilitySettings)
        .filter(_.or(
          _.betweenColumnKeys(_.variables, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
          _.betweenColumnKeys(_.settings, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
          _.betweenColumnKeys(_.switches, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
          _.betweenColumnKeys(_.probabilitySettings, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType))
        )).singleOption(noneOnEmpty = true).map(algoSettingsRow => {
        AlgoSettingNamespace(
          algoSettingsRow.family(_.variables),
          algoSettingsRow.family(_.switches),
          algoSettingsRow.family(_.settings),
          algoSettingsRow.family(_.probabilitySettings).map(probDistrib => probDistrib._1 -> ProbabilityDistribution.deserializeFromString(probDistrib._2))
        )
      })
      settingOpt
    }
    catch {
      case e:Exception =>
        warn(e, "Exception getting algo setting " + name)
        None
    }

  }

  override val buildInitialCache: Boolean = true

  override def deserializeData(data: Array[Byte]): ValidationNel[FailureResult, Option[AlgoSettingNamespace]] = {
    if(data == null || data.isEmpty)
      None.successNel
    else {
      AlgoSettingNamespaceConverter.getInstanceFromBytes(data) match {
        case Success(a) =>
          Some(a).successNel[FailureResult]
        case Failure(fails) =>
          fails.failure
      }
    }
  }

  override def path: String = "/AlgoSettings"

  def getSettingPath(settingName: String): String = path + "/" + settingName

  def removeSetting(settingName: String): Unit = {
    if(!instantUpdateExclusionList.contains(settingName))
      ZooCommon.deleteNodeIfExists(path = getSettingPath(settingName))
    zooKeptDataMap.update(settingName, None)
  }

  private def writeToZookeeper(settingName: String, settingOpt: Option[AlgoSettingNamespace]): Unit = {
    val path = getSettingPath(settingName)

    val bytes = settingOpt match {
      case Some(setting) =>
        AlgoSettingNamespaceConverter.toBytes(setting)
      case None =>
        Array.empty[Byte]
    }

    ZooCommon.createOrUpdateNode(ZooCommon.getEnvironmentClient, path, bytes)
  }

  def reloadSetting(settingName: String): Unit = {
    if(!instantUpdateExclusionList.contains(settingName)) {
      val newSetting = getSettingFromHbase(settingName)
      zooKeptDataMap.update(settingName, newSetting)
      writeToZookeeper(settingName, newSetting)
    }
  }

  private def arraysEqual(arr1: Array[Byte], arr2: Array[Byte]) : Boolean = {
    if(arr1.length != arr2.length) {
      //info("Length " + arr1.length + " does not equal " + arr2.length)
      return false
    }
    for(i <- arr1.indices) {
      if(arr1(i) != arr2(i)) {
        //info("Byte " + i + " of " + arr1.length)
        return false
      }
    }
    true
  }

  def loadSettings(force: Boolean = false): Unit = {
    info("Putting all algo settings in ZooKeeper")

    val currentHBaseSettings = ArrayBuffer[String]() //used at the end to clear since they just stop existing in hbase but need to be cached as empty

    Schema.AlgoSettings.query2.withFamilies(_.variables, _.switches, _.settings, _.probabilitySettings)
      .filter(_.or(
        _.betweenColumnKeys(_.variables, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
        _.betweenColumnKeys(_.settings, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
        _.betweenColumnKeys(_.switches, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType)),
        _.betweenColumnKeys(_.probabilitySettings, ScopedKey.partialByTypeFrom(ScopedKeyTypes.filterMinType), ScopedKey.partialByTypeTo(ScopedKeyTypes.filterMaxType))
      )).scanToIterable((algoSettingsRow: AlgoSettingsRow) => {
      val settingName = algoSettingsRow.rowid.key
      currentHBaseSettings += settingName

      val setting = AlgoSettingNamespace(
        algoSettingsRow.family(_.variables),
        algoSettingsRow.family(_.switches),
        algoSettingsRow.family(_.settings),
        algoSettingsRow.family(_.probabilitySettings).map(probDistrib => probDistrib._1 -> ProbabilityDistribution.deserializeFromString(probDistrib._2))
      )
      val path = getSettingPath(settingName)
      val bytes = AlgoSettingNamespaceConverter.toBytes(setting)

      val doUpdate = force || {
        zooKeptDataMap.get(settingName) match {
        case Some(existingSettingOpt) =>
          val existingBytes = existingSettingOpt match {
            case Some(b) => AlgoSettingNamespaceConverter.toBytes(b)
            case None => Array.empty[Byte]
          }
          if(!arraysEqual(existingBytes, bytes)) {
            info("Setting " + settingName + " changed")
            true
          }
          else
            false
        case None =>
          info("Setting " + settingName + " not found in zookeeper.")
          true
      }}
      if(doUpdate) {
        info("Updating setting '" + settingName + "'")
        zooKeptDataMap.update(settingName, Some(setting))
        ZooCommon.createOrUpdateNode(ZooCommon.getEnvironmentClient, path, bytes) match {
          case Success(s) =>
          case Failure(fails) => warn("Could not update setting " + settingName + ": " + fails.toString)
        }
      }
      else {
        info("Setting " + settingName + " has not changed.")
      }
    })

    //find anything that we have locally that doesn't exist in hbase and isn't already blank and wipe it
    val keysToClear = zooKeptDataMap.keys.toSet.filter(key => !currentHBaseSettings.contains(key) && zooKeptDataMap.getOrElse(key, None).isDefined) //getorelse because we're not locking anything
    info("Found " + keysToClear.size + " keys that have been removed from hbase but not zookeeper. " + keysToClear.mkString(","))
    keysToClear.foreach(key => {
      ZooCommon.createOrUpdateNode(ZooCommon.getEnvironmentClient, getSettingPath(key), Array.empty[Byte])
      zooKeptDataMap.update(key, None)
    })
  }
}



object AlgoSettingsZooKeeperUpdater {
 import com.gravity.logging.Logging._
  import ZooCommon.system
  import system._

  import concurrent.duration._

  val updateTimeStampPath = "/AlgoSettingsUpdateTimeStamp"
  val updateInterval: FiniteDuration = 5.minutes
  var updateCancellableOption : Option[Cancellable] = None
  val lock: InterProcessMutex = ZooCommon.buildLock(ZooCommon.getEnvironmentClient, "/AlgoSettingsUpdateLock")

  def start() {
    updateCancellableOption match {
      case Some(updateCancellable) =>
        warn("Attempted to start already started AlgoSettingsZooKeeperUpdater")
      case None =>
        updateCancellableOption = Some(system.scheduler.scheduleOnce(0.second)(update()))
        info("AlgoSettingsZooKeeperUpdater started.")
    }
  }

  def stop() {
    updateCancellableOption match {
      case Some(updateCancellable) =>
        updateCancellable.cancel()
        updateCancellableOption = None
        info("AlgoSettingsZooKeeperUpdater stopped.")
      case None =>
        warn("Attempted to stop already stopped AlgoSettingsZooKeeperUpdater")
    }
  }

  def update(): Unit = {
    lock.acquire()
    try {
      val doUpdate = ZooCommon.getTimeStamp(ZooCommon.getEnvironmentClient, updateTimeStampPath) match {
        case Some(millis) => (System.currentTimeMillis() - millis) > updateInterval.toMillis
        case None => true
      }
      if (doUpdate) {
        if (!AlgoSettingsZooKeeperStore.isStarted) AlgoSettingsZooKeeperStore.start()
        AlgoSettingsZooKeeperStore.loadSettings()
        warn("Completed zoo keeper algo settings update. Updating again in " + updateInterval.toString)
        ZooCommon.setTimeStamp(ZooCommon.getEnvironmentClient, updateTimeStampPath, System.currentTimeMillis())
      }
      else {
        warn("Zoo keeper algo settings update was completed elsewhere. Waiting " + updateInterval.toString + " trying again.")
      }
    }
    finally {
      lock.release()
    }
    updateCancellableOption = Some(system.scheduler.scheduleOnce(updateInterval)(update()))
  }

}


object AlgoSettingsZooApp extends App {
  //AlgoSettingsZooKeeperStore.start()
  //AlgoSettingsZooKeeperStore.zooKeptDataMap.keys.foreach(AlgoSettingsZooKeeperStore.removeSetting)
  //AlgoSettingsZooKeeperStore.loadSettings()
  AlgoSettingsZooKeeperUpdater.start()
  StdIn.readLine("enter to exit")
  AlgoSettingsZooKeeperUpdater.stop()
  Thread.sleep(1000)
}

object AlgoSettingsJustWatcher extends App {
  AlgoSettingsZooKeeperStore.start()
  //AlgoSettingsZooKeeperStore.zooKeptDataMap.keys.foreach(println)
  StdIn.readLine("enter to exit")
}