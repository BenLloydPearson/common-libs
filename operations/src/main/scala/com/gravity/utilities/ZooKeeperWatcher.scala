package com.gravity.utilities

import com.gravity.service.ZooCommon
import com.gravity.utilities.components.FailureResult
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{ChildData, PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}

import scala.reflect.ClassTag
import scalaz.{Failure, Success, ValidationNel}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 2/3/16
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

abstract class ZooKeeperWatcher[T:ClassTag] extends PathChildrenCacheListener {
 import com.gravity.logging.Logging._
    private def getNameFromPath(path: String) : String = {
      grvstrings.tokenize(path, "/").last
    }

  //must be lazy so the client and path exist
  private var hasBeenStarted = false

  private lazy val cache = buildCache()
  val buildInitialCache : Boolean
  val zooKeptDataMap = new GrvConcurrentMap[String, T]()
  val frameworkClient : CuratorFramework = ZooCommon.getEnvironmentClient

  def isStarted = hasBeenStarted

  def path : String

  def deserializeData(data: Array[Byte]) : ValidationNel[FailureResult, T]

  private def buildCache() = {
    val cache = new PathChildrenCache(frameworkClient, path, true)
    if(buildInitialCache)
      cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE)
    else
      cache.start(PathChildrenCache.StartMode.NORMAL)
    cache.getListenable.addListener(this)
    cache
  }

  def postInitialize() {}

  def childAdded(name: String, data: T) {}

  def childRemoved(name : String) {}

  def childUpdated(name: String, data: T) {}

  def connectionReconnected() {}

  def connectionSuspended() {}

  def connectionLost() {}

  def start(): Unit = {
    if(!hasBeenStarted) {
      this.synchronized {
        if(!hasBeenStarted) {
          import scala.collection.JavaConversions._

          val pokedCache = cache

          if (buildInitialCache) pokedCache.getCurrentData().foreach(updateMap)

          info("Watching " + path + ", got " + zooKeptDataMap.size() + " initial items.")

          postInitialize()
        }
      }
    }
    hasBeenStarted = true
  }

  def updateMap(child: ChildData) {
    deserializeData(child.getData) match {
      case Success(item) =>
        zooKeptDataMap.update(getNameFromPath(child.getPath), item)
      case Failure(fails) =>
        warn("Failed to deserialize zookeeper node " + child.getPath + ": " + fails.toString)
    }
  }

  override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
    import PathChildrenCacheEvent.Type._

    event.getType match {
      case CHILD_ADDED =>
        val child = event.getData
        deserializeData(child.getData) match {
          case Success(item) =>
            val name = getNameFromPath(child.getPath)
            zooKeptDataMap.update(name, item)
            childAdded(name, item)
          case Failure(fails) =>
            warn("Failed to deserialize zookeeper node " + child.getPath + ": " + fails.toString)
        }
      case CHILD_REMOVED =>
        val child = event.getData
        val path = child.getPath
        val name = getNameFromPath(path)
        zooKeptDataMap.remove(name)
        childRemoved(name)
      case CHILD_UPDATED =>
        val child = event.getData
        deserializeData(child.getData) match {
          case Success(item) =>
            val name = getNameFromPath(child.getPath)
            zooKeptDataMap.update(name, item)
            childUpdated(name, item)
          case Failure(fails) =>
            warn("Failed to deserialize zookeeper node " + child.getPath + ": " + fails.toString)
        }
      case CONNECTION_RECONNECTED => connectionReconnected()
      case CONNECTION_SUSPENDED => connectionSuspended()
      case CONNECTION_LOST => connectionLost()
      case _ => warn("Ignored ZooKeeper Curator Path Child Event " + event + " occurred.")
    }
  }
}


//case class StoredSettingDefinition[T](name: String, group: String, desc: String, default:T)
//
//trait StoredSettingValue[T] {
//  def definition: StoredSettingDefinition[T]
//  def value: T
//  def finalScope: Option[ScopedKey]
//}
//
////abstract class StoredSettingConverter[T] extends FieldConverter[StoredSetting[T]] {
////  //val
////
////  override def toValueRegistry(o: StoredSetting[T]): FieldValueRegistry = ???
////
////  override def fromValueRegistry(reg: FieldValueRegistry): StoredSetting[T] = ???
////
////  override val fields: FieldRegistry[StoredSetting[T]] = _
////}
//
//object SettingsWatcher {
//  val basePath : String = "/settings"
//  def getNameFromPath(path: String) : String = {
//    grvstrings.tokenize(path, "/").last
//  }
//
//  class StoredSettingDefinitionConverter[T](valueConverter: FieldConverter[T], fallback: T)(implicit m: Manifest[T]) extends FieldConverter[StoredSettingDefinition[T]] {
//    override def toValueRegistry(o: StoredSettingDefinition[T]): FieldValueRegistry = {
//      import o._
//      new FieldValueRegistry(fields, version = 0)
//        .registerFieldValue(0, name)
//        .registerFieldValue(1, group)
//        .registerFieldValue(2, desc)
//        .registerFieldValue[T](3, default)(m, valueConverter)
//    }
//
//    override def fromValueRegistry(reg: FieldValueRegistry): StoredSettingDefinition[T] = {
//      StoredSettingDefinition[T](reg.getValue[String](0), reg.getValue[String](1), reg.getValue[String](2), reg.getValue[T](3))
//    }
//
//    override val fields: FieldRegistry[StoredSettingDefinition[T]] = new FieldRegistry[StoredSettingDefinition[T]]("StoredSettingDefinition", version = 0)
//      .registerStringField("name", 0)
//      .registerStringField("group", 1)
//      .registerStringField("desc", 2)
//      .registerField[T]("defaultValue", 3, fallback)(m, valueConverter)
//  }
//}
//
////to watch the base settings directory and maintain watchers for each group
//trait SettingsParentWatcher extends ZooKeeperWatcher {
 import com.gravity.logging.Logging._
//  import SettingsWatcher._
//
//  private var groups = mutable.Set[String]()
//  //private val groupWatchers = new GrvConcurrentMap[String, SettingsGroupWatcher]()
//
//  override def childRemoved(child: ChildData): Unit = {
//    val groupName = getNameFromPath(child.getPath)
//    info("Settings group " + groupName + " removed.")
//    groups -= groupName
//   // groupWatchers.remove(groupName)
//  }
//
//  override def childAdded(child: ChildData): Unit = {
//    val groupName = getNameFromPath(child.getPath)
//    info("Settings group " + groupName + " added.")
//    groups += groupName
//    //not creating the groupwatcher until something asks for a setting in that group
//  }
//
//  override def childUpdated(child: ChildData): Unit = {
//    //don't need to do anything; the group watchers deal
//  }
//
//  override def path: String = SettingsWatcher.basePath
//
// // def getGroupWatcher : Option[GroupWatcher]
//}
//
//class SettingsDefinitionWatcher(val frameworkClient : CuratorFramework, val settingsGroup : String) extends ZooKeeperWatcher {
 import com.gravity.logging.Logging._
//  import SettingsWatcher._
//  override def path: String = SettingsWatcher.basePath + "/" + settingsGroup + "/definitions"
//
//  //we don't know how to serialize the setting values until someone asks for one, so we store the bytes and deserialize on demand
//  private val definitionsBytes = new GrvConcurrentMap[String, Array[Byte]]()
//  private val definitions = new GrvConcurrentMap[String, StoredSettingDefinition[_]]()
//  private val converters = new GrvConcurrentMap[String, StoredSettingDefinitionConverter[_]]
//
//  def processInitialChildren(children : Seq[ChildData]): Unit = {
//    children.foreach(child => {
//      definitionsBytes.update(getNameFromPath(child.getPath), child.getData)
//    })
//  }
//
//  override def childRemoved(child: ChildData): Unit = {
//    val name = getNameFromPath(child.getPath)
//    info("Setting definition for " + name + " in " + settingsGroup + " removed.")
//    definitionsBytes.remove(name)
//    definitions.remove(name)
//  }
//
//  override def childAdded(child: ChildData): Unit = {
//    val name = getNameFromPath(child.getPath)
//    info("Setting definition for " + name + " in " + settingsGroup + " added.")
//    definitionsBytes.update(name, child.getData)
//    definitions.remove(name)
//  }
//
//  override def childUpdated(child: ChildData): Unit = {
//    val name = getNameFromPath(child.getPath)
//    info("Setting definition for " + name + " in " + settingsGroup + " updated.")
//    definitionsBytes.update(name, child.getData)
//    definitions.remove(name)
//  }
//
//  def getDefinition[T](name: String, fallback: T)(implicit m: Manifest[T], ev: FieldConverter[T]) : Validation[FailureResult, StoredSettingDefinition[T]] = {
//    try {
//      //Note: should add locking to sync the defintions and definitionbytes collections
//      definitions.get(name) match {
//        case Some(definition) =>
//          Success(definition.asInstanceOf[StoredSettingDefinition[T]])
//        case None =>
//          definitionsBytes.get(name) match {
//            case Some(bytes) =>
//              val definitionConverter = converters.getOrElseUpdate(name, new StoredSettingDefinitionConverter[T](ev, fallback))
//
//              definitionConverter.getInstanceFromBytes(bytes) match {
//                case Success(definition) =>
//                  definitions.update(name, definition)
//                  Success(definition.asInstanceOf[StoredSettingDefinition[T]])
//                case Failure(fails) =>
//                  FailureResult("Could not deserialize setting " + name + " from group " + settingsGroup + ": " + fails.toString()).fail
//              }
//            case None =>
//              FailureResult("Setting " + name + " not found in group " + settingsGroup).fail[StoredSettingDefinition[T]]
//          }
//      }
//    }
//    catch {
//      case e: Exception => FailureResult("Exception getting setting " + name + " from group " + settingsGroup).fail[StoredSettingDefinition[T]]
//    }
//  }
//}


//class SettingsGroupWatcher(val frameworkClient : CuratorFramework, val settingsGroup : String) extends ZooKeeperWatcher {
 import com.gravity.logging.Logging._
//  override def path: String = SettingsWatcher.basePath + "/" + settingsGroup
//  //private val
//
//  override def childRemoved(child: ChildData): Unit = {
//
//  }
//
//  override def childAdded(child: ChildData): Unit = {
//
//  }
//
//  override def childUpdated(child: ChildData): Unit = {
//
//  }
//}

