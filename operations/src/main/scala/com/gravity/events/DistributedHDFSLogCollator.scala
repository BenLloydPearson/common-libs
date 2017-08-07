package com.gravity.events

import akka.actor.Cancellable
import com.gravity.utilities._

import scala.concurrent.duration._

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 1/21/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object DistributedHDFSLogCollator {
 import com.gravity.logging.Logging._

  import com.gravity.events.EventLogSystem.system

  private val categoryWatchers = new GrvConcurrentMap[String, DistributedLogStageWatcher]
  private var categoryCreationCancellable: Option[Cancellable] = None
  val categoryName = "HDFS Event Logs Distributed"

  def start() {
    createCategoryWatchers()
  }

  private def createCategoryWatchers() {
    import system.dispatcher

    categoryCreationCancellable = None
    val categoryFolders = Utilities.getHfdsStageSequenceCategoryDirs

    categoryFolders.foreach {
      categoryPath => {
        val categoryName = categoryPath.getName
        categoryWatchers.getOrElseUpdate(categoryName, new DistributedLogStageWatcher(categoryName))
      }
    }
    categoryCreationCancellable = Some(system.scheduler.scheduleOnce(5.minutes)(createCategoryWatchers()))
  }

  def shutdown() {
    categoryCreationCancellable.foreach(_.cancel())
    categoryWatchers.values.foreach(_.stop())
  }

}