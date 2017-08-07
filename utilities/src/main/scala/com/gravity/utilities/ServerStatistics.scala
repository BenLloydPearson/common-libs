package com.gravity.utilities

import scala.collection.concurrent

object ServerStatistics extends ServerStatisticsMXBean {
 import com.gravity.logging.Logging._

  val monitors = new GrvConcurrentMap[String, concurrent.Map[String, MonitorInstanceTrait]]()

  def getMonitor(name: String): Option[MonitorResult] = {

    def monitorCategorySizeCheck(instanceNameToInstances: concurrent.Map[String, MonitorInstanceTrait]): MonitorResult = {
      (instanceNameToInstances.values.size, instanceNameToInstances.values.headOption) match {
        case (size, Some(instance)) if size > instance.maxInstances =>
          MonitorResult(name, 1L, Seq(s"Monitor has more instances than allowed. Found: $size Max: ${instance.maxInstances}"))
        case _ =>
          MonitorResult(name, 0L, Seq())
      }
    }

    monitors.get(name).map { instances =>
      val monitorResults = instances.values.map(_.toMonitorResult) ++ Seq(monitorCategorySizeCheck(instances))
      MonitorResult(monitorResults.head.name, monitorResults.map(_.count).sum, monitorResults.flatMap(_.details).toSeq)
    }
  }

  def getAllMonitors: Iterable[MonitorResult] = monitors.keySet.flatMap(getMonitor)

  def register(monitor: MonitorInstanceTrait): MonitorInstanceTrait = {
    monitors.getOrElseUpdate(monitor.name, {
      new GrvConcurrentMap[String, MonitorInstanceTrait]
    }).putIfAbsent(monitor.instance, monitor)

    monitor
  }

  override def getStatistics: Array[String] = {
    Counters.getAll.map(_.toString).toArray
  }
}

trait ServerStatisticsMXBean {
  def getStatistics: Array[String]
}

