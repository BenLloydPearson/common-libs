package com.gravity.interests.jobs.intelligence.operations.metrics

import scala.collection._
import scala.concurrent.duration._

import com.gravity.domain.FieldConverters.{LiveMetricRequestConverter, LiveMetricResponseConverter, LiveMetricUpdateConverter}
import com.gravity.interests.jobs.intelligence.ScopedMetrics
import com.gravity.interests.jobs.intelligence.hbase.{ScopedFromToKey, ScopedMetricsBucket}
import com.gravity.interests.jobs.intelligence.operations.metric.{LiveMetricRequest, LiveMetricResponse, LiveMetricUpdate}
import com.gravity.service.grvroles
import com.gravity.service.remoteoperations.{RemoteOperationsClient, RemoteOperationsHelper, SplitRequestResponse}
import com.gravity.utilities.cache.CacheFactory
import com.gravity.utilities.cache.throttle.Throttle
import com.gravity.utilities.cache.throttle.model.LiveMetricThrottleRequestKey

/**
 * Created by agrealish14 on 10/10/16.
 */

object LiveMetricsService extends LiveMetricsService

trait LiveMetricsService {
  import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._
  private val counterCategory = "Live Metrics"

  private val remoteErrorCounter = getOrMakePerSecondCounter(counterCategory, "Errors - Remote", shouldLog = false)

  RemoteOperationsHelper.registerReplyConverter(LiveMetricRequestConverter)
  RemoteOperationsHelper.registerReplyConverter(LiveMetricResponseConverter)
  RemoteOperationsHelper.registerReplyConverter(LiveMetricUpdateConverter)

  val timeout: FiniteDuration = if (isRecogenRole) 20000.millis else 10000.millis

  val throttle: Throttle[LiveMetricThrottleRequestKey, ScopedMetrics] = CacheFactory.liveMetricsThrottle

  def updateLiveMetrics(updates:LiveMetricUpdate) = {

    RemoteOperationsClient.clientInstance.sendSplit[LiveMetricUpdate](updates, LiveMetricUpdateConverter)
  }

  def getLiveMetrics(keys:List[ScopedFromToKey],
                     bucket: ScopedMetricsBucket.Value = ScopedMetricsBucket.hourly,
                     impressionsThreshold:Int = 0,
                     hoursThreshold:Int = 48): Map[ScopedFromToKey, ScopedMetrics] ={

    getLiveMetrics(LiveMetricRequest(keys, bucket.id, impressionsThreshold, hoursThreshold))
  }

  def getLiveMetrics(request:LiveMetricRequest): Map[ScopedFromToKey, ScopedMetrics] ={

    if(isThrottled){
      throttledGet(request)
    } else {
      getFromOrigin(request)
    }
  }

  def clearThrottle: Unit = throttle.clear

  private def getFromOrigin(request:LiveMetricRequest): Map[ScopedFromToKey, ScopedMetrics] ={

    val res: SplitRequestResponse[LiveMetricResponse] = RemoteOperationsClient.clientInstance.requestResponseSplit[LiveMetricRequest, LiveMetricResponse](request, timeout, LiveMetricRequestConverter)

    if(res.failures.nonEmpty) {
      remoteErrorCounter.incrementBy(1)
      warn("live metrics call failed: " + res.failures.map(_.message))
    }

    res.responseObjectOption match {
      case Some(liveMetrics: LiveMetricResponse) => liveMetrics.toMetricsMap
      case None => Map.empty[ScopedFromToKey, ScopedMetrics]
    }
  }

  private def throttledGet(request:LiveMetricRequest): Map[ScopedFromToKey, ScopedMetrics] ={

    var missKeys = mutable.ArrayBuffer[ScopedFromToKey]()

    val resultsMap: Map[ScopedFromToKey, ScopedMetrics] = request.keys.map(k => {

      val throttleKey = new LiveMetricThrottleRequestKey(k, request.bucket, request.impressionsThreshold, request.hoursThreshold)

      throttle.get(throttleKey) match {
        case Some(metrics) => k -> metrics
        case None => {
          missKeys += k

          k-> ScopedMetrics.empty
        }
      }

    }).toMap

    if(missKeys.nonEmpty) {

      countPerSecond(counterCategory, "LruLiveMetricsThrottle.miss")

      val missRequest = request.copy(keys = missKeys.toList)

      val fromOrigin = getFromOrigin(missRequest)

      fromOrigin.foreach(row => {

        val throttleKey = new LiveMetricThrottleRequestKey(row._1, request.bucket, request.impressionsThreshold, request.hoursThreshold)

        throttle.put(throttleKey, row._2)
      })

      (resultsMap ++ fromOrigin).toMap

    } else {

      countPerSecond(counterCategory, "LruLiveMetricsThrottle.hit")

      resultsMap.toMap
    }
  }


  private def isRecogenRole = grvroles.isInOneOfRoles(grvroles.RECOGENERATION, grvroles.RECOGENERATION2)

  private val isThrottled: Boolean = isRecogenRole
}
