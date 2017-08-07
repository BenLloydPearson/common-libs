package com.gravity.utilities.cache

import com.gravity.service
import com.gravity.interests.jobs.intelligence.ScopedMetrics
import com.gravity.interests.jobs.intelligence.operations.user.{UserClickstream, UserRequestKey}
import com.gravity.service.grvroles
import com.gravity.utilities.cache.metrics.{LiveMetricsCacheDebugger, LiveScopedMetricsCache, ScopedMetricsCache}
import com.gravity.utilities.cache.metrics.impl.{LruLiveScopedMetricsCache, LruScopedMetricsCache, ScopedMetricsSkipCache}
import com.gravity.utilities.cache.throttle.Throttle
import com.gravity.utilities.cache.throttle.impl.LruThrottleImpl
import com.gravity.utilities.cache.throttle.model.LiveMetricThrottleRequestKey
import com.gravity.utilities.cache.user.{UserCache, UserCacheDebugger}
import com.gravity.utilities.cache.user.impl.LruUserCache

/**
 * Created by agrealish14 on 4/19/16.
 */
object CacheFactory {

  def getUserCache :UserCache = {

    LruUserCache
  }

  def getUserCacheDebugger: UserCacheDebugger= {

    LruUserCache
  }

  def getMetricsCache : ScopedMetricsCache = {

    if(service.grvroles.isInOneOfRoles(grvroles.MANAGEMENT)) {

      ScopedMetricsSkipCache

    } else if(service.grvroles.isInOneOfRoles(grvroles.METRICS_LIVE)) {

      LruLiveScopedMetricsCache

    } else  {

      LruScopedMetricsCache

    }
  }

  def getLiveScopedMetricsCache : LiveScopedMetricsCache = LruLiveScopedMetricsCache

  def getLiveScopedMetricsCacheDebugger: LiveMetricsCacheDebugger = {

    LruLiveScopedMetricsCache
  }

  val CLICKSTREAM_THROTTLE_MAX_ITEMS: Int = if (grvroles.isInOneOfRoles(grvroles.METRICS_SCOPED_INFERENCE)) 1000 else 500
  lazy val clickstreamThrottle :Throttle[UserRequestKey, UserClickstream] = new LruThrottleImpl[UserRequestKey, UserClickstream]("ClickstreamThrottle", CLICKSTREAM_THROTTLE_MAX_ITEMS)
  lazy val interactionClickstreamThrottle :Throttle[UserRequestKey, UserClickstream] = new LruThrottleImpl[UserRequestKey, UserClickstream]("InteractionClickstreamThrottle", CLICKSTREAM_THROTTLE_MAX_ITEMS)

  val LIVE_METRICS_THROTTLE_MAX_ITEMS: Int = if (grvroles.isInOneOfRoles(grvroles.RECOGENERATION2, grvroles.RECOGENERATION)) 500000 else 10000
  lazy val liveMetricsThrottle: Throttle[LiveMetricThrottleRequestKey, ScopedMetrics] = new LruThrottleImpl[LiveMetricThrottleRequestKey, ScopedMetrics]("LiveMetricsThrottle", LIVE_METRICS_THROTTLE_MAX_ITEMS)


}
