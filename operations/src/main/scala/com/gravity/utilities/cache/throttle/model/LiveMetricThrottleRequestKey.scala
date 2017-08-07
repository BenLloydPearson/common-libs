package com.gravity.utilities.cache.throttle.model

import com.gravity.interests.jobs.intelligence.hbase.ScopedFromToKey

/**
 * Created by agrealish14 on 11/17/16.
 */
case class LiveMetricThrottleRequestKey(fromToKey:ScopedFromToKey, bucket:Int, impressionsThreshold:Int, hoursThreshold:Int) {

}
