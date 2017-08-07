package com.gravity.interests.jobs.intelligence.operations.recommendations

import com.gravity.utilities.time.GrvDateMidnight

import scala.collection.Map
import com.gravity.interests.jobs.intelligence.StandardMetrics

//object for a clustered topic
case class LSHClusteredTopic(topicUri: String, topic: String, topicId: Long, var standardMetrics: Map[GrvDateMidnight, StandardMetrics], var calculatedViews: Long)
