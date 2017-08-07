package com.gravity.interests.jobs.intelligence.operations

import com.gravity.domain.FieldConverters._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.eventlogging.FieldValueRegistry
import com.gravity.utilities.grvfields._
import org.joda.time.DateTime

import scala.collection.Seq
import scalaz.syntax.validation._
import scalaz.{NonEmptyList, Validation}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/16/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object MetricsEvent {
  //import LegacyEventConverters._
  def getMetricsEventFromString(line: String): Validation[NonEmptyList[FailureResult], MetricsEvent with DiscardableEvent with Product with Serializable] = {
    if(line.startsWith("ClickEvent"))
      getInstanceFromString[ClickEvent](line)
    else if(line.startsWith("ImpressionEvent"))
      getInstanceFromString[ImpressionEvent](line)
    else if(line.startsWith("ImpressionViewedEvent"))
      getInstanceFromString[ImpressionViewedEvent](line)
//    else if(line.startsWith("RecoEvent"))
//      getInstanceFromString[RecoServedEvent2](line)
//    else if(line.startsWith("SponsoredStory"))
//      getInstanceFromString[SponsoredStoryServedEvent](line)
    else
      FailureResult("Not a metrics event").failureNel
  }

  def getMetricsEventRegistryFromString(line: String): Validation[NonEmptyList[FailureResult], FieldValueRegistry] = {
    if(line.startsWith("ClickEvent"))
      getInstanceFromString[ClickEvent](line).map(_.toValueRegistry())
    else if(line.startsWith("ImpressionEvent"))
      getInstanceFromString[ImpressionEvent](line).map(_.toValueRegistry())
    else if(line.startsWith("ImpressionViewedEvent"))
      getInstanceFromString[ImpressionViewedEvent](line).map(_.toValueRegistry())
//    else if(line.startsWith("RecoEvent"))
//      getInstanceFromString[RecoServedEvent2](line).map(_.toValueRegistry())
//    else if(line.startsWith("SponsoredStory"))
//      getInstanceFromString[SponsoredStoryServedEvent](line).map(_.toValueRegistry())
    else if(line.startsWith("AuxiliaryClickEvent"))
      getInstanceFromString[AuxiliaryClickEvent](line).map(_.toValueRegistry())
    else
      FailureResult("Not a metrics event").failureNel
  }

}

/**
* Introduced to bring ImpressionViewedEvent (not-a GrccableEvent) into the metrics event hierarchy.
* For now mostly a type marker.  In the future, it may become part of the mechanism to refactor legacy
* GrccableEvent.
*/
trait MetricsEvent {
  // metricable: a collection (e.g. articles, etc.) containing some metrics to tally
  def sizeOfMetricable: Integer
  def isMetricableEmpty: Boolean
  def metricableSiteGuid: String
  def metricableEventDate: DateTime
  def getSiteGuid: String
  def getArticleIds: Seq[Long]
}
