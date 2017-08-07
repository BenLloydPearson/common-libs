package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.utilities.grvcoll
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}

import scala.collection._

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */



trait StandardMetricsColumns[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  // TJC: Note that we have to change the Schema to actually change the TTL for standardMetricsHourlyOld from 691200 to 2678400!
  val standardMetricsHourlyOld : this.Fam[DateHour,StandardMetrics] = family[DateHour, StandardMetrics]("smh", rowTtlInSeconds = 2678400,compressed=true)

  val viralMetrics : this.Fam[GrvDateMidnight, ViralMetrics] = family[GrvDateMidnight, ViralMetrics]("vm",compressed=true, rowTtlInSeconds=2678400)

  // TJC: Note that we have to change the Schema to actually change the TTL for viralMetricsHourly from 691200 to 2678400!
  val viralMetricsHourly : this.Fam[DateHour, ViralMetrics] = family[DateHour, ViralMetrics]("vmh", rowTtlInSeconds = 2678400,compressed=true)
}

trait HasStandardMetrics {
  import com.gravity.utilities.Counters._
  // everything is derived off of these maps
  // val=>def per Mr. B. recommendation
  def standardMetricsHourlyOld : Map[DateHour, StandardMetrics]
  def viralMetrics : Map[GrvDateMidnight, ViralMetrics]

  lazy val standardMetricsOld: Map[GrvDateMidnight, StandardMetrics] = dailyStandardMetricsFromHourly

  // For INTERESTS-4421, eliminate reading and writing of daily standard metrics in favor of hourly standard metrics.
  private def dailyStandardMetricsFromHourly: Map[GrvDateMidnight, StandardMetrics] = {
    grvcoll.groupAndFold(StandardMetrics.empty)(standardMetricsHourlyOld.toSeq)(_._1.toGrvDateMidnight)((total, kv) => total + kv._2)
  }

  lazy val standardMetricsOldForDisplay: scala.Seq[DateMetrics] = standardMetricsOld.toSeq.sortBy(-_._1.getMillis).map {
    tup => DateMetrics(tup._1, tup._2)
  }

  lazy val aggregateMetricsOld: StandardMetrics = if (standardMetricsOld.size > 0) standardMetricsOld.values.reduceLeft(_ + _) else StandardMetrics.empty

  lazy val aggregateViralMetrics: ViralMetrics = if (viralMetrics.size > 0) viralMetrics.values.reduceLeft(_ + _) else ViralMetrics.empty

  def aggregateMetricsOldBetween(fromInclusive: GrvDateMidnight, toInclusive: GrvDateMidnight): StandardMetrics = {
    countPerSecond("Standard Metrics", "aggregateMetricsOldBetween called")
    AggregableMetrics.aggregateBetween(standardMetricsOld, fromInclusive, toInclusive)
  }

  def aggregateMetricsOldFor(days: Set[GrvDateMidnight]): StandardMetrics = if (standardMetricsOld.isEmpty || days.isEmpty) StandardMetrics.empty
  else days.collect(standardMetricsOld) match {
    case mt if (mt.isEmpty) => StandardMetrics.empty
    case mets => mets.reduceLeft(_ + _)
  }

  def aggregateViralMetricsBetween(fromInclusive: GrvDateMidnight, toInclusive: GrvDateMidnight): ViralMetrics = AggregableMetrics.aggregateBetween(viralMetrics, fromInclusive, toInclusive)

  lazy val dailyMetricsOldToday: StandardMetrics = {
    standardMetricsOld.getOrElse(new GrvDateMidnight, StandardMetrics.empty)
  }

  lazy val hourlyMetricsOldThisHour: StandardMetrics = {
    val hourly = standardMetricsHourlyOld
    val thisHour = DateHour.currentHour
    hourly.getOrElse(thisHour, StandardMetrics.empty)
  }

  def hourlyMetricsOldSorted(uniquesHourly: Map[DateHour, Long] = Map.empty[DateHour, Long]): scala.Seq[(DateHour, MetricsWithVisitors)] = hourlyMetricsOld(uniquesHourly).toSeq.sortBy(- _._1.getMillis)

  def hourlyMetricsOld(uniquesHourly: Map[DateHour, Long] = Map.empty[DateHour, Long]): Map[DateHour, MetricsWithVisitors] = MetricsWithVisitors.fromHourly(standardMetricsHourlyOld, uniquesHourly)

}

trait StandardMetricsRow[T <: HbaseTable[T, R, RR] with StandardMetricsColumns[T, R], R, RR <: HRow[T, R]] extends HasStandardMetrics {
  this: HRow[T, R] =>

  /**
   *  The former family(_.standardMetricsOld), now built dynamically from standardMetricsHourlyOld.
   *  Note that this means that for these numbers to have a whole day's worth of data, you must have queried all the hours in the day.
   */

  lazy val standardMetricsHourlyOld: Map[DateHour, StandardMetrics] = family(_.standardMetricsHourlyOld)

  lazy val viralMetrics: Map[GrvDateMidnight, ViralMetrics] = family(_.viralMetrics)

  lazy val viralMetricsHourly: Map[DateHour, ViralMetrics] = family(_.viralMetricsHourly)

}
