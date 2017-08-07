package com.gravity.interests.jobs.intelligence

import java.lang

import play.api.libs.json.{Format, Json}

import scala.collection.{mutable, Set, Map}
import com.gravity.utilities.time.DateHour
import com.gravity.utilities.analytics.{DateHourRange, TimeSliceResolution}
import org.joda.time.DateTime

case class MetricsWithVisitors(views: Long, publishes: Long, socialReferrers: Long, searchReferrers: Long, keyPageReferrers: Long, visitors: Long) {
  def +(that: MetricsWithVisitors): MetricsWithVisitors = MetricsWithVisitors(
    this.views + that.views,
    this.publishes + that.publishes,
    this.socialReferrers + that.socialReferrers,
    this.searchReferrers + that.socialReferrers,
    this.keyPageReferrers + that.keyPageReferrers,
    this.visitors + that.visitors
  )

  def -(that: MetricsWithVisitors): MetricsWithVisitors = MetricsWithVisitors(
    this.views - that.views,
    this.publishes - that.publishes,
    this.socialReferrers - that.socialReferrers,
    this.searchReferrers - that.socialReferrers,
    this.keyPageReferrers - that.keyPageReferrers,
    this.visitors - that.visitors
  )

  def /(value: Long): MetricsWithVisitors = MetricsWithVisitors(
    this.views / value,
    this.publishes / value,
    this.socialReferrers / value,
    this.searchReferrers / value,
    this.keyPageReferrers / value,
    this.visitors / value
  )

  def toNonNegative: MetricsWithVisitors = MetricsWithVisitors(
    if (views < 0) 0 else views,
    if (publishes < 0) 0 else publishes,
    if (socialReferrers < 0) 0 else socialReferrers,
    if (searchReferrers < 0) 0 else searchReferrers,
    if (keyPageReferrers < 0) 0 else keyPageReferrers,
    if (visitors < 0) 0 else visitors
  )
}


object MetricsWithVisitors {

  implicit val jsonFormat: Format[MetricsWithVisitors] = Json.format[MetricsWithVisitors]

  def buildFromStandardMetrics(metrics: StandardMetrics, visitors: Long): MetricsWithVisitors = MetricsWithVisitors(
    metrics.views,
    metrics.publishes,
    metrics.socialReferrers,
    metrics.searchReferrers,
    metrics.keyPageReferrers,
    visitors
  )

  val empty: MetricsWithVisitors = MetricsWithVisitors(0, 0, 0, 0, 0, 0)

  val zeroLong: lang.Long = new java.lang.Long(0l)

  def fromHourly(metricsMap: Map[DateHour, StandardMetrics], visitorsMap: Map[DateHour, Long]): Map[DateHour, MetricsWithVisitors] = {
    val ch = DateHour.currentHour
    val (stdMinHour, stdMaxHour) = if (metricsMap.isEmpty) {
      (ch, ch)
    } else {
      (metricsMap.minBy((dm: (DateHour, StandardMetrics)) => dm._1.getMillis)._1, metricsMap.maxBy((dm: (DateHour, StandardMetrics)) => dm._1.getMillis)._1)
    }

    val (visMinHour, visMaxHour) = if (visitorsMap.isEmpty) {
      (ch, ch)
    } else {
      (visitorsMap.minBy((dm: (DateHour, Long)) => dm._1.getMillis)._1, visitorsMap.maxBy((dm: (DateHour, Long)) => dm._1.getMillis)._1)
    }

    val maxHour = List(stdMaxHour, visMaxHour).maxBy(_.getMillis)

    val absoluteMinHour = DateHour(TimeSliceResolution.lastSevenDays.hourRange.fromInclusive)

    val minHour = List(stdMinHour, visMinHour).minBy(_.getMillis) match {
      case tooOld if (tooOld.isBefore(absoluteMinHour)) => absoluteMinHour
      case ok => ok
    }

    (for {
      dh <- DateHour.hoursBetween(minHour, maxHour)
      m = metricsMap.getOrElse(dh, StandardMetrics.empty)
      v = visitorsMap.getOrElse(dh, 0l)
    } yield dh -> MetricsWithVisitors.buildFromStandardMetrics(m, v)).toMap
  }

  def fromHourlyWithin(siteGuid: String, metricsMap: Map[DateHour, StandardMetrics], visitorsMap: Map[DateHour, Long], hours: Set[DateHour]): Map[DateHour, MetricsWithVisitors] = {

    val resultMap = mutable.HashMap[DateHour, MetricsWithVisitors]()
    resultMap.sizeHint(hours.size)

    for {
      k <- hours
      m = metricsMap.getOrElse(k, StandardMetrics.empty)
      v = visitorsMap.getOrElse(k, 0l)
    } {
      resultMap += k -> MetricsWithVisitors(m.views, m.publishes, m.socialReferrers, m.searchReferrers, m.keyPageReferrers, v)
    }

    resultMap
  }

  def fromHourlyWithin(siteGuid: String, metricsMap: Map[DateHour, StandardMetrics], visitorsMap: Map[DateHour, Long], period: TimeSliceResolution): Map[DateHour, MetricsWithVisitors] = {
    val keys = if (period != TimeSliceResolution.today) {
      period.hourRange.hoursWithin
    } else {
      val fromHour = DateHour(period.hourRange.fromInclusive)
      DateHourRange(fromHour, new DateTime()).hoursWithin
    }

    fromHourlyWithin(siteGuid, metricsMap, visitorsMap, keys)
  }

  def fromHourlyWithin(siteGuid: String, metricsMap: Map[DateHour, StandardMetrics], visitorsMap: Map[DateHour, Long], hourRange: DateHourRange): Map[DateHour, MetricsWithVisitors] = {
    fromHourlyWithin(siteGuid, metricsMap, visitorsMap, hourRange.hoursWithin)
  }
}
