package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.domain.grvmetrics
import com.gravity.domain.grvmetrics.MetricsWithTime
import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters._
import com.gravity.interests.jobs.intelligence.hbase.ScopedMetricsConverters._
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.interests.jobs.intelligence.operations.sites.SiteGuidService
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvtime._
import com.gravity.utilities.grvz._
import com.gravity.utilities.time.{DateHour, DateMinute, GrvDateMidnight}
import com.gravity.utilities.{GrvConcurrentMap, grvcoll, grvtime}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection._
import scalaz.Scalaz._
import scalaz._

object ScopedToMetricsService extends TableOperations[ScopedToMetricsTable,ScopedKey, ScopedToMetricsRow] {
  val table = Schema.ScopedToMetrics
}

object ScopedMetricsService extends operations.TableOperations[ScopedMetricsTable, ScopedFromToKey, ScopedMetricsRow] {
  val table = Schema.ScopedMetrics

  def fetchRelatedMetrics(fromKeys:Iterable[ScopedKey], toKeys:Iterable[ScopedKey])(query: ScopedMetricsService.QuerySpec): ValidationNel[FailureResult, Map[ScopedKey, Map[ScopedFromToKey, ScopedMetricsRow]]] = {
    val fromToKeys = fromKeys.flatMap(fromKey => toKeys.map(toKey => ScopedFromToKey(fromKey, toKey))).toSet

    for {
      metrics <- fetchMulti(fromToKeys, skipCache = false, returnEmptyResults = true)(query)
    } yield {
       metrics.groupBy(_._1.from)
    }
  }

  def fromImpressionEventSectionPath(event:ImpressionEvent) : Seq[ScopedFromToKey] = {
    event.currentSectionPath.sectionKeys(event.getSiteGuid).map(EverythingKey.fromEverythingScopedTo).toSeq
  }

  def fromClickEventSectionPath(event:ClickEvent) : Seq[ScopedFromToKey] = {
    event.currentSectionPath.sectionKeys(event.getSiteGuid).map(EverythingKey.fromEverythingScopedTo).toSeq
  }

  def fromImpressionEventArticlesToSection(event:ImpressionEvent): ValidationNel[FailureResult, Seq[ScopedFromToKey]] = {
    val keysFromURL = for {
      sectionKey <- SiteGuidService.extractSectionKeyFromUrl(event.getCurrentUrl, event.getSiteGuid)
      scopeFromToKeys = event.getArticleIds.map(ArticleKey(_)).map(ak => ScopedFromToKey(ak.toScopedKey, sectionKey.toScopedKey))
    } yield scopeFromToKeys.toSet.toList

    val keysFromSectionPath = event.currentSectionPath.sectionKeys(event.getSiteGuid).flatMap(sectionKey => {
      val scopeFromToKeys = event.getArticleIds.map(ArticleKey(_)).map(ak => ScopedFromToKey(ak.toScopedKey, sectionKey.toScopedKey))
      scopeFromToKeys
    }).toList.successNel[FailureResult]



    val result = keysFromURL >>*<< keysFromSectionPath

    if(event.siteGuid == ArticleWhitelist.siteGuid(_.CONDUIT)) {
      val keySize = keysFromSectionPath.map(keys=> keys.size) | 0
      val resultSize = result.map(_.size) | 0
      com.gravity.utilities.Counters.countPerSecond(counterCategory, "Conduit Received " + keySize + " Article Impressions via Section Path: " + event.currentSectionPath + " and result of size " + resultSize)
    }

    result
  }


  def fromClickEventArticleToSection(event:ClickEvent): ValidationNel[FailureResult, Seq[ScopedFromToKey]] = {
    val referrer = event.getCurrentUrl
    val urlDerivedEvent = for {
      sectionKey <- SiteGuidService.extractSectionKeyFromUrl(referrer, event.getSiteGuid)
      scopeFromToKey = ScopedFromToKey(event.article.key.toScopedKey, sectionKey.toScopedKey)
    } yield List(scopeFromToKey)

    val sectionPathDerivedEvents = event.currentSectionPath.sectionKeys(event.getSiteGuid).toList.map(sectionKey=>{
      ScopedFromToKey(event.article.key.toScopedKey, sectionKey.toScopedKey)
    }).successNel[FailureResult]

    val result = urlDerivedEvent >>*<< sectionPathDerivedEvents
    result
  }
}







/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


object ScopedMetricsTtl {

  val minutelyTtlInSeconds = grvtime.secondsFromHours(12)
  val hourlyTtlInSeconds = grvtime.secondsFromDays(10)
  val dailyTtlInSeconds = grvtime.secondsFromDays(32)
  val weeklyTtlInSeconds = grvtime.secondsFromWeeks(53)
  val monthlyTtlInSeconds = grvtime.secondsFromYears(2)

  def getMillisThreshold(ttl:Int) = new DateTime().minusSeconds(ttl).getMillis


  def getMillisThreshold(bucket: ScopedMetricsBucket.Value): Long = {

    bucket match {
      case ScopedMetricsBucket.minutely => getMillisThreshold(ScopedMetricsTtl.minutelyTtlInSeconds)
      case ScopedMetricsBucket.hourly => getMillisThreshold(ScopedMetricsTtl.hourlyTtlInSeconds)
      case ScopedMetricsBucket.daily => getMillisThreshold(ScopedMetricsTtl.dailyTtlInSeconds)
      //case ScopedMetricsBucket.weekly => getMillisThreshold(ScopedMetricsTtl.weeklyTtlInSeconds)
      case ScopedMetricsBucket.monthly => getMillisThreshold(ScopedMetricsTtl.monthlyTtlInSeconds)
    }
  }

  def isPersistable(metricsKey: ScopedMetricsKey, bucket: ScopedMetricsBucket.Value) = {

    if(metricsKey.dateTimeMs > getMillisThreshold(bucket)) {

      true

    } else {

      false
    }
  }
}




trait ScopedToMetricsColumns[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  val scopedToMetrics = family[ScopedToMetricsKey, Long]("sctm", rowTtlInSeconds = 604800, compressed = true)
}

trait ScopedToMetricsColumnsRow[T <: HbaseTable[T, R, RR] with ScopedToMetricsColumns[T, R], R, RR <: HRow[T, R]] {
  this: HRow[T, R] =>
}

trait ScopedMetricsColumns[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  
  val scopedMetricsByMinute = family[ScopedMetricsKey, Long]("scn", rowTtlInSeconds = ScopedMetricsTtl.minutelyTtlInSeconds, compressed = true)
  val scopedMetricsByHour = family[ScopedMetricsKey, Long]("scm", rowTtlInSeconds = ScopedMetricsTtl.hourlyTtlInSeconds, compressed = true)
  val scopedMetricsByDay = family[ScopedMetricsKey, Long]("scd", rowTtlInSeconds = ScopedMetricsTtl.dailyTtlInSeconds, compressed = true)
  val scopedMetricsByWeek = family[ScopedMetricsKey, Long]("scw", rowTtlInSeconds = ScopedMetricsTtl.weeklyTtlInSeconds, compressed = true)
  val scopedMetricsByMonth = family[ScopedMetricsKey, Long]("scmo", rowTtlInSeconds = ScopedMetricsTtl.monthlyTtlInSeconds, compressed = true)
}

trait ScopedMetricsColumnsRow[T <: HbaseTable[T, R, RR] with ScopedMetricsColumns[T, R], R, RR <: HRow[T, R]] {
  this: HRow[T, R] =>

  val scopedMetricsByHour = family(_.scopedMetricsByHour)
  val totalArticleImpressions = scopedMetricsByHour.filterKeys(_.countBy == RecommendationMetricCountBy.unitImpression).values.sum
  val totalClicks = scopedMetricsByHour.filterKeys(_.countBy == RecommendationMetricCountBy.click).values.sum
  val totalCTR = grvmetrics.safeCTR(totalClicks, totalArticleImpressions)

  val scopedMetricsByMinute = family(_.scopedMetricsByMinute)
  val scopedMetricsByDay = family(_.scopedMetricsByDay)
  val scopedMetricsByWeek = family(_.scopedMetricsByWeek)
  val scopedMetricsByMonth = family(_.scopedMetricsByMonth)

//  private val scopedMetricsByMemo = new KeyedMemos[(ScopedMetricsKey)=>Boolean, RecommendationMetrics]
  def scopedMetricsBy(filter:(ScopedMetricsKey) => Boolean) = {
    scopedMetricsByHour.aggregate(RecommendationMetrics.empty)((total, kv)=>{
      if(filter(kv._1))
        total + kv._1.recoMetrics(kv._2)
      else
        total
    }, _+_)
  }
}


class KeyedMemos[K,V: Manifest] {
  type MemoKey = K

  private lazy val filteredMemo = new GrvConcurrentMap[MemoKey, V]()
  def memoized(key:MemoKey)(work:V) = filteredMemo.getOrElseUpdate(key,work)
}



class ScopedToMetricsTable extends HbaseTable[ScopedToMetricsTable, ScopedKey, ScopedToMetricsRow](tableName = "scoped_to_metrics", rowKeyClass = classOf[ScopedKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with ScopedToMetricsColumns[ScopedToMetricsTable,ScopedKey]
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new ScopedToMetricsRow(result, this)

  val meta = family[String, Any]("meta", compressed = true)
 val name = column(meta, "nm",classOf[String])
}


class ScopedToMetricsRow(result: DeserializedResult, table: ScopedToMetricsTable) extends HRow[ScopedToMetricsTable, ScopedKey](result, table) with ScopedToMetricsColumnsRow[ScopedToMetricsTable, ScopedKey, ScopedToMetricsRow]

class ScopedMetricsTable extends HbaseTable[ScopedMetricsTable, ScopedFromToKey, ScopedMetricsRow](tableName = "scoped_metrics", rowKeyClass = classOf[ScopedFromToKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with ScopedMetricsColumns[ScopedMetricsTable,ScopedFromToKey]
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new ScopedMetricsRow(result, this)

  val meta = family[String, Any]("meta", compressed = true)
  val name = column(meta, "nm",classOf[String])
}


class ScopedMetricsRow(result: DeserializedResult, table: ScopedMetricsTable) extends HRow[ScopedMetricsTable, ScopedFromToKey](result, table) with ScopedMetricsColumnsRow[ScopedMetricsTable, ScopedFromToKey, ScopedMetricsRow] {

  def getMetricsByLastX(by: Byte, toCount: Int, placementId: Option[Int] = None, maxHours: Int = 1, fromHour: DateHour = grvtime.currentHour): RecommendationMetrics = {

    val maxHour = fromHour
    val minHour = fromHour.minusHours(maxHours)

    // pre-filter metrics by date and placement (to minimize the sort candidates later)
    val filteredMetrics = scopedMetricsByHour.filter{ case (key, value) => {
        key.dateHour.isBetween(minHour, maxHour)
      }
    }

    // used to "break" out of the count once we hit our target count (ie: 1000 impressions)
    var drop = (key: ScopedMetricsKey, value: Long) => false

    // use to track our target count (ie: 1000 impressions)
    var count = 0L

    // filter the metrics while tracking the current count for the stat we are interested in...
    // (impressions, clicks, etc).  When we hit the threshold, we will ignore the remaining stats
    filteredMetrics.toList.sortBy(-_._1.dateHour.getMillis)
      .foldLeft(RecommendationMetrics.empty){ case (acc, (key, value)) =>
        if (!drop(key, value)) {
          if (key.countBy == by) {
            count += value
            if (count >= toCount) {
              // this is the signal to ignore remaining stats regarding this (stat -> count)
              drop = (k: ScopedMetricsKey, v: Long) => k.dateHour.isBefore(key.dateHour)
            }
          }

          acc |+| key.recoMetrics(value)
        } else {
          acc
        }
      }

  }

}

class ScopedMetricMap( val map: Map[ScopedMetricsKey, Long]) extends MetricsWithTime[(ScopedMetricsKey, Long), ScopedMetrics, Long](map) {
  def +(that: ScopedMetricMap) =  {
    val allVals = this.map.toSeq ++ that.map.toSeq

    new ScopedMetricMap(
      grvcoll.groupAndFold(0L)(this.map, that.map)(_._1) {
        case (lhsLong, (_, rhsLong)) =>
          lhsLong + rhsLong
      }
    )
  }
}

object ScopedMetricMap {
  val empty = new ScopedMetricMap(Map())
}

case class ScopedMetricsBuckets(var minutely: ScopedMetricMap = ScopedMetricMap.empty,
                                var hourly: ScopedMetricMap = ScopedMetricMap.empty,
                                var daily: ScopedMetricMap = ScopedMetricMap.empty,
                                var monthly: ScopedMetricMap = ScopedMetricMap.empty) {
  def +(that: ScopedMetricsBuckets) = {
    ScopedMetricsBuckets(
      this.minutely + that.minutely,
      this.hourly   + that.hourly,
      this.daily    + that.daily,
      this.monthly  + that.monthly
    )
  }
}

object ScopedMetricsBuckets {
  val empty = new ScopedMetricsBuckets()
}

/**
 * This class will maintain all of the date related key data required for querying/aggregating ScopedMetrics
 * across the 4 families (minute, hour, day, month).
 * This initial implementation will be built to support querying metrics up-to-the current moment in time.
 */
case class ScopedMetricsDateKeySplits(
       minuteMin: DateMinute, minuteMax: DateMinute, hourMin: DateHour, hourMax: DateHour,
       dayMin: DateHour, dayMax: DateHour, monthMin: DateHour, monthMax: DateHour, upToPointInTime: DateTime) {

  lazy val scopedMetricsKeyForMinutely: ScopedMetricsKey = ScopedMetricsKey.partialByStartDate(minuteMin.minusMinutes(1))
  lazy val scopedMetricsKeyForHourly: ScopedMetricsKey = ScopedMetricsKey.partialByStartDate(hourMin.minusHours(1))
  lazy val scopedMetricsKeyForDaily: ScopedMetricsKey = ScopedMetricsKey.partialByStartDate(dayMin.minusHours(1))
  lazy val scopedMetricsKeyForMonthly: ScopedMetricsKey = ScopedMetricsKey.partialByStartDate(monthMin.minusHours(1))

  def filterMinutely(row: ScopedMetricsRow): Map[ScopedMetricsKey, Long] = row.scopedMetricsByMinute.filterKeys {
    case key: ScopedMetricsKey => key.dateMinute.isBetween(minuteMin, minuteMax)
  }

  def filterHourly(row: ScopedMetricsRow): Map[ScopedMetricsKey, Long] = row.scopedMetricsByHour.filterKeys {
    case key: ScopedMetricsKey => key.dateHour.isBetween(hourMin, hourMax)
  }

  def filterDaily(row: ScopedMetricsRow): Map[ScopedMetricsKey, Long] = row.scopedMetricsByDay.filterKeys {
    case key: ScopedMetricsKey => key.dateHour.isBetween(dayMin, dayMax)
  }

  def filterMonthly(row: ScopedMetricsRow): Map[ScopedMetricsKey, Long] = row.scopedMetricsByMonth.filterKeys {
    case key: ScopedMetricsKey => key.dateHour.isBetween(monthMin, monthMax)
  }

  def spliceMetrics(row: ScopedMetricsRow): Map[ScopedMetricsKey, Long] = {
    filterMinutely(row) ++ filterHourly(row) ++ filterDaily(row) ++ filterMonthly(row)
  }

  def mapKeyValuesToMetrics[K](rowMap: Map[ScopedFromToKey, ScopedMetricsRow])(keyMapper: ScopedFromToKey => K): Map[K, Map[ScopedMetricsKey, Long]] = {
    rowMap.map {
      case (key, row) => keyMapper(key) -> spliceMetrics(row)
    }
  }

  override def toString: String = {
    val f = ScopedMetricsDateKeySplits.timeFormat
    s"Up-to: '${upToPointInTime.toString(f)}' :: Minutely('${minuteMin.toString(f)}' -> '${minuteMax.toString(f)}') :: Hourly('${hourMin.toString(f)}' -> '${hourMax.toString(f)}') :: Daily('${dayMin.toString(f)}' -> '${dayMax.toString(f)}') :: Monthly('${monthMin.toString(f)}' -> '${monthMax.toString(f)}')"
  }
}

object ScopedMetricsDateKeySplits {
  val timeFormat = DateTimeFormat.forPattern("yyyy-MM-dd 'at' hh:mm:ss a")

  def forNow: ScopedMetricsDateKeySplits = upTo(grvtime.currentTime)

  def upTo(when: DateTime): ScopedMetricsDateKeySplits = {
    val startOfCurrentHour = when.toDateHour

    val minuteMax = when.toDateMinute

    val midnightToday = new GrvDateMidnight(when.getMillis).toDateHour

    val hourMax = startOfCurrentHour.minusHours(1)
    val hourMin = midnightToday

    val minuteMin = hourMax.toDateMinute

    val startOfCurrentMonth = when.toDateMonth

    val dayMax = midnightToday.minusHours(1)
    val dayMin = startOfCurrentMonth.toDateHour

    val monthMax = dayMin.minusHours(1)
    val monthMin = startOfCurrentMonth.minusYears(2).toDateHour

    ScopedMetricsDateKeySplits(minuteMin, minuteMax, hourMin, hourMax, dayMin, dayMax, monthMin, monthMax, when)
  }
}

object PrintMeSomeSplitsYo extends App {
//  val when = new DateTime(2015, 5, 5, 13, 42, 48)
  println(ScopedMetricsDateKeySplits.forNow)
}
