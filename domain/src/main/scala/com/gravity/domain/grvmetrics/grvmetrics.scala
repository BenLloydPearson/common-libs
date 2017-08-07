package com.gravity.domain

import com.gravity.domain.recommendations.{RecoReportMetrics, _}
import com.gravity.interests.jobs.intelligence.hbase.ScopedMetricsKey
import com.gravity.interests.jobs.intelligence.{RollupRecommendationMetricsNoSiteGroupByDayKey, RollupRecommendationMetricsNoSiteGroupByHourKey, _}
import com.gravity.utilities.grvtime._
import com.gravity.utilities.time.DateHour.asReadableInstant
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}
import com.gravity.utilities.{grvcoll, grvmath, grvtime}
import org.apache.commons.math.analysis.interpolation.LoessInterpolator
import org.joda.time._
import play.api.libs.json._

import scala.collection._
import scala.math.Ordering.LongOrdering
import scalaz.Monoid
import scalaz.std.iterable._
import scalaz.syntax.foldable._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * A package that deals with the typical representations of Gravity Metrics in a standardized fashion, with a thorough separation of concerns between
 * time scopes, metric types, etc.  Hopefully through the process of doing this key algorithms that are repeated regularly will be centralized.
 *
 * Look to GrvMetricsTest for usage instructions.
 *
 * Using this package should amount to importing grvmetrics._
 */
package object grvmetrics {

  /**
   * Calculates Click Through Rate (CTR) for the specified `clicks` and `impressions`
   * @param clicks the number of clicks to be considered
   * @param impressions the number of impressions to be considered
   * @return The CTR or ZERO (0) if either `clicks` or `impressions` equal ZERO (0)
   */
  def safeCTR(clicks: Long, impressions: Long): Double = if (impressions == 0 || clicks == 0) 0D else clicks.toDouble / impressions

  /**
   * Can work on Metrics. Implements Monoid because assumedly a Metric can have an empty value and multiple Metrics can be added together.
   * @tparam M The type of the Metric
   */
  trait Metric[M] extends Monoid[M]

  /**
   * Knows how to do sufficient time series sensitive operations to work with metrics
   * @tparam T The class of Time
   */
  trait TimeWrangler[T] extends scala.math.Ordering[T]{
    def now: T

    def percentageComplete: Double

    def between(start: T, end: T): Iterable[T]


    /**
     * A convenience method for expressing, "3 time units ago".  0 should mean the current time unit.  A negative number should mean future time units.
     * @param unit
     * @return
     */
    def unitsBeforeNow(unit: Int): T

    def unitsAfterNow(unit: Int): T = unitsBeforeNow(-unit)

    def numeric(time: T): Long


    //Convert to datetime to allow generic translation operations
    def readableDateTime(time: T): ReadableDateTime

    //Convert from readable back to the representation
    def readableDateTimeToRepr(dateTime: ReadableDateTime): T

    def month(time: T): T = readableDateTimeToRepr(grvtime.dateToMonth(readableDateTime(time)))

    //    def next(intervals:Int) : T

    def day(time: T): T = readableDateTimeToRepr(readableDateTime(time).toDateTime.toGrvDateMidnight)

    def hour(time: T): T = readableDateTimeToRepr(DateHour(readableDateTime(time)))
  }

  /**
   * Something that contains a time value.
   * @tparam M The item that will be transformed from.
   * @tparam T The time that will be extracted
   */
  trait TimeContainer[M, T] {
    def time(value: M): T

  }


  /**
   * Represents something that can be transformed into a Metric representation.
   * @tparam O The original item.
   * @tparam M The target metric.
   */
  trait CanBeMetric[O, M] {
    def toMetric(original: O): M

    def timeSeriesEntry[T](original: O)(implicit t: TimeContainer[O, T]): (T, M) = (t.time(original), toMetric(original))
  }

  /**
   * Allows grouping of data before aggregation.  Usually this precedes some kind of sort, so the Grouper must be Ordered
   * @tparam O The original data
   * @tparam G The group key
   */
  trait CanBeGrouper[O, G] extends scala.math.Ordering[G] {
    def fromData(data: O): G
  }

  trait CanBeSitePlacementKey[A] {
    def sitePlacementKey(data: A): SitePlacementKey
  }

  /**
   * Convenience method for dealing with key-value pairs that can be transformed into metrics.
   * @tparam K The key
   * @tparam V The value
   * @tparam M The metric
   */
  trait KeyValueMetric[K, V, M] extends CanBeMetric[(K, V), M] {
    def toMetric(key: K, value: V): M

    def toMetric(keyValue: (K, V)): M = toMetric(keyValue._1, keyValue._2)

    def timeSeriesEntry[T](key: K, value: V)(implicit t: TimeContainer[K, T]): (T, M) = (t.time(key), toMetric(key, value))

  }

  class MetricTimeResult[M: Metric, T: TimeWrangler](metrics: Seq[(M, T)]) {

    def sortByTime: scala.Seq[(M, T)] = metrics.sortBy(_._2)

    def reverseSortByTime: scala.Seq[(M, T)] = metrics.sortBy(_._2).reverse
  }

  class Metrics[A, M](data: Iterable[A])(implicit m: Metric[M], ag: CanBeMetric[A, M]) {
    def aggregateBy(filter: (A) => Boolean): M = {
      aggregate(data.filter(filter).map(ag.toMetric))
    }

    def groupReduce[G](implicit cg: CanBeGrouper[A, G]): Map[G, M] = {
      groupMapgregate((data) => cg.fromData(data))
    }

    def groupReduceSeq[G](implicit cg: CanBeGrouper[A, G]): scala.Seq[(G, M)] = {
      groupMapgregate((data) => cg.fromData(data)).toSeq.sortBy(_._1)
    }


    def group[G](implicit cg: CanBeGrouper[A,G]): Predef.Map[G, scala.Iterable[A]] = {
      data.groupBy(cg.fromData)
    }

    protected def aggregate(data: Iterable[M]): M = data.concatenate

    private var aggregateAllRes: M = _

    def aggregateAll: M = {
      if (aggregateAllRes == null) aggregateAllRes = aggregate(data.map(ag.toMetric))
      aggregateAllRes
    }

    def groupReduceSeqBy[G](grouper: (A) => G): Seq[(G, M)] = groupMapgregate(grouper).toSeq

    def groupMapgregate[G](grouper: (A) => G): scala.collection.Map[G, M] = {
      grvcoll.groupAndFold(m.zero)(data.toSeq)(grouper)((total, kv) => m.append(total, ag.toMetric(kv)))
    }
  }

  class RealizedMetricalTimeSeries[M,T](val data: Iterable[(T,M)])(
        implicit m : Metric[M],
        tw: TimeWrangler[T]
    ) {

  }

  /**
   * This is where you add methods that work with metrics and time.
   */
  class MetricsWithTime[A, M, T](val data: Iterable[A])
                                (
                                  implicit m: Metric[M], //Prove that M is a Metric
                                  ag: CanBeMetric[A, M], //Prove that A can be transformed to M
                                  t: TimeContainer[A, T], //Provide that A can have a Time element extracted from it
                                  tw: TimeWrangler[T]) //Prove that we can work with the Time elmeent
    extends Metrics[A, M](data) {

    def timeSeries: Seq[(T, M)] = {
      timeSeriesMap.toSeq.sortBy(_._1) //Ironically given that the original representation here is a map, it is more efficient to map first and sequence later
    }

    /**
     * Group the metrics by a function, then for each result convert to a time series.
     */
    def groupedTimeSeriesBy[G](grouper: (A) => G): immutable.Map[G, immutable.Map[T, M]] = {
      data.groupBy(grouper).map((groupAndSeries) => {

        val group = groupAndSeries._1
        val series = groupAndSeries._2
        group -> new MetricsWithTime[A,M,T](series).timeSeriesMap
      })
    }

    def groupedTimeSeriesByJson[G](grouper: (A) => G)(x: ((T,M)) => Long)(y: ((T,M)) => Double) : Seq[Series] = {
      val reslt = groupedTimeSeriesBy(grouper).map{kv=>
        Series(kv._1.toString, None, {
          kv._2.map{timeseries=>
            SeriesItem(x(timeseries), y(timeseries), None)
          }.toSeq.sortBy(_.x)
        })
      }.toSeq
      reslt
    }

    def groupedTimeSeriesByJsonForHighcharts[G](grouper: (A) => G)(x: ((T,M)) => Long)(y: ((T,M)) => Double) : Seq[Highchart.Series[_]] = {
      val reslt = groupedTimeSeriesBy(grouper).map{kv=>
        Highchart.Series(kv._1.toString, {
          kv._2.flatMap{timeseries=>
            val xts = x(timeseries)
            val yts = y(timeseries)
            Some(mutable.Buffer(xts, yts))
          }.toSeq.sortBy(_(0).asInstanceOf[Long]).toArray
        })
      }.toSeq
      reslt
    }

    def groupedTimeSeriesWeightedByJsonForHighcharts[G](grouper: (A) => G)
                                                       (x: ((T,M)) => Long)
                                                       (y: ((T,M)) => Double)
                                                       (weightFx: ((T,M)) => Double) : Seq[Highchart.Series[_]] = {
      val reslt = groupedTimeSeriesBy(grouper).map{kv=>
        Highchart.Series(kv._1.toString, {
          kv._2.flatMap{timeseries=>
            val xts = x(timeseries)
            val yts = y(timeseries)
            Some(mutable.Buffer(xts, yts, weightFx(timeseries)))
          }.toSeq.sortBy(_(0).asInstanceOf[Long]).toArray
        })
      }.toSeq
      reslt
    }


    def groupedTimeSeriesWeightedByJson[G](grouper: (A) => G)
                                          (x: ((T,M)) => Long)
                                          (y: ((T,M)) => Double)
                                          (weightFx: ((T,M)) => Double) : Seq[Series] = {
      val reslt = groupedTimeSeriesBy(grouper).map{kv=>
        Series(kv._1.toString, None, {
          kv._2.map{timeseries=>
            val size = weightFx(timeseries)
            SeriesItem(x(timeseries), y(timeseries), Some(size))
          }.toSeq.sortBy(_.x)
        })
      }.toSeq
      reslt
    }

    def groupedTimeSeriesWeightedByJsonWithFilter[G](filter:((T,M)) => Boolean)(grouper: (A) => G)(x: ((T,M)) => Long)(y: ((T,M)) => Double)(weightFx: ((T,M)) => Double) : Seq[Series] = {
      val reslt = groupedTimeSeriesBy(grouper).map{kv=>
        Series(kv._1.toString, None, {
          kv._2.filter(filter).map{timeseries=>
            val size = weightFx(timeseries)
            SeriesItem(x(timeseries), y(timeseries), Some(size))
          }.toSeq.sortBy(_.x)
        })
      }.toSeq
      reslt
    }

    def groupedTimeSeriesWeightedByJsonWithFilterForHighcharts[G]
        (filter:((T,M)) => Boolean)
        (grouper: (A) => G)
        (x: ((T,M)) => Long)
        (y: ((T,M)) => Double)
        (weightFx: ((T,M)) => Double)
          : Seq[Highchart.Series[_]] = {
      val reslt = groupedTimeSeriesBy(grouper).map{kv=>
        Highchart.Series(kv._1.toString, {
          kv._2.filter(filter).flatMap{timeseries=>
            val xts = x(timeseries)
            val yts = y(timeseries)
            Some(mutable.Buffer(xts, yts, weightFx(timeseries)))
          }.toSeq.sortBy(_(0).asInstanceOf[Long]).toArray
        })
      }.toSeq.sortBy(_.name)
      reslt
    }



    /**
     * Group the metrics by a CanBeGrouper implementation, then for each result convert to a time series
     */
    def groupedTimeSeries[G](implicit g: CanBeGrouper[A,G]): immutable.Map[G, immutable.Map[T, M]] = {
      data.groupBy(g.fromData).map((groupAndSeries) => {

        val group = groupAndSeries._1
        val series = groupAndSeries._2
        group -> new MetricsWithTime[A,M,T](series).timeSeriesMap
      })
    }

    def timeSeriesReversed: Seq[(T, M)] = {
      timeSeriesMap.toSeq.sortBy(_._1).reverse
    }

    def forTime[NT](implicit t: TimeContainer[A, NT], tw: TimeWrangler[NT]): MetricsWithTime[A, M, NT] = new MetricsWithTime[A, M, NT](data)

    def localRegressionFx(start: Int, end: Int)(projector: (M) => Double): (T) => Double = {
      val loess = new LoessInterpolator()
      val orderedMetrics = timeSeriesRangedUnits(start, end).toSeq.sortBy(_._1)

      val independent = orderedMetrics.map(mt => tw.numeric(mt._1).toDouble).toArray
      val dependent = orderedMetrics.map(mt => projector(mt._2)).toArray
      val fx = loess.interpolate(independent, dependent)

      (timeUnit: T) => {
        fx.value(tw.numeric(timeUnit))
      }
    }

    def movingAverage[N: Numeric](start:Int, end:Int, period:Int)(projector: (M) => N) : Seq[Double] = {
      val orderedMetrics = timeSeriesRangedUnits(start,end).toSeq.sortBy(_._1)
      val dependent = orderedMetrics.map(mt => projector(mt._2))
      val movingAverage = grvmath.simpleMovingAverage(dependent, period)
      movingAverage
    }


    /**
     * Returns a function that estimates the metrics for a given date.  The time unit parameters are presented as "time units ago" where the time units are the units for your chosen time pivot.
     * @param start Time units ago - start
     * @param end Time units ago - end
     * @param projector
     * @tparam N
     * @return
     */
    def linearRegressionFx[N: Numeric](start: Int, end: Int)(projector: (M) => N): (T) => Double = {
      require(end >= 0, "Cannot use the future as the source of a projection")
      val orderedMetrics = timeSeriesRangedUnits(start, end).toSeq.sortBy(_._1)

      val independent = orderedMetrics.map(mt => tw.numeric(mt._1))
      val dependent = orderedMetrics.map(mt => projector(mt._2))

      val equation = grvmath.regressionEquation(independent, dependent)

      (timeUnit: T) => {
        equation(tw.numeric(timeUnit))
      }
    }

    def aggregateByTimeUnits(start: Int, end: Int): M = {
      timeSeriesRangedUnits(start, end).map(_._2).concatenate
    }

    def aggregateBy(start: T, end: T): M = {
      timeSeriesRanged(start, end).map(_._2).concatenate
    }

    def timeSeriesRangedUnits(start: Int, end: Int): scala.Iterable[(T, M)] = {
      timeSeriesRanged(tw.unitsBeforeNow(start), tw.unitsBeforeNow(end))
    }


    def timeSeriesRanged(start: T, end: T): scala.Iterable[(T, M)] = {
      val ts = timeSeriesMapWithEmptyDefault
      tw.between(start, end).map(time => time -> ts(time))
    }

    def timeSeriesMap: scala.collection.immutable.Map[T, M] = {
      val metricsByDate = data.map(kv => ag.timeSeriesEntry(kv))
      val result = metricsByDate.groupBy(_._1).map(kv => {
        kv._1 -> aggregate(kv._2.map(_._2))
      })
      result
    }


    def timeSeriesMapWithEmptyDefault: Map[T, M] = timeSeriesMap.withDefaultValue(m.zero).asInstanceOf[scala.collection.Map[T, M]]

    def orderedTimeSeriesMap: SortedMap[T, M] = SortedMap(timeSeriesMap.toArray: _*)

    def groupedByDay: Map[T, M] = groupMapgregate(a => tw.day(t.time(a)))

    def groupedByMonth: Map[T, M] = groupMapgregate(a => tw.month(t.time(a)))

    def groupedByHour: Map[T, M] = groupMapgregate(a => tw.hour(t.time(a)))

    def sortedByDay: scala.Seq[(T, M)] = groupMapgregate(a => tw.day(t.time(a))).toSeq.sortBy(_._1)

    def sortedByMonth: scala.Seq[(T, M)] = groupMapgregate(a => tw.month(t.time(a))).toSeq.sortBy(_._1)

    def sortedByHour: scala.Seq[(T, M)] = groupMapgregate(a => tw.hour(t.time(a))).toSeq.sortBy(_._1)

    def sortedByDayDesc: scala.Seq[(T, M)] = groupMapgregate(a => tw.day(t.time(a))).toSeq.sortBy(_._1).reverse

    def sortedByMonthDesc: scala.Seq[(T, M)] = groupMapgregate(a => tw.month(t.time(a))).toSeq.sortBy(_._1).reverse

    def sortedByHourDesc: scala.Seq[(T, M)] = groupMapgregate(a => tw.hour(t.time(a))).toSeq.sortBy(_._1).reverse

  }

  /**
   * This is a staging class between an Iterable of A and a working Metrics class.  Think of it as an implicit factory.
   * @param data
   * @tparam A
   */
  class MetricStage[A](data: Iterable[A]) {

    def asMetricsWithTime[M, T](implicit m: Metric[M], t: TimeContainer[A, T], tw: TimeWrangler[T], kw: CanBeMetric[A, M]): MetricsWithTime[A, M, T] = new MetricsWithTime[A, M, T](data)

    def asMetrics[M](implicit m: Metric[M], kw: CanBeMetric[A, M]): Metrics[A, M] = new Metrics[A, M](data)

    def asRecoMetrics(implicit m: Metric[RecommendationMetrics], kw: CanBeMetric[A, RecommendationMetrics], t: TimeContainer[A, DateHour], tw: TimeWrangler[DateHour]): MetricsWithTime[A, RecommendationMetrics, DateHour] = new MetricsWithTime[A, RecommendationMetrics, DateHour](data)
    def asRecoMetricsReadableInstant(implicit m: Metric[RecommendationMetrics], kw: CanBeMetric[A, RecommendationMetrics], t: TimeContainer[A, ReadableInstant], tw: TimeWrangler[ReadableInstant]): MetricsWithTime[A, RecommendationMetrics, ReadableInstant] = new MetricsWithTime[A, RecommendationMetrics, ReadableInstant](data)
    def asRecoMetricsByDay(implicit m: Metric[RecommendationMetrics], kw: CanBeMetric[A, RecommendationMetrics], t: TimeContainer[A, GrvDateMidnight], tw: TimeWrangler[GrvDateMidnight]): MetricsWithTime[A, RecommendationMetrics, GrvDateMidnight] = new MetricsWithTime[A, RecommendationMetrics, GrvDateMidnight](data)

    def asSponsoredMetrics(implicit m: Metric[SponsoredRecommendationMetrics], kw: CanBeMetric[A, SponsoredRecommendationMetrics], t: TimeContainer[A, DateHour], tw: TimeWrangler[DateHour]): MetricsWithTime[A, SponsoredRecommendationMetrics, DateHour] = new MetricsWithTime[A, SponsoredRecommendationMetrics, DateHour](data)
    def asSponsoredMetricsByDay(implicit m: Metric[SponsoredRecommendationMetrics], kw: CanBeMetric[A, SponsoredRecommendationMetrics], t: TimeContainer[A, GrvDateMidnight], tw: TimeWrangler[GrvDateMidnight]): MetricsWithTime[A, SponsoredRecommendationMetrics, GrvDateMidnight] = new MetricsWithTime[A, SponsoredRecommendationMetrics, GrvDateMidnight](data)
  }


  implicit def __toMR[M: Metric, T: TimeWrangler](data: Seq[(M, T)]): MetricTimeResult[M, T] = new MetricTimeResult[M, T](data)

  implicit def __toAg[A](map: Iterable[A]): MetricStage[A] = new MetricStage(map)


  //  def recoMetricMap[K,V](map: Map[K,V])(implicit ag : CanBeMetric[(K,V),RecommendationMetrics], m:Metric[RecommendationMetrics]) = new AggregableMetricMapForMetric[K,V,RecommendationMetrics](map)
  //  def sponsoredMetricMap[K,V](map: Map[K,V])(implicit ag : CanBeMetric[(K,V),SponsoredRecommendationMetrics], m:Metric[SponsoredRecommendationMetrics]) = new AggregableMetricMapForMetric[K,V,SponsoredRecommendationMetrics](map)

  // TYPES OF METRICS THAT CAN BE ACTED ON


  implicit object RecommendationMetricsMetric extends Metric[RecommendationMetrics] {
    def append(s1: RecommendationMetrics, s2: => RecommendationMetrics): RecommendationMetrics = s1 + s2

    val zero: RecommendationMetrics = RecommendationMetrics.empty
  }

  implicit object ScopedMetricsMetric extends Metric[ScopedMetrics] {
    def append(s1: ScopedMetrics, s2: => ScopedMetrics): ScopedMetrics = s1 + s2

    val zero: ScopedMetrics = ScopedMetrics.empty
  }

  implicit object SponsoredMetricMetric extends Metric[SponsoredRecommendationMetrics] {
    def append(s1: SponsoredRecommendationMetrics, s2: => SponsoredRecommendationMetrics): SponsoredRecommendationMetrics = s1 + s2

    val zero: SponsoredRecommendationMetrics = SponsoredRecommendationMetrics.empty
  }

  implicit object ViralMetricsMetric extends Metric[ViralMetrics] {
    def append(s1: ViralMetrics, s2: => ViralMetrics): ViralMetrics = s1 + s2

    val zero: ViralMetrics = ViralMetrics.empty
  }

  implicit object StandardMetricsMetric extends Metric[StandardMetrics] {
    def append(s1: StandardMetrics, s2: => StandardMetrics): StandardMetrics = s1 + s2

    val zero: StandardMetrics = StandardMetrics.empty
  }

  implicit object RecoReportMetricsMetric extends Metric[RecoReportMetrics] {
    def append(s1: RecoReportMetrics, s2: => RecoReportMetrics): RecoReportMetrics = s1 + s2
    val zero: RecoReportMetrics = RecoReportMetrics.empty
  }

  implicit object SyndicationReportMetricsMetric extends Metric[SyndicationReportMetrics] {
    def zero: SyndicationReportMetrics = SyndicationReportMetrics.empty
    def append(f1: SyndicationReportMetrics, f2: => SyndicationReportMetrics): SyndicationReportMetrics = f1 + f2
  }

  implicit object RecoArticleReportMetricsMetric extends Metric[RecoArticleReportMetrics] {
    def append(s1: RecoArticleReportMetrics, s2: => RecoArticleReportMetrics): RecoArticleReportMetrics = s1 + s2
    val zero: RecoArticleReportMetrics = RecoArticleReportMetrics.empty
  }

  implicit object CampaignReportMetricsMetric extends Metric[CampaignReportMetrics] {
    def zero: CampaignReportMetrics = CampaignReportMetrics.empty
    def append(f1: CampaignReportMetrics, f2: => CampaignReportMetrics): CampaignReportMetrics = f1 + f2
  }

  implicit object CampaignDashboardMetricsLiteMetric extends Metric[CampaignDashboardMetricsLite] {
    def zero: CampaignDashboardMetricsLite = CampaignDashboardMetricsLite.empty

    def append(f1: CampaignDashboardMetricsLite, f2: => CampaignDashboardMetricsLite): CampaignDashboardMetricsLite = f1 + f2
  }

  implicit object DateHourRecoReportMetricsCanBeMetric extends CanBeMetric[(DateHour, RecoReportMetrics), RecoReportMetrics] {
    def toMetric(original: (DateHour, RecoReportMetrics)): RecoReportMetrics = original._2
  }

  implicit object DateHourCampaignReportMetricsCanBeMetric extends CanBeMetric[(DateHour, CampaignReportMetrics), CampaignReportMetrics] {
    def toMetric(original: (DateHour, CampaignReportMetrics)): CampaignReportMetrics = original._2
  }

  implicit object AdvertiserCampaignReportRowCanBeMetric extends CanBeMetric[AdvertiserCampaignReportRow, CampaignReportMetrics] {
    def toMetric(original: AdvertiserCampaignReportRow): CampaignReportMetrics = CampaignReportMetrics(
      original.articleImpressionsClean,
      original.articleImpressionsDiscarded,
      original.articleImpressionsViewedClean,
      original.articleImpressionsViewedDiscarded,
      original.clicksClean,
      original.clicksDiscarded,
      original.advertiserSpendClean.pennies,
      original.advertiserSpendDiscarded.pennies,
      original.conversionsClean
    )
  }

  implicit object AdvertiserCampaignArticleReportRowCanBeMetric extends CanBeMetric[AdvertiserCampaignArticleReportRow, CampaignReportMetrics] {
    def toMetric(original: AdvertiserCampaignArticleReportRow): CampaignReportMetrics = CampaignReportMetrics(
      original.articleImpressionsClean,
      original.articleImpressionsDiscarded,
      original.articleImpressionsViewedClean,
      original.articleImpressionsViewedDiscarded,
      original.clicksClean,
      original.clicksDiscarded,
      original.advertiserSpendClean.pennies,
      original.advertiserSpendDiscarded.pennies,
      0l
    )
  }

  implicit object AdvertiserCampaignReportRowCanBeMetricLite extends CanBeMetric[AdvertiserCampaignReportRow, CampaignDashboardMetricsLite] {
    def toMetric(original: AdvertiserCampaignReportRow): CampaignDashboardMetricsLite = CampaignDashboardMetricsLite(original)
  }

  implicit object AdvertiserCampaignArticleReportRowCanBeMetricLite extends CanBeMetric[AdvertiserCampaignArticleReportRow, CampaignDashboardMetricsLite] {
    def toMetric(original: AdvertiserCampaignArticleReportRow): CampaignDashboardMetricsLite = CampaignDashboardMetricsLite(original)
  }

  implicit object ClickstreamTransformer extends KeyValueMetric[ClickStreamKey, Long, RecommendationMetrics] {
    def toMetric(key: ClickStreamKey, value: Long): RecommendationMetrics = {
      key.clickType match {
        case ClickType.viewed =>
          RecommendationMetrics.fromCountByType(RecommendationMetricCountBy.click,value)
        case ClickType.clicked =>
          RecommendationMetrics.fromCountByType(RecommendationMetricCountBy.click,value)
        case ClickType.impressionserved =>
          RecommendationMetrics.fromCountByType(RecommendationMetricCountBy.articleView,value)
        case ClickType.impressionviewed => // This is in lieu of impression viewed existing as separate from served
          RecommendationMetrics.fromCountByType(RecommendationMetricCountBy.articleView,value)
        case unknownType =>
          throw new UnsupportedOperationException("UNable to use clicktype " + unknownType.toString)
      }
    }
  }
  //TYPES THAT CAN BE TRANSFORMED INTO METRICS
  implicit object ScopedTransformer extends KeyValueMetric[ScopedMetricsKey, Long, RecommendationMetrics] {
    def toMetric(key: ScopedMetricsKey, value: Long): RecommendationMetrics = {
      RecommendationMetrics.fromCountByType(key.countBy, value)
    }
  }

  implicit object ScopedToScopedTransformer extends KeyValueMetric[ScopedMetricsKey, Long, ScopedMetrics] {
    def toMetric(key: ScopedMetricsKey, value: Long): ScopedMetrics = {
      key.countBy match {
        case RecommendationMetricCountBy.articleImpression => ScopedMetrics(value,0,0,0,0)
        case RecommendationMetricCountBy.click => ScopedMetrics(0,value,0,0,0)
        case RecommendationMetricCountBy.articleView => ScopedMetrics(0,0,value,0,0)
        case RecommendationMetricCountBy.unitImpression => ScopedMetrics(value,0,0,0,0)
        case RecommendationMetricCountBy.unitImpressionViewed => ScopedMetrics(0,0,value,0,0)
        case RecommendationMetricCountBy.conversion => ScopedMetrics.empty
        case RecommendationMetricCountBy.costPerClick => ScopedMetrics(0,0,0, value,0)
        case RecommendationMetricCountBy.unitImpressionFailed => ScopedMetrics(0,0,0,0,value)
        case _ => ScopedMetrics.empty
      }
    }
  }


  implicit object StandardMetricsTransformerDateMidnight extends CanBeMetric[(GrvDateMidnight, StandardMetrics), StandardMetrics] {
    def toMetric(original: (GrvDateMidnight, StandardMetrics)): StandardMetrics = original._2
  }

  implicit object StandardMetricsTransformerDateHour extends CanBeMetric[(DateHour, StandardMetrics), StandardMetrics] {
    def toMetric(original: (DateHour, StandardMetrics)): StandardMetrics = original._2
  }


  implicit object ViralMetricsTransformerDateMidnight extends CanBeMetric[(GrvDateMidnight, ViralMetrics), ViralMetrics] {
    def toMetric(original: (GrvDateMidnight, ViralMetrics)): ViralMetrics = original._2
  }



  implicit object ViralMetricsTransformerDateHour extends CanBeMetric[(DateHour, ViralMetrics), ViralMetrics] {
    def toMetric(original: (DateHour, ViralMetrics)): ViralMetrics = original._2
  }


  implicit object ArticleRecommendationTransformer extends KeyValueMetric[ArticleRecommendationMetricKey, Long, RecommendationMetrics] {
    def toMetric(key: ArticleRecommendationMetricKey, value: Long): RecommendationMetrics = {
      RecommendationMetrics.fromCountByType(key.countBy, value)
    }
  }

  implicit object SponsoredRecommendationTransformer extends KeyValueMetric[SponsoredMetricsKey, Long, RecommendationMetrics] {
    def toMetric(key: SponsoredMetricsKey, value: Long): RecommendationMetrics = {
      RecommendationMetrics.fromCountByType(key.countBy, value)
    }
  }

  implicit object SponsoredToSponsoredRecommendationTransformer extends KeyValueMetric[SponsoredMetricsKey, Long, SponsoredRecommendationMetrics] {
    def toMetric(key: SponsoredMetricsKey, value: Long): SponsoredRecommendationMetrics = {
      SponsoredRecommendationMetrics.apply(RecommendationMetrics.fromCountByType(key.countBy, value), key.totalSpent(value))
    }
  }

  implicit object CampaignSponsoredTransformer extends KeyValueMetric[CampaignMetricsKey, Long, SponsoredRecommendationMetrics] {
    def toMetric(key: CampaignMetricsKey, value: Long): SponsoredRecommendationMetrics = SponsoredRecommendationMetrics(RecommendationMetrics.fromCountByType(key.countBy, value), key.cpc)
  }

  implicit object CampaignTransformer extends KeyValueMetric[CampaignMetricsKey, Long, RecommendationMetrics] {
    def toMetric(key: CampaignMetricsKey, value: Long): RecommendationMetrics = RecommendationMetrics.fromCountByType(key.countBy, value)
  }

  implicit object ArticleSponsoredKey extends KeyValueMetric[ArticleSponsoredMetricsKey, Long, RecommendationMetrics] {
    def toMetric(key: ArticleSponsoredMetricsKey, value: Long): RecommendationMetrics = RecommendationMetrics.fromCountByType(key.countBy, value)
  }


  implicit object RollupTransformer extends KeyValueMetric[RollupRecommendationMetricKey, Long, RecommendationMetrics] {
    def toMetric(key: RollupRecommendationMetricKey, value: Long): RecommendationMetrics = RecommendationMetrics.fromCountByType(key.countBy, value)
  }

  //TYPES THAT CAN BE TRANSFORMED INTO TIME
  implicit object ViralMetricsDateMidnight extends TimeContainer[(GrvDateMidnight, ViralMetrics), GrvDateMidnight] {
    def time(value: (GrvDateMidnight, ViralMetrics)): GrvDateMidnight = value._1
  }

  implicit object ViralMetricsDateHour extends TimeContainer[(DateHour, ViralMetrics), DateHour] {
    def time(value: (DateHour, ViralMetrics)): DateHour = value._1
  }

  /**
   * This is why we have (X,Y),Z.  Because here we're aggregating ViralMetrics stored by DateHour by GrvDateMidnight.
   */
  implicit object ViralMetricsDateHourToDateMidnight extends TimeContainer[(DateHour, ViralMetrics), GrvDateMidnight] {
    def time(value: (DateHour, ViralMetrics)): GrvDateMidnight = value._1.toGrvDateMidnight
  }


  implicit object ScopedLongTime extends TimeContainer[(ScopedMetricsKey, Long), Long] {
    def time(value: (ScopedMetricsKey, Long)): Long = value._1.dateTimeMs
  }

  implicit object ScopedDateTime extends TimeContainer[(ScopedMetricsKey, Long), DateHour] {
    def time(value: (ScopedMetricsKey, Long)): DateHour = DateHour(value._1.dateTimeMs)
  }

  implicit object ScopedDateMidnight extends TimeContainer[(ScopedMetricsKey, Long), GrvDateMidnight] {
    def time(value: (ScopedMetricsKey, Long)): GrvDateMidnight = new GrvDateMidnight(value._1.dateTimeMs)
  }

  implicit object ArticleRecommendationMetricsLong extends TimeContainer[(ArticleRecommendationMetricKey, Long), Long] {
    def time(value: (ArticleRecommendationMetricKey, Long)): Long = value._1.dateHour.getMillis
  }

  implicit object ArticleRecommendationMetricsDateHour extends TimeContainer[(ArticleRecommendationMetricKey, Long), DateHour] {
    def time(value: (ArticleRecommendationMetricKey, Long)): DateHour = value._1.dateHour
  }

  implicit object ArticleRecommendationMetricsReadableInstant extends TimeContainer[(ArticleRecommendationMetricKey, Long), ReadableInstant] {
    def time(value: (ArticleRecommendationMetricKey, Long)): DateTime = value._1.dateHour.dateTime
  }

  implicit object ArticleRecommendationMetricsDateMidnight extends TimeContainer[(ArticleRecommendationMetricKey, Long), GrvDateMidnight] {
    def time(value: (ArticleRecommendationMetricKey, Long)): GrvDateMidnight = new GrvDateMidnight(value._1.dateHour.getMillis)
  }

  implicit object StandardMetricsDateMidnight extends TimeContainer[(GrvDateMidnight, StandardMetrics), GrvDateMidnight] {
    def time(value: (GrvDateMidnight, StandardMetrics)): GrvDateMidnight = value._1
  }

  implicit object StandardMetricsDateHour extends TimeContainer[(DateHour, StandardMetrics), DateHour] {
    def time(value: (DateHour, StandardMetrics)): DateHour = value._1
  }

  implicit object StandardMetricsDateHourToDateMidnight extends TimeContainer[(DateHour, StandardMetrics), GrvDateMidnight] {
    def time(value: (DateHour, StandardMetrics)): GrvDateMidnight = value._1.toGrvDateMidnight
  }


  implicit object RollupRecommendationMetricsLong extends TimeContainer[(RollupRecommendationMetricKey, Long), Long] {
    def time(value: (RollupRecommendationMetricKey, Long)): Long = value._1.dateHour.getMillis
  }


  implicit object RollupRecommendationMetricsDateHour extends TimeContainer[(RollupRecommendationMetricKey, Long), DateHour] {
    def time(value: (RollupRecommendationMetricKey, Long)): DateHour = value._1.dateHour
  }

  implicit object RollupRecommendationMetricsReadableInstant extends TimeContainer[(RollupRecommendationMetricKey, Long), ReadableInstant] {
    def time(value: (RollupRecommendationMetricKey, Long)): DateTime = value._1.dateHour.dateTime
  }

  implicit object RollupRecommendationMetricsDateMidnight extends TimeContainer[(RollupRecommendationMetricKey, Long), GrvDateMidnight] {
    def time(value: (RollupRecommendationMetricKey, Long)): GrvDateMidnight = value._1.dateHour.toGrvDateMidnight
  }

  implicit object ArticleSponsoredMetricsDateHour extends TimeContainer[(ArticleSponsoredMetricsKey, Long), DateHour] {
    def time(value: (ArticleSponsoredMetricsKey, Long)): DateHour = value._1.dateHour
  }

  implicit object SponsoredRecommendationMetricsLong extends TimeContainer[(SponsoredMetricsKey, Long), Long] {
    def time(value: (SponsoredMetricsKey, Long)): Long = value._1.dateHour.getMillis
  }

  implicit object SponsoredRecommendationMetricsDateHour extends TimeContainer[(SponsoredMetricsKey, Long), DateHour] {
    def time(value: (SponsoredMetricsKey, Long)): DateHour = value._1.dateHour
  }

  implicit object SponsoredRecommendationMetricsDateMidnight extends TimeContainer[(SponsoredMetricsKey, Long), GrvDateMidnight] {
    def time(value: (SponsoredMetricsKey, Long)): GrvDateMidnight = new GrvDateMidnight(value._1.dateHour.getMillis)
  }

  implicit object ClickstreamHour extends TimeContainer[(ClickStreamKey,Long), DateHour] {
    def time(value: (ClickStreamKey, Long)): DateHour = value._1.hour
  }

  implicit object CampaignMetricsDateHourContainer extends TimeContainer[(CampaignMetricsKey,Long), DateHour] {
    def time(value: (CampaignMetricsKey, Long)): DateHour = value._1.dateHour
  }

  implicit object DateHourRecoReportMetricsTimeContainer extends TimeContainer[(DateHour, RecoReportMetrics), DateHour] {
    def time(value: (DateHour, RecoReportMetrics)): DateHour = value._1
  }

  implicit object AdvertiserCampaignReportRowTimeContainer extends TimeContainer[AdvertiserCampaignReportRow, DateHour] {
    def time(value: AdvertiserCampaignReportRow): DateHour = value.logHour
  }

  implicit object AdvertiserCampaignArticleReportRowTimeContainer extends TimeContainer[AdvertiserCampaignArticleReportRow, GrvDateMidnight] {
    def time(value: AdvertiserCampaignArticleReportRow): GrvDateMidnight = value.logDate.toGrvDateMidnight
  }

  implicit object DateHourCampaignReportMetricsTimeContainer extends TimeContainer[(DateHour, CampaignReportMetrics), DateHour] {
    def time(value: (DateHour, CampaignReportMetrics)): DateHour = value._1
  }

  //TYPICAL GROUPINGS FOR DIFFERENT SCENARIOS

  implicit object RollupMetricsNoSiteGrouper extends CanBeGrouper[(RollupRecommendationMetricKey, Long), RollupRecommendationMetricsNoSiteGroupByDayKey] with TimeContainer[RollupRecommendationMetricsNoSiteGroupByDayKey, GrvDateMidnight] {
    def fromData(data: (RollupRecommendationMetricKey, Long)): RollupRecommendationMetricsNoSiteGroupByDayKey = RollupRecommendationMetricsNoSiteGroupByDayKey(data._1.dateHour.toGrvDateMidnight, data._1.placementId, data._1.bucketId)

    def compare(x: RollupRecommendationMetricsNoSiteGroupByDayKey, y: RollupRecommendationMetricsNoSiteGroupByDayKey): Int = {
      val k1 = x
      val k2 = y

      if (k1.dateMidnight.getMillis > k2.dateMidnight.getMillis) return -1
      if (k2.dateMidnight.getMillis > k1.dateMidnight.getMillis) return 1

      if (k1.placementId < k2.placementId) return -1
      if (k2.placementId < k1.placementId) return 1

      if (k1.bucket < k2.bucket) return -1
      if (k2.bucket < k1.bucket) return 1

      0
    }

    def time(value: RollupRecommendationMetricsNoSiteGroupByDayKey): GrvDateMidnight = value.dateMidnight
  }

  implicit object RollupMetricsNoSiteGrouperByHour extends CanBeGrouper[(RollupRecommendationMetricKey, Long), RollupRecommendationMetricsNoSiteGroupByHourKey] with TimeContainer[RollupRecommendationMetricsNoSiteGroupByHourKey, DateHour] {
    def fromData(data: (RollupRecommendationMetricKey, Long)): RollupRecommendationMetricsNoSiteGroupByHourKey = RollupRecommendationMetricsNoSiteGroupByHourKey(data._1.dateHour, data._1.placementId, data._1.bucketId)

    def compare(x: RollupRecommendationMetricsNoSiteGroupByHourKey, y: RollupRecommendationMetricsNoSiteGroupByHourKey): Int = {
      val k1 = x
      val k2 = y

      if (k1.hour.isAfter(k2.hour)) return -1
      if (k2.hour.isAfter(k1.hour)) return 1

      if (k1.placementId < k2.placementId) return -1
      if (k2.placementId > k1.placementId) return 1

      if (k1.bucket < k2.bucket) return -1
      if (k2.bucket < k1.bucket) return 1

      0
    }

    def time(value: RollupRecommendationMetricsNoSiteGroupByHourKey): DateHour = value.hour
  }

  implicit object AdvertiserCampaignReportRowByDayCampaignKeyCanBeGrouper extends CanBeGrouper[AdvertiserCampaignReportRow, AdvertiserCampaignReportDayCampaignKey] with TimeContainer[AdvertiserCampaignReportDayCampaignKey, GrvDateMidnight] {
    def fromData(data: AdvertiserCampaignReportRow): AdvertiserCampaignReportDayCampaignKey = AdvertiserCampaignReportDayCampaignKey(data.logHour.toGrvDateMidnight, data.advertiserGuid, data.campaignId)

    def time(value: AdvertiserCampaignReportDayCampaignKey): GrvDateMidnight = value.day

    def compare(x: AdvertiserCampaignReportDayCampaignKey, y: AdvertiserCampaignReportDayCampaignKey): Int = {
      if (x.day.isBefore(y.day)) return -1
      if (y.day.isBefore(x.day)) return 1

      val guidComp = x.advertiserGuid.compareToIgnoreCase(y.advertiserGuid)
      if (0 != guidComp) return guidComp
      x.campaignId.compare(y.campaignId)
    }
  }

  implicit object AdvertiserCampaignReportRowByCampaignKeyCanBeGrouper extends CanBeGrouper[AdvertiserCampaignReportRow, CampaignKey] {
    def fromData(data: AdvertiserCampaignReportRow): CampaignKey = data.campaignKey

    def compare(x: CampaignKey, y: CampaignKey): Int = {
      val siteIdComp = x.siteKey.siteId.compareTo(y.siteKey.siteId)
      if (0 != siteIdComp) return siteIdComp
      x.campaignId.compareTo(y.campaignId)
    }
  }

  implicit object AdvertiserCampaignReportRowByDateMidnightCanBeGrouper extends CanBeGrouper[AdvertiserCampaignReportRow, GrvDateMidnight] {
    def fromData(data: AdvertiserCampaignReportRow): GrvDateMidnight = data.logHour.toGrvDateMidnight

    def compare(x: GrvDateMidnight, y: GrvDateMidnight): Int = x.getMillis.compare(y.getMillis)
  }

  implicit object AdvertiserCampaignArticleReportRowByArticleByCampaignKeyCanBeGrouper extends CanBeGrouper[AdvertiserCampaignArticleReportRow, AdvertiserCampaignReportArticleCampaignKey] {
    def fromData(data: AdvertiserCampaignArticleReportRow): AdvertiserCampaignReportArticleCampaignKey = AdvertiserCampaignReportArticleCampaignKey(data.articleId, data.advertiserGuid, data.campaignId)

    def compare(x: AdvertiserCampaignReportArticleCampaignKey, y: AdvertiserCampaignReportArticleCampaignKey): Int = {
      val articleComp = x.articleId.compare(y.articleId)
      if (0 != articleComp) return articleComp

      val guidComp = x.advertiserGuid.compareToIgnoreCase(y.advertiserGuid)
      if (0 != guidComp) return guidComp
      x.campaignId.compare(y.campaignId)
    }
  }

  implicit object AdvertiserCampaignArticleReportRowByDayByArticleByCampaignKeyCanBeGrouper extends CanBeGrouper[AdvertiserCampaignArticleReportRow, AdvertiserCampaignReportDayArticleCampaignKey] {
    def fromData(data: AdvertiserCampaignArticleReportRow): AdvertiserCampaignReportDayArticleCampaignKey = AdvertiserCampaignReportDayArticleCampaignKey(
      data.logDate.toGrvDateMidnight, data.articleId, data.advertiserGuid, data.campaignId)

    def compare(x: AdvertiserCampaignReportDayArticleCampaignKey, y: AdvertiserCampaignReportDayArticleCampaignKey): Int = {
      val dayComp = x.day.getMillis.compare(y.day.getMillis)
      if (0 != dayComp) return dayComp

      val articleComp = x.articleId.compare(y.articleId)
      if (0 != articleComp) return articleComp

      val guidComp = x.advertiserGuid.compareToIgnoreCase(y.advertiserGuid)
      if (0 != guidComp) return guidComp
      x.campaignId.compare(y.campaignId)
    }
  }

  //TYPES THAT CAN BE UTILIZES IN TIME SERIES

  /**
   * This allows us to build metrical data structures that have Longs as their value instead of massive numbers of DateTime objects.
   *
   * The Long will be treated as a date-hour milliseconds stamp, and intervals will be calculated via date-hour.
   */
  implicit object LongTimeWrangler extends TimeWrangler[Long] with LongOrdering {
    def now: Long = grvtime.currentHourMillis


    //Convert from readable back to the representation
    def readableDateTimeToRepr(dateTime: ReadableDateTime): Long = dateTime.getMillis

    def readableDateTime(time: Long): ReadableDateTime = DateHour(time): ReadableDateTime

    def numeric(time: Long): Long = time

    def unitsBeforeNow(unit: Int): Long = grvtime.currentHour.minusHours(unit).getMillis

    def between(start: Long, end: Long): immutable.IndexedSeq[Long] = {
      val startHour: DateHour = DateHour(start)
      val endHour = DateHour(end)
      val hours = Hours.hoursBetween(startHour, endHour).getHours
      (0 to hours).map(hour => startHour.plusHours(hour).getMillis)
    }

    def percentageComplete: Double = grvtime.percentIntoCurrentHour
  }

  implicit object DateHourTimeWrangler extends TimeWrangler[DateHour] {
    def readableDateTime(time: DateHour): ReadableDateTime = time: ReadableDateTime

    //Convert from readable back to the representation
    def readableDateTimeToRepr(dateTime: ReadableDateTime): DateHour = DateHour(dateTime)

    def now: DateHour = grvtime.currentHour

    def percentageComplete: Double = grvtime.percentIntoCurrentHour

    def numeric(time: DateHour): Long = time.getMillis

    def unitsBeforeNow(unit: Int): DateHour = now.minusHours(unit)

    def between(start: DateHour, end: DateHour): immutable.IndexedSeq[DateHour] = {
      val hours: Int = Hours.hoursBetween(start, end).getHours
      (0 to hours).map(hour => start.plusHours(hour))
    }

    def compare(x: DateHour, y: DateHour): Int = x.compareTo(y)
  }

  implicit object ReadableInstantTimeWrangler extends TimeWrangler[ReadableInstant] {
    def readableDateTime(time: ReadableInstant): DateTime = new DateTime(time.getMillis)

    def readableDateTimeToRepr(dateTime: ReadableDateTime): ReadableDateTime = dateTime

    def now: DateTime = grvtime.currentHour.toDateTime

    def percentageComplete: Double = grvtime.percentIntoCurrentHour

    def numeric(time: ReadableInstant): Long = time.getMillis

    def unitsBeforeNow(unit: Int): DateTime = now.minusHours(unit)

    def between(start: ReadableInstant, end: ReadableInstant): immutable.IndexedSeq[DateTime] = {
      val hours = Hours.hoursBetween(start, end).getHours
      val startDt = new DateTime(start.getMillis)
      (0 to hours).map(hour => startDt.plusHours(hour))
    }

    def compare(x: ReadableInstant, y: ReadableInstant): Int = x.compareTo(y)
  }

  implicit object DateMidnightTimeWrangler extends TimeWrangler[GrvDateMidnight] {
    def now: GrvDateMidnight = grvtime.currentDay

    //Convert from readable back to the representation
    def readableDateTimeToRepr(dateTime: ReadableDateTime): GrvDateMidnight = dateTime.toDateTime.toGrvDateMidnight

    def readableDateTime(time: GrvDateMidnight): ReadableDateTime = time: ReadableDateTime

    def numeric(time: GrvDateMidnight): Long = time.getMillis

    def unitsBeforeNow(unit: Int): GrvDateMidnight = now.minusDays(unit)

    def percentageComplete: Double = grvtime.percentIntoCurrentDay

    def between(start: GrvDateMidnight, end: GrvDateMidnight): immutable.IndexedSeq[GrvDateMidnight] = {
      val days = Days.daysBetween(start, end).getDays
      (0 to days).map(day => start.plusDays(day))
    }

    def compare(x: GrvDateMidnight, y: GrvDateMidnight): Int = x.compareTo(y)
  }

}

case class SeriesItem(x:Long, y:Double, size: Option[Double])
case class Series(key:String, color:Option[String], values: Seq[SeriesItem])


object Highchart {

  case class Highchart[D: Writes](chart: Chart, title: Option[Title] = None, xAxis: Option[XAxis] = None,
                                  yAxis: Option[YAxis] = None, series: List[Series[D]] = List.empty)

  case class Chart(`type`: String)

  case class Title(text: String)

  case class XAxis(categories: List[String])

  case class YAxis(title: Option[Title] = None)

  case class Series[D: Writes](name: String, data: D, `type`: Option[String] = None, pointInterval: Option[Float] = None)

  implicit val seriesArrayArrayLongFormat: Writes[Series[Array[Array[Long]]]] = Writes[Series[Array[Array[Long]]]] {
    case series => Json.obj(
      "name" -> series.name,
      "data" -> Json.toJson(series.data),
      "type" -> series.`type`,
      "pointInterval" -> series.pointInterval
    )
  }
}


case class GraphSeries(name:String, data: Seq[Long], `type`:String="column", stacking:String="normal")
case class CtrGraphSeries(name:String, data: Seq[Double], `type`:String="column", stacking:String="normal")