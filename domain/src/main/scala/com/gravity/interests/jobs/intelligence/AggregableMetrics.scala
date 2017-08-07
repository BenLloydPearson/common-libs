package com.gravity.interests.jobs.intelligence

import com.gravity.domain.grvmetrics
import com.gravity.domain.recommendations.RecoReportMetrics
import com.gravity.interests.jobs.intelligence.hbase.MetricsKeyLite
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.time.{DateHour, GrvDateMidnight, Time}
import com.gravity.utilities.{DateTimeStepper, HashUtils, grvmath}
import org.joda.time.ReadableDateTime
import play.api.libs.json.{Format, Json}

import scala.collection._
import scalaz.std.iterable._
import scalaz.syntax.foldable._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.option._
import scalaz.{Monoid, Semigroup}

trait AggregableMetrics

case class Measurements(mean: Double, variance: Double) {
  val standardDeviation: Double = math.sqrt(variance)
}

object Measurements {
  val empty: Measurements = Measurements(0.0, 0.0)
}

object AggregableMetrics {
  implicit def zzzMapReadableDateTimeToAggregable[K <: ReadableDateTime, V : Monoid](map: Map[K, V]): KeyedByReadableDateTimeMapWrap[K, V] = new KeyedByReadableDateTimeMapWrap(map)
  implicit def zzzMapDateHourToAggregable[K <: DateHour, V : Monoid](map: Map[K, V]): KeyedByDateHourMapWrap[K, V] = new KeyedByDateHourMapWrap(map)

  implicit val doubleMonoidInstance: Monoid[Double] = new Monoid[Double] {
  	def append(f1: Double, f2: => Double) = f1 + f2
  	def zero: Double = 0d
  }

  class KeyedByReadableDateTimeMapWrap[K <: ReadableDateTime, V : Monoid](x: Map[K, V]) {
    def aggregateBetween(fromInclusive: K, toInclusive: K): V = {
      AggregableMetrics.aggregateBetween(x, fromInclusive, toInclusive)
    }

    def aggregateAll(): V = x.values.concatenate
  }

  class KeyedByDateHourMapWrap[K <: DateHour, V : Monoid](x: Map[K, V]) {
    def aggregateBetween(fromInclusive: K, toInclusive: K): V = {
      AggregableMetrics.aggregateBetweenHourly(x, fromInclusive, toInclusive)
    }

    def aggregateAll(): V = x.values.concatenate
  }

  trait CanTotal[T, N] {
    def total(metrics: T*): N
  }

  def aggregate[K, V : Monoid](maps: Iterable[Map[K, V]]): Map[K, V] = {
    if (maps.isEmpty) {
      return Map.empty
    }

    val result = mutable.Map[K, V]()
    maps foreach (aggregate(result, _))
    result
  }

  def aggregate[K, V : Monoid](aggregateMap: mutable.Map[K, V], inputMap: Map[K, V]) {
    inputMap foreach {
      case (key, metrics) =>
        val sum = aggregateMap.get(key).orZero |+| metrics
        aggregateMap.update(key, sum)
    }
  }


  def aggregateBetween[K <: ReadableDateTime, V](metricsMap: Map[K, V], fromInclusive: K, toInclusive: K)(implicit monoid: Monoid[V]): V = {
    metricsMap.foldLeft(monoid.zero) { case (acc, (dm, metrics)) =>
      if ((dm.isEqual(fromInclusive) || dm.isAfter(fromInclusive)) && (dm.isBefore(toInclusive) || dm.isEqual(toInclusive))) {
        acc |+| metrics
      } else {
        acc
      }
    }
  }


  def aggregateBetweenHourly[K <: DateHour, V](metricsMap: Map[K, V], fromInclusive: K, toInclusive: K)(implicit monoid: Monoid[V]): V = {
    metricsMap.foldLeft(monoid.zero) { case (acc, (dm, metrics)) =>
      if ((dm.isEqual(fromInclusive) || dm.isAfter(fromInclusive)) && (dm.isBefore(toInclusive) || dm.isEqual(toInclusive))) {
        acc |+| metrics
      } else {
        acc
      }
    }
  }

  def sumAll[K, V](metricsMap: Map[K, V])(implicit ev1: K => ReadableDateTime, ev2: Monoid[V]): V = metricsMap.values.concatenate

  def hourlyTrendingScoreBetweenInclusive[V, N](metrics: Map[DateHour, V], fromInclusive: GrvDateMidnight, toInclusive: GrvDateMidnight)(implicit monoid: Monoid[V], ct: CanTotal[V, N], n: Numeric[N]): Double = {
    hourlyTrendingScoreBetween(metrics, fromInclusive, toInclusive.plusDays(1))
  }

  def hourlyTrendingScoreBetween[V, N](metrics: Map[DateHour, V], fromInclusive: ReadableDateTime, toExclusive: ReadableDateTime)(implicit monoid: Monoid[V], ct: CanTotal[V, N], n: Numeric[N]): Double = {
    val nextHour = DateHour.currentHour.plusHours(1).toDateTime
    val toExclusiveCappedAtNextHour = if (toExclusive.isAfter(nextHour)) nextHour else toExclusive
    val hours = DateHour.hoursBetween(fromInclusive, toExclusiveCappedAtNextHour)

    val totals = for {
      h <- hours
      v = metrics.get(h).orZero
    } yield {
      ct.total(v)
    }

    val score = grvmath.getTrendingScore(totals, period = if (totals.size <= 3) 1 else 3)

    if (score == Double.NaN || score == Double.PositiveInfinity || score == Double.NaN) {
      println("Received score as : " + score + " from input: " + totals)
    }
    score

  }

  def rollingTrendBetween[K, V](metrics: Map[K, V], fromInclusive: K, toInclusive: K, step: DateTimeStepper[K], period: Int = 1)
                               (implicit kToRdt: K => ReadableDateTime, ct: CanTotal[V, Long]): Seq[Double] = {
    val times = Time.iterate(fromInclusive, toInclusive, step)
    val series = times.map(metrics.get(_) some (ct.total(_)) none 0L).toIndexedSeq
    series.indices map (i => grvmath.getTrendingScore(series.slice(0, i + 1), period))
  }
}

object StandardMetrics {

  val empty: StandardMetrics = StandardMetrics(0, 0, 0, 0, 0)

  implicit val jsonFormat: Format[StandardMetrics] = Json.format[StandardMetrics]

  implicit val StandardMetricsSemigroup: Semigroup[StandardMetrics] = new Semigroup[StandardMetrics] {
    def append(sm1: StandardMetrics, sm2: => StandardMetrics): StandardMetrics = sm1 + sm2
  }
  implicit val StandardMetricsMonoid: Monoid[StandardMetrics] = Monoid.instance(StandardMetricsSemigroup.append, empty)

  def buildFromMap(metricsMap: Map[StandardMetricType.Type, Long]): StandardMetrics = {
    StandardMetrics(
      metricsMap.getOrElse(StandardMetricType.views, 0l),
      metricsMap.getOrElse(StandardMetricType.socialReferrers, 0l),
      metricsMap.getOrElse(StandardMetricType.searchReferrers, 0l),
      metricsMap.getOrElse(StandardMetricType.keyPageReferrers, 0l),
      metricsMap.getOrElse(StandardMetricType.publishes, 0l)
    )
  }

  val OneView: StandardMetrics = StandardMetrics(1, 0, 0, 0, 0)
  val OneSocial: StandardMetrics = StandardMetrics(0, 1, 0, 0, 0)
  val OneSearch: StandardMetrics = StandardMetrics(0, 0, 1, 0, 0)
  val OneKeyPage: StandardMetrics = StandardMetrics(0, 0, 0, 1, 0)
  val OnePublish: StandardMetrics = StandardMetrics(0, 0, 0, 0, 1)

  def measure(metrics: Iterable[StandardMetrics]): StandardStatistics = {
    val vMean = grvmath.meanBy(metrics)(_.views)
    val vVar = grvmath.varianceBy(metrics, Some(vMean))(_.views)

    val socMean = grvmath.meanBy(metrics)(_.socialReferrers)
    val socVar = grvmath.varianceBy(metrics, Some(socMean))(_.socialReferrers)

    val seoMean = grvmath.meanBy(metrics)(_.searchReferrers)
    val seoVar = grvmath.varianceBy(metrics, Some(seoMean))(_.searchReferrers)

    val kMean = grvmath.meanBy(metrics)(_.keyPageReferrers)
    val kVar = grvmath.varianceBy(metrics, Some(kMean))(_.keyPageReferrers)

    val pMean = grvmath.meanBy(metrics)(_.publishes)
    val pVar = grvmath.varianceBy(metrics, Some(pMean))(_.publishes)


    StandardStatistics(
      views = Measurements(vMean, vVar),
      socialReferrers = Measurements(socMean, socVar),
      searchReferrers = Measurements(seoMean, seoVar),
      keyPageReferrers = Measurements(kMean, kVar),
      publishes = Measurements(pMean, pVar)
    )
  }

  def mapOldToNewDaily(metrics: Map[GrvDateMidnight, StandardMetrics]): Map[StandardMetricsDailyKey, Long] = {
    for {
      (dm, m) <- metrics
      (mt, v) <- m.metricsSeq
    } yield StandardMetricsDailyKey(dm, mt) -> v
  }

  def mapOldToNewHourly(metrics: Map[DateHour, StandardMetrics]): Map[StandardMetricsHourlyKey, Long] = {
    for {
      (dh, m) <- metrics
      (mt, v) <- m.metricsSeq
    } yield StandardMetricsHourlyKey(dh, mt) -> v
  }

  def mapNewToOldDaily(metrics: Map[StandardMetricsDailyKey, Long]): Map[GrvDateMidnight, StandardMetrics] = {
    metrics.toSeq.groupBy(_._1.dateMidnight).map(grp => {
      val dm = grp._1
      val keyVals = grp._2

      var (pubs, soc, seo, kps, views) = (0l, 0l, 0l, 0l, 0l)

      for ((k, v) <- keyVals) k.metricType match {
        case StandardMetricType.publishes => pubs = v
        case StandardMetricType.socialReferrers => soc = v
        case StandardMetricType.searchReferrers => seo = v
        case StandardMetricType.keyPageReferrers => kps = v
        case StandardMetricType.views => views = v
        case _ =>
      }

      dm -> StandardMetrics(views, soc, seo, kps, pubs)
    })
  }

  def mapNewToOldHourly(metrics: Map[StandardMetricsHourlyKey, Long]): Map[DateHour, StandardMetrics] = {
    metrics.toSeq.groupBy(_._1.dateHour).map(grp => {
      val dh = grp._1
      val keyVals = grp._2

      var (pubs, soc, seo, kps, views) = (0l, 0l, 0l, 0l, 0l)

      for ((k, v) <- keyVals) k.metricType match {
        case StandardMetricType.publishes => pubs = v
        case StandardMetricType.socialReferrers => soc = v
        case StandardMetricType.searchReferrers => seo = v
        case StandardMetricType.keyPageReferrers => kps = v
        case StandardMetricType.views => views = v
        case _ =>
      }

      dh -> StandardMetrics(views, soc, seo, kps, pubs)
    })
  }

  def sumNewMaps(maps: Map[StandardMetricsDailyKey, Long]*): Map[StandardMetricsDailyKey, Long] = sumMaps(maps: _*)

  def sumNewHourlyMaps(maps: Map[StandardMetricsHourlyKey, Long]*): Map[StandardMetricsHourlyKey, Long] = sumMaps(maps: _*)

  def sumMaps[K](maps: Map[K, Long]*): Map[K, Long] = {
    val sum = mutable.Map[K, Long]()
    maps.flatten.foreach {
      case (key, v: Long) =>
        val value = sum.get(key) match {
          case Some(l) => l + v
          case None => v
        }

        sum(key) = value
    }

    sum.toMap
  }
}

case class StandardMetrics(views: Long, socialReferrers: Long, searchReferrers: Long, keyPageReferrers: Long, publishes: Long) extends AggregableMetrics {
  def +(that: StandardMetrics): StandardMetrics = StandardMetrics(
    this.views + that.views,
    this.socialReferrers + that.socialReferrers,
    this.searchReferrers + that.searchReferrers,
    this.keyPageReferrers + that.keyPageReferrers,
    this.publishes + that.publishes
  )

  def +(metricsMap: Map[StandardMetricType.Type, Long]): StandardMetrics = this.+(StandardMetrics.buildFromMap(metricsMap))

  def -(that: StandardMetrics): StandardMetrics = StandardMetrics(
    this.views - that.views,
    this.socialReferrers - that.socialReferrers,
    this.searchReferrers - that.searchReferrers,
    this.keyPageReferrers - that.keyPageReferrers,
    this.publishes - that.publishes
  )

  def -(metricsMap: Map[StandardMetricType.Type, Long]): StandardMetrics = this.-(StandardMetrics.buildFromMap(metricsMap))

  def /(amount: Int): StandardMetrics = if (amount == 0) {
    StandardMetrics.empty
  } else {
    StandardMetrics(
      views / amount,
      socialReferrers / amount,
      searchReferrers / amount,
      keyPageReferrers / amount,
      publishes / amount
    )
  }

  def setPublishes(pubs: Long): StandardMetrics = StandardMetrics(
    this.views,
    this.socialReferrers,
    this.searchReferrers,
    this.keyPageReferrers,
    pubs
  )

  def isEmpty: Boolean = views == 0 && socialReferrers == 0 && searchReferrers == 0 && keyPageReferrers == 0 && publishes == 0

  lazy val kpPercent: Double = grvmath.percentOf(keyPageReferrers, views)

  lazy val keyPageMetrics: StandardMetrics = StandardMetrics(
    grvmath.roundToLong(views * kpPercent),
    grvmath.roundToLong(socialReferrers * kpPercent),
    grvmath.roundToLong(searchReferrers * kpPercent),
    keyPageReferrers,
    grvmath.roundToLong(publishes * kpPercent)
  )

  lazy val nonKeyPageMetrics: StandardMetrics = this - keyPageMetrics

  lazy val metricsSeq: scala.Seq[(StandardMetricType.Value, Long)] = Seq(
    StandardMetricType.views -> views,
    StandardMetricType.socialReferrers -> socialReferrers,
    StandardMetricType.searchReferrers -> searchReferrers,
    StandardMetricType.keyPageReferrers -> keyPageReferrers,
    StandardMetricType.publishes -> publishes
  )

  def toDailyMap(dm: GrvDateMidnight): Map[StandardMetricsDailyKey, Long] = StandardMetrics.mapOldToNewDaily(Map(dm -> this))
  def toHourlyMap(dh: DateHour): Map[StandardMetricsHourlyKey, Long] = StandardMetrics.mapOldToNewHourly(Map(dh -> this))

  override def toString: String = {
    val fmt = "StandardMetrics {%n\tviews: %d%n\tsocial: %d%n\tsearch: %d%n\tkeyPage: %d%n\tpublishes: %d%n}%n"
    fmt.format(views, socialReferrers, searchReferrers, keyPageReferrers, publishes)
  }
}

case class ViralMetrics(tweets: Long = 0, retweets: Long = 0, facebookClicks: Long = 0, facebookLikes: Long = 0, facebookShares: Long = 0, influencers: Long = 0, stumbleUponShares: Long = 0, facebookComments: Long = 0, deliciousShares: Long = 0, redditShares: Long = 0, googlePlusOneShares: Long = 0, diggs: Long = 0, pinterestShares:Long = 0,linkedInShares: Long = 0, facebookTotals: Long = 0, maleTotals: Long = 0, femaleTotals: Long = 0, mostlyFemaleTotals: Long = 0, mostlyMaleTotals: Long = 0, unisexTotals: Long = 0) extends AggregableMetrics {
  val total: Long = tweets + retweets + facebookClicks + facebookLikes + facebookShares + influencers + facebookComments + deliciousShares + redditShares + googlePlusOneShares + diggs + pinterestShares + linkedInShares

  def +(that: ViralMetrics): ViralMetrics = ViralMetrics(
    this.tweets + that.tweets,
    this.retweets + that.retweets,
    this.facebookClicks + that.facebookClicks,
    this.facebookLikes + that.facebookLikes,
    this.facebookShares + that.facebookShares,
    this.influencers + that.influencers,
    this.stumbleUponShares + that.stumbleUponShares,
    this.facebookComments + that.facebookComments,
    this.deliciousShares + that.deliciousShares,
    this.redditShares + that.redditShares,
    this.googlePlusOneShares + that.googlePlusOneShares,
    this.diggs + that.diggs,
    this.pinterestShares + that.pinterestShares,
    this.linkedInShares + that.linkedInShares,
    this.facebookTotals + that.facebookTotals,
    this.maleTotals + that.maleTotals,
    this.mostlyMaleTotals + that.mostlyMaleTotals,
    this.femaleTotals + that.femaleTotals,
    this.mostlyFemaleTotals + that.mostlyFemaleTotals,
    this.unisexTotals + that.unisexTotals
  )

  //for APIs that only tell us total metrics and not
  //metrics by time, lets us calculate difference
  def -(that: ViralMetrics): ViralMetrics = ViralMetrics(
    if ((this.tweets - that.tweets) > 0) this.tweets - that.tweets else 0,
    if ((this.retweets - that.retweets) > 0) this.retweets - that.retweets else 0,
    if ((this.facebookClicks - that.facebookClicks) > 0) this.facebookClicks - that.facebookClicks else 0,
    if ((this.facebookLikes - that.facebookLikes) > 0) this.facebookLikes - that.facebookLikes else 0,
    if ((this.facebookShares - that.facebookShares) > 0) this.facebookShares - that.facebookShares else 0,
    if ((this.influencers - that.influencers) > 0) this.influencers - that.influencers else 0,
    if ((this.stumbleUponShares - that.stumbleUponShares) > 0) this.stumbleUponShares - that.stumbleUponShares else 0,
    if ((this.facebookComments - that.facebookComments) > 0) this.facebookComments - that.facebookComments else 0,
    if ((this.deliciousShares - that.deliciousShares) > 0) this.deliciousShares - that.deliciousShares else 0,
    if ((this.redditShares - that.redditShares) > 0) this.redditShares - that.redditShares else 0,
    if ((this.googlePlusOneShares - that.googlePlusOneShares) > 0) this.googlePlusOneShares - that.googlePlusOneShares else 0,
    if ((this.diggs - that.diggs) > 0) this.diggs - that.diggs else 0,
    if ((this.pinterestShares - that.pinterestShares) > 0) this.pinterestShares - that.pinterestShares else 0,
    if ((this.linkedInShares - that.linkedInShares) > 0) this.linkedInShares - that.linkedInShares else 0,
    if ((this.facebookTotals - that.facebookTotals) > 0) this.facebookTotals - that.facebookTotals else 0,
    if ((this.maleTotals - that.maleTotals) > 0) this.maleTotals - that.maleTotals else 0,
    if ((this.mostlyMaleTotals - that.mostlyMaleTotals) > 0) this.mostlyMaleTotals - that.mostlyMaleTotals else 0,
    if ((this.femaleTotals - that.femaleTotals) > 0) this.femaleTotals - that.femaleTotals else 0,
    if ((this.mostlyFemaleTotals - that.mostlyFemaleTotals) > 0) this.mostlyFemaleTotals - that.mostlyFemaleTotals else 0,
    if ((this.unisexTotals - that.unisexTotals) > 0) this.unisexTotals - that.unisexTotals else 0
  )

  //lets us estimate hourly metrics by dividing
  //total metrics changed by hours since last checked
  def /(that: Double): ViralMetrics = ViralMetrics(
    math.round(this.tweets / that),
    math.round(this.retweets / that),
    math.round(this.facebookClicks / that),
    math.round(this.facebookLikes / that),
    math.round(this.facebookShares / that),
    math.round(this.influencers / that),
    math.round(this.stumbleUponShares / that),
    math.round(this.facebookComments / that),
    math.round(this.deliciousShares / that),
    math.round(this.redditShares / that),
    math.round(this.googlePlusOneShares / that),
    math.round(this.diggs / that),
    math.round(this.pinterestShares / that),
    math.round(this.linkedInShares / that),
    math.round(this.facebookTotals / that),
    math.round(this.maleTotals / that),
    math.round(this.femaleTotals / that),
    math.round(this.mostlyMaleTotals / that),
    math.round(this.mostlyFemaleTotals / that),
    math.round(this.unisexTotals / that)

  )

  //compare two viralMetrics objects and return the largest value for each type
  def getLargest(that: ViralMetrics): ViralMetrics = ViralMetrics(
    if (this.tweets > that.tweets) this.tweets else that.tweets,
    if (this.retweets > that.retweets) this.retweets else that.retweets,
    if (this.facebookClicks > that.facebookClicks) this.facebookClicks else that.facebookClicks,
    if (this.facebookLikes > that.facebookLikes) this.facebookLikes else that.facebookLikes,
    if (this.facebookShares > that.facebookShares) this.facebookShares else that.facebookShares,
    if (this.influencers > that.influencers) this.influencers else that.influencers,
    if (this.stumbleUponShares > that.stumbleUponShares) this.stumbleUponShares else that.stumbleUponShares,
    if (this.facebookComments > that.facebookComments) this.facebookComments else that.facebookComments,
    if (this.deliciousShares > that.deliciousShares) this.deliciousShares else that.deliciousShares,
    if (this.redditShares > that.redditShares) this.redditShares else that.redditShares,
    if (this.googlePlusOneShares > that.googlePlusOneShares) this.googlePlusOneShares else that.googlePlusOneShares,
    if (this.diggs > that.diggs) this.diggs else that.diggs,
    if (this.pinterestShares > that.pinterestShares) this.pinterestShares else that.pinterestShares,
    if (this.linkedInShares > that.linkedInShares) this.linkedInShares else that.linkedInShares,
    if (this.facebookTotals > that.facebookTotals) this.facebookTotals else that.facebookTotals
  )

  def isEmpty: Boolean = total < 1

  // value from [-1 to 1], where -1 is mostly male, 1 is mostly female
  def getGenderWeight: Float = {

    if (facebookShares == 0) return 0.0f

    (maleTotals * -1.0f +
     mostlyMaleTotals * -0.5f +
     femaleTotals * 1.0f +
     mostlyFemaleTotals * 0.5f) / facebookShares
  }
}

object ViralMetrics {
  val empty: ViralMetrics = ViralMetrics()

  implicit val ViralMetricsSemigroup: Semigroup[ViralMetrics] = new Semigroup[ViralMetrics] {
    def append(a: ViralMetrics, b: => ViralMetrics): ViralMetrics = a + b
  }

  implicit val ViralMetricsMonoid: Monoid[ViralMetrics] = Monoid.instance(ViralMetricsSemigroup.append, empty)

  implicit object CanAggregate extends AggregableMetrics.CanTotal[ViralMetrics, Long] {
    override def total(metrics: ViralMetrics*): Long = metrics.map(_.total).sum
  }

}

case class ScopedMetrics(impressions:Long, clicks:Long, views:Long, spend:Long, failedImpressions:Long) {
  def CTR: Double = grvmetrics.safeCTR(clicks, impressions)

  /**
   * Think of the metrics as a test.  How many impressions happened that did not result in clicks?
   * @param useViews If you want to use views instead of impressions
   * @return Will not return a number below zero (in the case that the numbers are clearly incorrect)
   */
  def losses(useViews:Boolean=false): Long = (impressionsOrViews(useViews) - clicks) max 0

  /**
   * Do you want impressions or views?  Saves a bunch of if statements that break readability in places.
   */
  def impressionsOrViews(useViews:Boolean = false): Long = if(useViews) views else impressions
  /**
   * Overload that returns CTR based on whether or not you want it to be view-based or impression-based.
   * @param useViews Whether or not you want view based.
   * @return CTR
   */
  def CTR(useViews:Boolean=false): Double = if(useViews) viewsCTR else CTR

  def viewsCTR: Double = grvmetrics.safeCTR(clicks, views)

  def +(that: ScopedMetrics) : ScopedMetrics = ScopedMetrics(
    this.impressions + that.impressions,
    this.clicks + that.clicks,
    this.views + that.views,
    this.spend + that.spend,
    this.failedImpressions + that.failedImpressions
  )

  def ecpm: Double = if(clicks > 0 && impressions > 0) spend / (impressions.toDouble / 1000.toDouble) else 0.0
  def viewECPM: Double = if(clicks > 0 && views > 0) spend / (views.toDouble / 1000.toDouble) else 0.0

  def estimatedSpend(cpc:Long): Long = clicks * cpc
  def estimatedECPM(cpc:Long): Double = if(clicks > 0 && impressions > 0) estimatedSpend(cpc) / (impressions.toDouble / 1000.toDouble) else 0.0
  def estimatedViewECPM(cpc:Long): Double = if(clicks > 0 && views > 0) estimatedSpend(cpc) / (views.toDouble / 1000.toDouble) else 0.0

  def failureRate: Double = grvmetrics.safeCTR(failedImpressions, failedImpressions+impressions)
}

object ScopedMetrics {
  val empty: ScopedMetrics = ScopedMetrics(0,0,0,0,0)
  //val bad: ScopedMetrics = ScopedMetrics(-999,-999,-999,-999,-999)
  implicit object ScopedMetricsMonoid extends Monoid[ScopedMetrics] {
    def append(s1: ScopedMetrics, s2: => ScopedMetrics): ScopedMetrics = s1 + s2

    val zero: ScopedMetrics = empty
  }
}

class RecommendationMetrics(val articleImpressions: Long, val clicks: Long, val unitImpressions: Long, val organicClicks: Long,
                            val sponsoredClicks: Long, val unitImpressionsViewed: Long, val unitImpressionsControl: Long,
                            val unitImpressionsNonControl: Long, val organicClicksControl: Long, val organicClicksNonControl: Long,
                            val conversions: Long) extends AggregableMetrics {
  def articleClickThroughRate: Double = if (articleImpressions != 0) clicks.toDouble / articleImpressions.toDouble else 0.0
  def unitClickThroughRate: Double = if (unitImpressions != 0) clicks.toDouble / unitImpressions.toDouble else 0.0
  def unitViewedClickThroughRate: Double = if(unitImpressionsViewed != 0) clicks.toDouble / unitImpressionsViewed.toDouble else 0.0
  def unitImpressionsClickThroughRate: Double = if(unitImpressionsViewed != 0) clicks.toDouble / unitImpressionsViewed.toDouble else 0.0
  def organicClickThroughRate: Double = if (unitImpressions != 0) organicClicks.toDouble / unitImpressions.toDouble else 0.0
  def organicClickThroughRateViewed: Double = if (unitImpressionsViewed != 0) organicClicks.toDouble / unitImpressionsViewed.toDouble else 0.0
  def organicClickThroughRateControl: Double = if (unitImpressionsControl != 0) organicClicksControl.toDouble / unitImpressionsControl else 0.0
  def organicClickThroughRateNonControl: Double = if (unitImpressionsNonControl != 0) organicClicksNonControl.toDouble / unitImpressionsNonControl else 0.0
  def sponsoredClickThroughRate: Double = if (unitImpressions != 0) sponsoredClicks.toDouble / unitImpressions.toDouble else 0.0
  def sponsoredClickThroughRateViewed: Double = if (unitImpressionsViewed != 0) sponsoredClicks.toDouble / unitImpressionsViewed.toDouble else 0.0

  def estimatedSpend(cpc:Long): Long = clicks * cpc
  def estimatedECPM(cpc:Long): Double = if(clicks > 0 && unitImpressions > 0) estimatedSpend(cpc) / (unitImpressions.toDouble / 1000.toDouble) else 0.0

  def +(that: RecommendationMetrics): RecommendationMetrics = RecommendationMetrics(
    this.articleImpressions + that.articleImpressions,
    this.clicks + that.clicks,
    this.unitImpressions + that.unitImpressions,
    this.organicClicks + that.organicClicks,
    this.sponsoredClicks + that.sponsoredClicks,
    this.unitImpressionsViewed + that.unitImpressionsViewed,
    this.unitImpressionsControl + that.unitImpressionsControl,
    this.unitImpressionsNonControl + that.unitImpressionsNonControl,
    this.organicClicksControl + that.organicClicksControl,
    this.organicClicksNonControl + that.organicClicksNonControl,
    this.conversions + that.conversions
  )

  def +(that: RecoReportMetrics): RecommendationMetrics = RecommendationMetrics(
    this.articleImpressions,
    this.clicks + that.totalClicks,
    this.unitImpressions + that.unitImpressions,
    this.organicClicks + that.organicClicks,
    this.sponsoredClicks + that.sponsoredClicks,
    this.unitImpressionsViewed + that.unitImpressionsViewed,
    this.unitImpressionsControl + that.unitImpressionsControl,
    this.unitImpressionsNonControl + that.unitImpressionsNonControl,
    this.organicClicksControl + that.organicClicksControl,
    this.organicClicksNonControl + that.organicClicksNonControl,
    this.conversions
  )

  def withRollups(rollups: RecommendationMetrics): RecommendationMetrics = new RecommendationMetrics(
    articleImpressions,
    rollups.clicks,
    rollups.unitImpressions,
    organicClicks,
    sponsoredClicks,
    rollups.unitImpressionsViewed,
    rollups.unitImpressionsControl,
    rollups.unitImpressionsNonControl,
    rollups.organicClicksControl,
    rollups.organicClicksNonControl,
    conversions
  )

  def adjustByPercentage(percent:Double) : RecommendationMetrics = {
    val multiplier = 1.0 / percent
    println("Adj: " + multiplier)
    println("Previous" + this)
    val res = RecommendationMetrics(
      (this.articleImpressions * multiplier).toLong,
      (this.clicks * multiplier).toLong,
      (this.unitImpressions * multiplier).toLong,
      (this.organicClicks * multiplier).toLong,
      (this.sponsoredClicks * multiplier).toLong,
      (this.unitImpressionsViewed * multiplier).toLong,
      (this.unitImpressionsControl * multiplier).toLong,
      (this.unitImpressionsNonControl * multiplier).toLong,
      (this.organicClicksControl * multiplier).toLong,
      (this.organicClicksNonControl * multiplier).toLong,
      (this.conversions * multiplier).toLong
    )
    println("Adjusted" + res)
    res
  }

  def -(that: RecommendationMetrics): RecommendationMetrics = RecommendationMetrics(
    this.articleImpressions - that.articleImpressions,
    this.clicks - that.clicks,
    this.unitImpressions - that.unitImpressions,
    this.organicClicks - that.organicClicks,
    this.sponsoredClicks - that.sponsoredClicks,
    this.unitImpressionsViewed - that.unitImpressionsViewed,
    this.unitImpressionsControl - that.unitImpressionsControl,
    this.unitImpressionsNonControl - that.unitImpressionsNonControl,
    this.organicClicksControl - that.organicClicksControl,
    this.organicClicksNonControl - that.organicClicksNonControl,
    this.conversions - that.conversions
  )

  override def hashCode(): Int = {
    HashUtils.generateHashCode(
      HashUtils.longBytes(articleImpressions),
      HashUtils.longBytes(clicks),
      HashUtils.longBytes(unitImpressions),
      HashUtils.longBytes(organicClicks),
      HashUtils.longBytes(sponsoredClicks),
      HashUtils.longBytes(unitImpressionsViewed),
      HashUtils.longBytes(unitImpressionsControl),
      HashUtils.longBytes(unitImpressionsNonControl),
      HashUtils.longBytes(organicClicksControl),
      HashUtils.longBytes(organicClicksNonControl),
      HashUtils.longBytes(conversions)
    )
  }


  override def equals(obj: Any): Boolean = obj match {
    case that: RecommendationMetrics =>
      this.articleImpressions == that.articleImpressions &&
        this.clicks == that.clicks &&
        this.unitImpressions == that.unitImpressions &&
        this.unitImpressionsViewed == that.unitImpressionsViewed &&
        this.organicClicks == that.organicClicks &&
        this.sponsoredClicks == that.sponsoredClicks &&
        this.unitImpressionsControl == that.unitImpressionsControl &&
        this.unitImpressionsNonControl == that.unitImpressionsNonControl &&
        this.organicClicksControl == that.organicClicksControl &&
        this.organicClicksNonControl == that.organicClicksNonControl &&
        this.conversions == that.conversions

    case sumthinElse => false
  }

  override lazy val toString: String = {
    "recoMetrics = { articleImpressions: " + articleImpressions +
      ", unitImpressions: " + unitImpressions +
      ", unitImpressionsControl: " + unitImpressionsControl +
      ", unitImpressionsNonControl: " + unitImpressionsNonControl +
      ", organicClicksControl: " + organicClicksControl +
      ", organicClicksNonControl: " + organicClicksNonControl +
      ", unitImpressionsViewed: " + unitImpressionsViewed +
      ", clicks: " + clicks +
      ", countPerSecond(counterCategory, article): " + articleClickThroughRate.formatted("%.4f") +
      ", countPerSecond(counterCategory, unit): " + unitClickThroughRate.formatted("%.4f") +
      ", ctr-viewed(unit):" + unitViewedClickThroughRate.formatted("%.4f") +
      ", conversions: " + conversions +
      " }"
  }
}

object RecommendationMetrics {

  val empty: RecommendationMetrics = RecommendationMetrics(0, 0)

  implicit val RecommendationMetricsSemigroup: Semigroup[RecommendationMetrics] = new Semigroup[RecommendationMetrics] {
    def append(f1: RecommendationMetrics, f2: => RecommendationMetrics): RecommendationMetrics = f1 + f2
  }

  implicit val RecommendationMetricsMonoid: Monoid[RecommendationMetrics] = Monoid.instance(RecommendationMetricsSemigroup.append, empty)

  def apply(articleImpressions: Long, clicks: Long, unitImpressions: Long = 0l, organicClicks: Long = 0l,
            sponsoredClicks: Long = 0l, unitImpressionsViewed: Long = 0l, unitImpressionsControl: Long = 0l,
            unitImpressionsNonControl: Long = 0l, organicClicksControl: Long = 0l, organicClicksNonControl: Long = 0l,
            conversions: Long = 0l): RecommendationMetrics = new RecommendationMetrics(
    articleImpressions, clicks, unitImpressions, organicClicks, sponsoredClicks, unitImpressionsViewed,
    unitImpressionsControl, unitImpressionsNonControl, organicClicksControl, organicClicksNonControl, conversions
  )

  def fromCountByType(countBy:Byte, value:Long): RecommendationMetrics = {
    countBy match {
      case RecommendationMetricCountBy.click => RecommendationMetrics(0l, value, 0l)
      case RecommendationMetricCountBy.articleImpression => RecommendationMetrics(value, 0l, 0l)
      case RecommendationMetricCountBy.unitImpression => RecommendationMetrics(0l, 0l, value)
      case RecommendationMetricCountBy.unitImpressionViewed => RecommendationMetrics(0l, 0l, 0l, 0l, 0l, value)
      case RecommendationMetricCountBy.conversion => RecommendationMetrics(0l, 0l, conversions = value)
      case _ => RecommendationMetrics.empty
    }
  }
}

trait CampaignDashboardMetrics {
  def impressions: Long
  def impressionsViewed: Long
  def clicks: Long
  def spent: Long
  def conversions: Long = 0l
  def +(that: CampaignDashboardMetrics): CampaignDashboardMetrics

  def isEmpty: Boolean = impressions == 0l && impressionsViewed == 0l && clicks == 0l && spent == 0l
  def nonEmpty: Boolean = !isEmpty

  def averageCpc: Long = if (clicks > 0) math.round(spent.toDouble / clicks) else 0l
  def ctr: Double = if (impressions > 0) clicks.toDouble / impressions else 0d
  def ctrViewed: Double = if (impressionsViewed > 0) clicks.toDouble / impressionsViewed else 0d
}

object CampaignDashboardMetrics {
  val empty: CampaignDashboardMetrics = new CampaignDashboardMetrics {
    def +(that: CampaignDashboardMetrics): CampaignDashboardMetrics = that

    def spent: Long = 0l

    def clicks: Long = 0l

    def impressionsViewed: Long = 0l

    def impressions: Long = 0l

    override def ctrViewed: Double = 0d

    override def ctr: Double = 0d

    override def averageCpc: Long = 0l

    override def isEmpty: Boolean = true

    override def nonEmpty: Boolean = false
  }

  def append(x: CampaignDashboardMetrics, y: => CampaignDashboardMetrics): CampaignDashboardMetrics = x + y

  implicit val campaignDashboardMetricsMonoid: Monoid[CampaignDashboardMetrics] = Monoid.instance(append, empty)
}

trait PluginDashboardMetrics {
  def unitImpressions: Long
  def unitImpressionsControl: Long
  def unitImpressionsNonControl: Long
  def unitImpressionsViewedControl: Long
  def unitImpressionsViewedNonControl: Long
  def organicClicks: Long
  def organicClicksControl: Long
  def organicClicksNonControl: Long
  def sponsoredClicks: Long
  def revenue: Double
  def unitImpressionsViewed: Long
  def totalRevenue: Double
  def +(that: PluginDashboardMetrics): PluginDashboardMetrics

  def isEmpty: Boolean = unitImpressions == 0 && unitImpressionsViewed == 0 && organicClicks == 0 && sponsoredClicks == 0 && revenue == 0.0
  def nonEmpty: Boolean = !isEmpty

  def totalClicks: Long = organicClicks + sponsoredClicks
  def organicCtr: Double = if (unitImpressions > 0) organicClicks / unitImpressions.toDouble else 0.0
  def organicCtrViewed: Double = if (unitImpressionsViewed > 0) organicClicks / unitImpressionsViewed.toDouble else 0.0
  def organicCtrControl: Double = if (unitImpressionsControl > 0) organicClicksControl / unitImpressionsControl.toDouble else 0.0
  def organicCtrNonControl: Double = if (unitImpressionsNonControl > 0) organicClicksNonControl / unitImpressionsNonControl.toDouble else 0.0
  def organicCtrViewedControl: Double = if (unitImpressionsViewedControl > 0) organicClicksControl.toDouble / unitImpressionsViewedControl else 0.0
  def organicCtrViewedNonControl: Double = if (unitImpressionsViewedNonControl > 0) organicClicksNonControl.toDouble / unitImpressionsViewedNonControl else 0.0
  def organicCtrUplift: Option[Double] = grvmath.upliftPercentageOpt(organicCtrControl, organicCtr)
  def sponsoredCtr: Double = if (unitImpressions > 0) sponsoredClicks / unitImpressions.toDouble else 0.0
  def sponsoredCtrViewed: Double = if (unitImpressionsViewed > 0) sponsoredClicks / unitImpressionsViewed.toDouble else 0.0
  def totalCtr: Double = if (unitImpressions > 0) totalClicks / unitImpressions.toDouble else 0.0
  def rpc: Double = if (sponsoredClicks > 0) revenue / sponsoredClicks else 0.0
  def rpm: Double = if (unitImpressions > 0) revenue / (unitImpressions * 0.001) else 0.0
  def rpmViewed: Double = if (unitImpressionsViewed > 0) revenue / (unitImpressionsViewed * 0.001) else 0.0
  def rpmGross: Double = if (unitImpressions > 0) totalRevenue / (unitImpressions * 0.001) else 0.0
}

object PluginDashboardMetrics {
  val empty: PluginDashboardMetrics = new PluginDashboardMetrics {
    def unitImpressions: Long = 0

    def unitImpressionsControl: Long = 0l

    def unitImpressionsNonControl: Long = 0l

    def unitImpressionsViewed: Long = 0

    def organicClicks: Long = 0

    def organicClicksControl: Long = 0

    def organicClicksNonControl: Long = 0

    def sponsoredClicks: Long = 0

    def revenue: Double = 0.0

    def totalRevenue: Double = 0l

    def +(that: PluginDashboardMetrics): PluginDashboardMetrics = that

    override def rpm: Double = 0d

    override def rpmViewed: Double = 0d

    override def rpc: Double = 0d

    override def totalCtr: Double = 0d

    override def sponsoredCtrViewed: Double = 0d

    override def sponsoredCtr: Double = 0d

    override def organicCtrViewed: Double = 0d

    override def organicCtr: Double = 0d

    override def organicCtrControl: Double = 0d

    override def organicCtrNonControl: Double = 0d

    override def organicCtrUplift: Option[Double] = None

    override def totalClicks: Long = 0l

    override def isEmpty: Boolean = true

    override def nonEmpty: Boolean = false

    def unitImpressionsViewedControl: Long = 0L

    def unitImpressionsViewedNonControl: Long = 0L
  }

  implicit val pluginDashboardMetricsSemigroup: Semigroup[PluginDashboardMetrics] with Object {def append(s1: PluginDashboardMetrics, s2: => PluginDashboardMetrics): PluginDashboardMetrics} = new Semigroup[PluginDashboardMetrics] {
    def append(s1: PluginDashboardMetrics, s2: => PluginDashboardMetrics): PluginDashboardMetrics = s1 + s2
  }

  implicit val pluginDashboardMetricsMonoid: Monoid[PluginDashboardMetrics] = Monoid.instance(pluginDashboardMetricsSemigroup.append, empty)
}

/** @note Not wired for impressions/clicks/CTR control vs. non-control breakdown. (Organic wired only at this time.) */
case class SponsoredRecommendationMetrics(override val articleImpressions: Long, override val clicks: Long,
                                          override val unitImpressions: Long, totalSpent: Long, override val organicClicks: Long = 0l,
                                          override val sponsoredClicks: Long = 0l, override val unitImpressionsViewed: Long = 0l,
                                          override val conversions: Long = 0l)
  extends RecommendationMetrics(articleImpressions, clicks, unitImpressions, organicClicks, sponsoredClicks, unitImpressionsViewed, 0l, 0l, 0l, 0l, conversions) with CampaignDashboardMetrics {

  lazy val averageCPC: DollarValue = if (clicks == 0) DollarValue.zero else DollarValue(math.round(totalSpent.toDouble / clicks.toDouble))
  lazy val totalSpentDollars: DollarValue = DollarValue(totalSpent)
  lazy val RPM: Double = if(unitImpressions > 0) totalSpent / (unitImpressions.toDouble / 1000.toDouble) else 0
  lazy val RPMDollars: DollarValue = DollarValue(RPM.toInt)

  def +(that: SponsoredRecommendationMetrics): SponsoredRecommendationMetrics = SponsoredRecommendationMetrics(
    this.articleImpressions + that.articleImpressions,
    this.clicks + that.clicks,
    this.unitImpressions + that.unitImpressions,
    this.totalSpent + that.totalSpent,
    this.organicClicks + that.organicClicks,
    this.sponsoredClicks + that.sponsoredClicks,
    this.unitImpressionsViewed + that.unitImpressionsViewed,
    this.conversions + that.conversions
  )

  def -(that: SponsoredRecommendationMetrics): SponsoredRecommendationMetrics = SponsoredRecommendationMetrics(
    this.articleImpressions - that.articleImpressions,
    this.clicks - that.clicks,
    this.unitImpressions - that.unitImpressions,
    this.totalSpent - that.totalSpent,
    this.organicClicks - that.organicClicks,
    this.sponsoredClicks - that.sponsoredClicks,
    this.unitImpressionsViewed - that.unitImpressionsViewed,
    this.conversions - that.conversions
  )

  override def withRollups(rollups: RecommendationMetrics): SponsoredRecommendationMetrics = new SponsoredRecommendationMetrics(
    articleImpressions,
    rollups.clicks,
    rollups.unitImpressions,
    totalSpent,
    organicClicks,
    sponsoredClicks,
    rollups.unitImpressionsViewed,
    conversions
  )

  def +(that: CampaignDashboardMetrics): CampaignDashboardMetrics = {
    val thatSponsored = SponsoredRecommendationMetrics(
      that.impressions, that.clicks, 0l, that.spent, unitImpressionsViewed = that.impressionsViewed, conversions = that.conversions
    )

    SponsoredRecommendationMetrics(
      this.articleImpressions + thatSponsored.articleImpressions,
      this.clicks + thatSponsored.clicks,
      this.unitImpressions + thatSponsored.unitImpressions,
      this.totalSpent + thatSponsored.totalSpent,
      this.organicClicks + thatSponsored.organicClicks,
      this.sponsoredClicks + thatSponsored.sponsoredClicks,
      this.unitImpressionsViewed + thatSponsored.unitImpressionsViewed,
      this.conversions + thatSponsored.conversions
    ).asInstanceOf[CampaignDashboardMetrics]
  }

  def spent: Long = totalSpent

  def impressionsViewed: Long = unitImpressionsViewed

  def impressions: Long = articleImpressions
}

object SponsoredRecommendationMetrics {
  val empty: SponsoredRecommendationMetrics = SponsoredRecommendationMetrics(0l, 0l, 0l, 0l, 0l, 0l, 0l)

  implicit val SponsoredRecommendationMetricsSemigroup: Semigroup[SponsoredRecommendationMetrics] with Object {def append(a: SponsoredRecommendationMetrics, b: => SponsoredRecommendationMetrics): SponsoredRecommendationMetrics} = new Semigroup[SponsoredRecommendationMetrics]{
    def append(a: SponsoredRecommendationMetrics, b: => SponsoredRecommendationMetrics): SponsoredRecommendationMetrics = a + b
  }

  implicit val SponsoredRecommendationMetricsMonoid: Monoid[SponsoredRecommendationMetrics] = Monoid.instance(SponsoredRecommendationMetricsSemigroup.append, empty)

  def apply(recoMetrics: RecommendationMetrics, totalSpent: DollarValue): SponsoredRecommendationMetrics = SponsoredRecommendationMetrics(
    recoMetrics.articleImpressions,
    recoMetrics.clicks,
    recoMetrics.unitImpressions,
    totalSpent.pennies,
    recoMetrics.organicClicks,
    recoMetrics.sponsoredClicks,
    recoMetrics.unitImpressionsViewed,
    recoMetrics.conversions
  )

  def filterAndGroupWithRollup[K](sponsoredMetrics: Map[SponsoredMetricsKey, Long], rollupMetrics: Map[RollupRecommendationMetricKey, Long], grouper: (MetricsKeyLite) => K, filter: (MetricsKeyLite) => Boolean = s => true)(implicit checker: (CampaignKey) => Boolean): Map[K, SponsoredRecommendationMetrics] = {

//    val groupedSponsored = sponsoredMetrics.filterKeys(filter).groupBy(kv => grouper(kv._1))
//    val groupedRollups = rollupMetrics.filterKeys(filter).groupBy(kv => grouper(kv._1))
    val groupedSponsored = mutable.Map[K, mutable.Map[SponsoredMetricsKey, Long]]()
    val groupedRollups = mutable.Map[K, mutable.Map[RollupRecommendationMetricKey, Long]]()

    for {
      (smk, value) <- sponsoredMetrics
      if filter(smk)
    } {
      val key = grouper(smk)

      groupedSponsored.get(key) match {
        case Some(innerMap) =>
          innerMap.get(smk) match {
            case Some(previousValue) => innerMap.update(smk, previousValue + value)
            case None => innerMap += smk -> value
          }
        case None =>
          val innerMap = mutable.Map[SponsoredMetricsKey, Long]()
          innerMap += smk -> value
          groupedSponsored.update(key, innerMap)
      }
    }

    for {
      (rrmk, value) <- rollupMetrics
      if filter(rrmk)
    } {
      val key = grouper(rrmk)

      groupedRollups.get(key) match {
        case Some(innerMap) =>
          innerMap.get(rrmk) match {
            case Some(previousValue) => innerMap.update(rrmk, previousValue + value)
            case None => innerMap += rrmk -> value
          }
        case None =>
          val innerMap = mutable.Map[RollupRecommendationMetricKey, Long]()
          innerMap += rrmk -> value
          groupedRollups.update(key, innerMap)
      }
    }

    val allKeys = groupedSponsored.keySet ++ groupedRollups.keySet

    if (allKeys.isEmpty) return Map.empty[K, SponsoredRecommendationMetrics]

    (for (key <- allKeys) yield {
      val articleMetrics = groupedSponsored.getOrElse(key, Map.empty[SponsoredMetricsKey, Long]).toSeq.foldLeft(SponsoredRecommendationMetrics.empty) {
        case (rm: SponsoredRecommendationMetrics, kv: (SponsoredMetricsKey, Long)) => rm + kv._1.sponsoredMetrics(kv._2)
      }

      val unitMetrics = groupedRollups.getOrElse(key, Map.empty[RollupRecommendationMetricKey, Long]).toSeq.foldLeft(RecommendationMetrics.empty) {
        case (rm: RecommendationMetrics, kv: (RollupRecommendationMetricKey, Long)) => rm + kv._1.recoMetrics(kv._2)
      }

      key -> articleMetrics.withRollups(unitMetrics)
    }).toMap
  }
}

case class DateMetrics(date: GrvDateMidnight, metrics: StandardMetrics)

case class StandardStatistics(views: Measurements, socialReferrers: Measurements, searchReferrers: Measurements, keyPageReferrers: Measurements, publishes: Measurements)

object StandardStatistics {
  val empty: StandardStatistics = StandardStatistics(Measurements.empty, Measurements.empty, Measurements.empty, Measurements.empty, Measurements.empty)
}

case class InterestCounts(uri: String,
                          var viewCount: Long,
                          var publishedArticles: Long,
                          var socialReferrers: Long,
                          var searchReferrers: Long,
                          var keyPageReferrers: Long,
                          var name: String = "",
                          var viralMetrics: ViralMetrics = ViralMetrics.empty,
                          viralVelocity: Double = 0,
                          visitors: Long = 0l) {

  def isEmpty: Boolean = if (viralMetrics.isEmpty) {
    viewCount != 0 && publishedArticles != 0 && socialReferrers != 0 && searchReferrers != 0 && keyPageReferrers != 0
  } else {
    false
  }
}
