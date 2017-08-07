package com.gravity.domain.grvmetrics

import com.gravity.domain.{Series, SeriesItem, grvmetrics}
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.ScopedMetricsKey
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}
import com.gravity.utilities.web.ApiServlet
import com.gravity.utilities.{BaseScalaTest, grvtime}

import scala.collection._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
class GrvMetricsTest extends BaseScalaTest {
  val thisHour: DateHour = grvtime.currentHour
  val thisHourMillis: Long = thisHour.getMillis
  val lastHour: DateHour = thisHour.minusHours(1)
  val lastHourMillis: Long = lastHour.getMillis

  test("Test Rollup Metrics") {
    val rm = Seq[(RollupRecommendationMetricKey, Long)](
        RollupRecommendationMetricKey(thisHour, SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l,
      RollupRecommendationMetricKey(thisHour.minusHours(1), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l,
      RollupRecommendationMetricKey(thisHour.minusHours(2), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l,
      RollupRecommendationMetricKey(thisHour.minusHours(3), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l,
      RollupRecommendationMetricKey(thisHour.minusHours(4), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l
    )

    val res = rm.asMetricsWithTime[RecommendationMetrics, DateHour].timeSeries
    println(res)
  }

  test("Test Viral Metrics Date Midnight") {
    val sm = Seq[(GrvDateMidnight, ViralMetrics)](
      new GrvDateMidnight() -> ViralMetrics(20, 2, 3, 4, 5, 6),
      new GrvDateMidnight().minusDays(1) -> ViralMetrics(10, 2, 3, 4, 5, 6)
    )

    println(sm.asMetricsWithTime[ViralMetrics, GrvDateMidnight].timeSeries)

    val lastTwoDays = sm.asMetricsWithTime[ViralMetrics,GrvDateMidnight].aggregateByTimeUnits(2,0)

    println("Last two days: " + lastTwoDays)
    val smfx = sm.asMetricsWithTime[ViralMetrics, GrvDateMidnight].linearRegressionFx(1, 0)(_.tweets)

    println(smfx(new GrvDateMidnight().plusDays(1)))

    println("By day of year" + sm.asMetricsWithTime[ViralMetrics,GrvDateMidnight].groupMapgregate((_._1.dayOfYear())))

  }

  test("Test Viral Metrics Date Hour") {
    val sm = Seq[(DateHour, ViralMetrics)](
      grvtime.currentHour -> ViralMetrics(20, 2, 3, 4, 5, 6),
      grvtime.currentHour.minusHours(1) -> ViralMetrics(10, 2, 3, 4, 5, 6)
    )

    println(sm.asMetricsWithTime[ViralMetrics, DateHour].timeSeries)

    val smfx = sm.asMetricsWithTime[ViralMetrics, DateHour].linearRegressionFx(1, 0)(_.tweets)

    println("Coerced into date midnight" + sm.asMetricsWithTime[ViralMetrics, GrvDateMidnight].timeSeries)

    val nextHour = smfx(grvtime.currentHour.plusHours(1))

    println(nextHour)
  }


  test("Test StandardMetrics") {
    val sm = Map[GrvDateMidnight, StandardMetrics](
      new GrvDateMidnight() -> StandardMetrics(20, 2, 3, 4, 5),
      new GrvDateMidnight().minusDays(1) -> StandardMetrics(10, 2, 3, 4, 5)
    )

    println(sm.asMetricsWithTime[StandardMetrics, GrvDateMidnight].timeSeries)

    val smfx = sm.asMetricsWithTime[StandardMetrics, GrvDateMidnight].linearRegressionFx(1, 0)(_.views)

    println(smfx(new GrvDateMidnight().plusDays(1)))
  }

  test("Time Series Projection") {
    val scopedMap = Map[ScopedMetricsKey, Long](
      ScopedMetricsKey(thisHourMillis, RecommendationMetricCountBy.click) -> 1l,
      ScopedMetricsKey(lastHourMillis, RecommendationMetricCountBy.click) -> 2l,
      ScopedMetricsKey(thisHour.minusHours(1).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(2).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(3).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(4).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(5).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(6).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(7).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(8).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(9).getMillis, RecommendationMetricCountBy.click) -> 4l
    )


    val metrics = scopedMap.asMetricsWithTime[RecommendationMetrics, DateHour]

    println(metrics.timeSeriesRangedUnits(5, 0))

    //Create a function that projects clicks into the future.  The arguments mean, "Use the data from 5 hours ago, leading up to now".
    val clickProjectionFx = metrics.linearRegressionFx(5, 0)(_.clicks)
    println(clickProjectionFx(grvtime.currentHour.plusHours(1)))
    println(clickProjectionFx(grvtime.currentHour.plusHours(2)))
    println(clickProjectionFx(grvtime.currentHour.plusHours(3)))
    println(clickProjectionFx(grvtime.currentHour.plusHours(4)))

    //    val clickLocalProjectionFx = metrics.localRegressionFx(9,0)(_.clicks)
    //    println(clickLocalProjectionFx(grvtime.currentHour.plusHours(0)))
    //    println(clickLocalProjectionFx(grvtime.currentHour.plusHours(2)))
    //    println(clickLocalProjectionFx(grvtime.currentHour.plusHours(3)))
    //    println(clickLocalProjectionFx(grvtime.currentHour.plusHours(4)))

    //Create a function that projects clicks into the future by day instead of by hour.
    val metricsByDay = scopedMap.asMetricsWithTime[RecommendationMetrics, GrvDateMidnight]
    val clickProjectionFxByDay = metricsByDay.linearRegressionFx(5, 0)(_.clicks)
    println(clickProjectionFxByDay(new GrvDateMidnight().plusDays(1)))
  }

  test("Time Series Chart") {
    val artMap = Map[ArticleRecommendationMetricKey, Long](
      ArticleRecommendationMetricKey(thisHour, RecommendationMetricCountBy.click, 0, SiteKey("hi"), 0, algoId=1) -> 3l,
      ArticleRecommendationMetricKey(thisHour, RecommendationMetricCountBy.click, 0, SiteKey("hi"), 0,algoId=1) -> 3l,
      ArticleRecommendationMetricKey(thisHour, RecommendationMetricCountBy.unitImpression, 0, SiteKey("hi"), 0,algoId=1) -> 3l,
      ArticleRecommendationMetricKey(thisHour, RecommendationMetricCountBy.unitImpression, 0, SiteKey("hi"), 0,algoId=1) -> 3l,
      ArticleRecommendationMetricKey(lastHour, RecommendationMetricCountBy.click, 0, SiteKey("hi"), 0,algoId=1) -> 3l,
      ArticleRecommendationMetricKey(lastHour, RecommendationMetricCountBy.click, 0, SiteKey("hi"), 0,algoId=1) -> 3l,
      ArticleRecommendationMetricKey(lastHour, RecommendationMetricCountBy.unitImpression, 0, SiteKey("hi"), 0,algoId=1) -> 3l,
      ArticleRecommendationMetricKey(lastHour, RecommendationMetricCountBy.unitImpression, 0, SiteKey("hi"), 0,algoId=1) -> 3l,
      ArticleRecommendationMetricKey(thisHour, RecommendationMetricCountBy.click, 0, SiteKey("hi"), 0, algoId=2) -> 3l,
      ArticleRecommendationMetricKey(thisHour, RecommendationMetricCountBy.click, 0, SiteKey("hi"), 0,algoId=2) -> 3l,
      ArticleRecommendationMetricKey(thisHour, RecommendationMetricCountBy.unitImpression, 0, SiteKey("hi"), 0,algoId=2) -> 3l,
      ArticleRecommendationMetricKey(thisHour, RecommendationMetricCountBy.unitImpression, 0, SiteKey("hi"), 0,algoId=2) -> 3l,
      ArticleRecommendationMetricKey(lastHour, RecommendationMetricCountBy.click, 0, SiteKey("hi"), 0,algoId=2) -> 3l,
      ArticleRecommendationMetricKey(lastHour, RecommendationMetricCountBy.click, 0, SiteKey("hi"), 0,algoId=2) -> 3l,
      ArticleRecommendationMetricKey(lastHour, RecommendationMetricCountBy.unitImpression, 0, SiteKey("hi"), 0,algoId=2) -> 3l,
      ArticleRecommendationMetricKey(lastHour, RecommendationMetricCountBy.unitImpression, 0, SiteKey("hi"), 0,algoId=2) -> 3l
    )

     val res = artMap.asRecoMetrics.groupedTimeSeriesBy(_._1.algoId).map{kv=>
       Series("Algo: " + kv._1, None, {
         kv._2.map{timeseries=>
           SeriesItem(timeseries._1.getMillis,timeseries._2.unitImpressions,None)
         }.toSeq
       })
     }.toSeq

    println(ApiServlet.serializeToJson(res, true))

    println(res)
  }


  test("Metrical Basics") {

    val artMap = Map[ArticleRecommendationMetricKey, Long](
      ArticleRecommendationMetricKey(thisHour, RecommendationMetricCountBy.click, 0, SiteKey("hi"), 0) -> 3l,
      ArticleRecommendationMetricKey(thisHour, RecommendationMetricCountBy.click, 0, SiteKey("hi"), 0) -> 3l,
      ArticleRecommendationMetricKey(thisHour, RecommendationMetricCountBy.unitImpression, 0, SiteKey("hi"), 0) -> 3l,
      ArticleRecommendationMetricKey(thisHour, RecommendationMetricCountBy.unitImpression, 0, SiteKey("hi"), 0) -> 3l
    )

    val scopedMap = Map[ScopedMetricsKey, Long](
      ScopedMetricsKey(lastHourMillis, RecommendationMetricCountBy.click) -> 3l,
      ScopedMetricsKey(lastHourMillis, RecommendationMetricCountBy.click) -> 3l,
      ScopedMetricsKey(lastHourMillis, RecommendationMetricCountBy.click) -> 3l,
      ScopedMetricsKey(thisHourMillis, RecommendationMetricCountBy.click) -> 3l,
      ScopedMetricsKey(thisHourMillis, RecommendationMetricCountBy.click) -> 3l,
      ScopedMetricsKey(thisHourMillis, RecommendationMetricCountBy.click) -> 3l
    )

    val campaignMap = Map[CampaignMetricsKey, Long](
      CampaignMetricsKey(lastHour, RecommendationMetricCountBy.click, 3, SiteKey("hi"), 0, 0, 0, DollarValue(10), 0) -> 1l,
      CampaignMetricsKey(thisHour, RecommendationMetricCountBy.click, 3, SiteKey("hi"), 0, 0, 0, DollarValue(10), 0) -> 1l,
      CampaignMetricsKey(lastHour.minusHours(2), RecommendationMetricCountBy.click, 3, SiteKey("hi"), 0, 0, 0, DollarValue(10), 0) -> 1l,
      CampaignMetricsKey(lastHour.minusHours(3), RecommendationMetricCountBy.click, 3, SiteKey("hi"), 0, 0, 0, DollarValue(10), 0) -> 1l,
      CampaignMetricsKey(lastHour.minusHours(4), RecommendationMetricCountBy.click, 3, SiteKey("hi"), 0, 0, 0, DollarValue(10), 0) -> 1l,
      CampaignMetricsKey(lastHour.minusHours(5), RecommendationMetricCountBy.click, 3, SiteKey("hi"), 0, 0, 0, DollarValue(10), 0) -> 1l
    )

    //    val sponsoredMap = Map[SponsoredMetricsKey, Long](
    //    )


    val result = artMap.asMetricsWithTime[RecommendationMetrics,Long].aggregateAll
    println(result)

    val result2 = scopedMap.asMetricsWithTime[RecommendationMetrics,Long].aggregateAll
    println(result2)

//    val campaignAggregation = campaignMap.asMetricsWithTime[RecommendationMetrics,Long].aggregateAll
//    println(campaignAggregation)

    val campaignSponsoredAggregation = campaignMap.asMetrics[RecommendationMetrics].aggregateAll
    println(campaignSponsoredAggregation)


    val timeSeries1 = scopedMap.asMetricsWithTime[RecommendationMetrics, Long].timeSeriesMap

    println(timeSeries1)

    val timeSeries2 = scopedMap.asMetricsWithTime[RecommendationMetrics, DateHour].timeSeriesMap

    println(timeSeries2)
    val timeSeries3 = scopedMap.asMetricsWithTime[RecommendationMetrics, GrvDateMidnight].timeSeriesMap

    println(timeSeries3)

    val timeSeries4WithEmpty = scopedMap.asMetricsWithTime[RecommendationMetrics, GrvDateMidnight].timeSeriesMapWithEmptyDefault


    println(timeSeries4WithEmpty(new GrvDateMidnight().minusDays(300)))

    val timeSeries5Range = scopedMap.asMetricsWithTime[RecommendationMetrics, GrvDateMidnight].timeSeriesRanged(grvtime.currentDay.minusDays(5), grvtime.currentDay)

    println(timeSeries5Range)
    val timeSeries6Range = scopedMap.asMetricsWithTime[RecommendationMetrics, GrvDateMidnight].timeSeriesRangedUnits(5, 0)
    println(timeSeries6Range)

    val future7Range = scopedMap.asMetricsWithTime[RecommendationMetrics, DateHour].timeSeriesRangedUnits(5, -10)
    println(future7Range)
    //    val timeSeries2 = scopedMap.orderedTimeSeries[RecommendationMetrics,Long]

    //    println(timeSeries2)

  }


  test("Transformations") {
    import grvmetrics._

    val thisHour = grvtime.currentHour
    val lastHour = grvtime.currentHour.minusHours(1)
    val rollupMetrics = Seq[(RollupRecommendationMetricKey, Long)](
      RollupRecommendationMetricKey(thisHour, SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l,
      RollupRecommendationMetricKey(thisHour.minusHours(1), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l,
      RollupRecommendationMetricKey(thisHour.minusHours(2), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l,
      RollupRecommendationMetricKey(thisHour.minusHours(3), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l,
      RollupRecommendationMetricKey(thisHour.minusHours(4), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l
    )

    val rollupMetricsTimeseries = rollupMetrics.asMetricsWithTime[RecommendationMetrics, DateHour].timeSeries

    val rollupMetricsByDay = rollupMetrics.asMetricsWithTime[RecommendationMetrics,GrvDateMidnight].timeSeries
  }

  test("Projections") {
    import grvmetrics._

    val thisHour = grvtime.currentHour
    val lastHour = grvtime.currentHour.minusHours(1)
    val rollupMetrics = Seq[(RollupRecommendationMetricKey, Long)](
      RollupRecommendationMetricKey(thisHour, SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 50l,
      RollupRecommendationMetricKey(thisHour.minusHours(1), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 40l,
      RollupRecommendationMetricKey(thisHour.minusHours(2), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 30l,
      RollupRecommendationMetricKey(thisHour.minusHours(3), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 20l,
      RollupRecommendationMetricKey(thisHour.minusHours(4), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l
    )

    val fx = rollupMetrics.asMetricsWithTime[RecommendationMetrics, DateHour].linearRegressionFx(5,0)(_.clicks)

    println(fx(thisHour.plusHours(1)))

    println(fx(thisHour.plusHours(2)))
    println(fx(thisHour.plusHours(3)))

    val fxByDay  = rollupMetrics.asMetricsWithTime[RecommendationMetrics, GrvDateMidnight].linearRegressionFx(1,0)(_.clicks)

    println(fxByDay(new GrvDateMidnight().plusDays(1)))
  }

  test("Monoid Combinations") {
    import grvmetrics._

    val thisHour = grvtime.currentHour
    val lastHour = grvtime.currentHour.minusHours(1)
    val thisHourMillis = thisHour.getMillis
    val lastHourMillis = lastHour.getMillis
    val rollupMetrics = Seq[(RollupRecommendationMetricKey, Long)](
      RollupRecommendationMetricKey(thisHour, SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l,
      RollupRecommendationMetricKey(thisHour.minusHours(1), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l,
      RollupRecommendationMetricKey(thisHour.minusHours(2), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l,
      RollupRecommendationMetricKey(thisHour.minusHours(3), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l,
      RollupRecommendationMetricKey(thisHour.minusHours(4), SiteKey("hi"),0,0,0,RecommendationMetricCountBy.click) -> 10l
    )


    val scopedMetrics = Map[ScopedMetricsKey, Long](
      ScopedMetricsKey(thisHourMillis, RecommendationMetricCountBy.click) -> 1l,
      ScopedMetricsKey(lastHourMillis, RecommendationMetricCountBy.click) -> 2l,
      ScopedMetricsKey(thisHour.minusHours(1).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(2).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(3).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(4).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(5).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(6).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(7).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(8).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(9).getMillis, RecommendationMetricCountBy.click) -> 4l
    )


    val rollupMetricsTimeseries = rollupMetrics.asMetricsWithTime[RecommendationMetrics, DateHour].timeSeries
    val scopedMetricsTimeSeries = scopedMetrics.asMetricsWithTime[RecommendationMetrics, DateHour].timeSeries




  }

  test("New Metrics Type") {
    case class MyMetrics(clicks:Long, views:Long)

    implicit object MyMetricsMetric extends Metric[MyMetrics] {
      def append(s1: MyMetrics, s2: => MyMetrics): MyMetrics = MyMetrics(s1.clicks + s2.clicks, s1.views + s2.views)

      val zero: MyMetrics = MyMetrics(0,0)
    }

    implicit object MyMetricsFromScopedMetrics extends CanBeMetric[(ScopedMetricsKey, Long), MyMetrics] {
      def toMetric(original: (ScopedMetricsKey, Long)): MyMetrics = MyMetrics(original._2, original._2)
    }
    val thisHour = grvtime.currentHour
    val lastHour = grvtime.currentHour.minusHours(1)
    val thisHourMillis = thisHour.getMillis
    val lastHourMillis = lastHour.getMillis

    val scopedMetrics = Map[ScopedMetricsKey, Long](
      ScopedMetricsKey(thisHourMillis, RecommendationMetricCountBy.click) -> 1l,
      ScopedMetricsKey(lastHourMillis, RecommendationMetricCountBy.click) -> 2l,
      ScopedMetricsKey(thisHour.minusHours(1).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(2).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(3).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(4).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(5).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(6).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(7).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(8).getMillis, RecommendationMetricCountBy.click) -> 4l,
      ScopedMetricsKey(thisHour.minusHours(9).getMillis, RecommendationMetricCountBy.click) -> 4l
    )

    val mymetric1 = MyMetrics(3,6)
    val mymetric2 = MyMetrics(6,7)
    //
    //
    //
    //val scopedMetricsTimeSeries = scopedMetrics.asMetricsWithTime[MyMetrics, DateHour].timeSeries

  }

}
