package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.operations.ArticleDataLiteCache
import com.gravity.utilities.grvcoll._
import com.gravity.hbase.schema.{HRow, HbaseTable}
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.utilities.analytics.{DateMidnightRange, TimeSliceResolution}
import com.gravity.utilities.time.GrvDateMidnight
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import org.joda.time.DateTime

import scala.Predef
import scala.collection._

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


/*
  To express that an entity has articles associated with it.
*/
trait HasArticles[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  lazy val recentArticlesRowTtl: Int = 604800

  val recentArticles: this.Fam[PublishDateAndArticleKey, ArticleKey] = family[PublishDateAndArticleKey, ArticleKey]("rat", rowTtlInSeconds = recentArticlesRowTtl, compressed = true)

  val topSortedArticles: this.Fam[ArticleRangeSortedKey, scala.Seq[ArticleKey]] = family[ArticleRangeSortedKey, Seq[ArticleKey]]("tsa", rowTtlInSeconds = 2592000,compressed = true)

}


trait HasArticlesRow[T <: HbaseTable[T, R, RR] with HasArticles[T, R], R, RR <: HRow[T, R]] {
  this: HRow[T, R] =>
  implicit val conf = HBaseConfProvider.getConf.defaultConf
  import com.gravity.logging.Logging._

  lazy val recentArticlesMaxAge: Option[Int] = Some(8) // 8 days is the default for recent articles

  def reportCreatedOn(resolution: TimeSliceResolution): Option[DateTime] = columnFromFamilyTimestamp(_.topSortedArticles, ArticleRangeSortedKey.getDefaultKey(resolution.range))


  def getTopArticlesAndData(limit: Int, periods: TimeSliceResolution*): Option[Seq[(ArticleAndMetrics, ArticleRow)]] = {
    val ranges = periods.map(_.range)
    getTopArticlesAndDataByRanges(limit, ranges: _*)
  }

  def getTopArticlesAndDataByKey(limit: Int, arsk: ArticleRangeSortedKey): Option[Seq[(ArticleAndMetrics, ArticleRow)]] = {
    topSortedArticleKeys.get(arsk) match {
      case Some(akms) => {
        // the columns are for gilt city
        val baseQry = Schema.Articles.query2.withKeys(akms.toSet).withColumns(_.city, _.deals, _.detailsUrl, _.purchaseUrl, _.tagLine)

        val (qry, useAllMetrics, dms) = if (arsk.metricsInterval != ArticleRangeSortedKey.intervalForAllTime) {
          val dmRange = arsk.metricsRange
          val dhRange = dmRange.toDateHourRange
          val qry = baseQry.withFamilies(_.meta, _.storedGraphs, _.standardMetricsHourlyOld)
            .filter(
              _.or(
                _.betweenColumnKeys(_.standardMetricsHourlyOld, dhRange.fromHour, dhRange.toHour),
                _.allInFamilies(_.text, _.meta, _.storedGraphs)
              )
            )

          (qry, false, dmRange.daysWithin)
        } else {
          val qry = baseQry.withFamilies(_.meta, _.storedGraphs, _.standardMetricsHourlyOld)
          (qry, true, Set.empty[GrvDateMidnight])
        }

        val metaMap = qry.executeMap()
        val results = for {
          (ak, a) <- metaMap
          url <- a.column(_.url)
          pubDate <- a.column(_.publishTime)
          m = if (useAllMetrics) a.aggregateMetricsOld else a.aggregateMetricsOldFor(dms)
          category = a.column(_.category).getOrElse("")
          title = a.column(_.title).getOrElse(url)
          summary = a.column(_.summary).getOrElse("")
          image = a.column(_.image).getOrElse("")
        } yield {
          (ArticleAndMetrics(ak, url, title, pubDate, m, category, summary, image), a)

        }

        Some(results.toSeq.sortBy(-_._1.metrics.views).take(limit))
      }
      case None => None
    }

  }


  def getTopArticlesAndDataByRanges(limit: Int, ranges: DateMidnightRange*): Option[Seq[(ArticleAndMetrics, ArticleRow)]] = {
    val rangeSortKeys = ranges.map(r => ArticleRangeSortedKey.getDefaultKey(r))
    val rangeSortKeySet = rangeSortKeys.toSet
    val akms = (for {
      (arsk, aks) <- topSortedArticleKeys
      if (rangeSortKeySet.contains(arsk))
    } yield aks.take(limit)).flatten.toSet

    if (akms.isEmpty) {
      None
    }
    else {
      val dmSet = ranges.flatMap(_.daysWithin).toSet
      val dhSeq = ranges.flatMap(_.hoursWithin)
      val qry = Schema.Articles.query2
          .withKeys(akms)
          .withFamilies(_.meta, _.storedGraphs)
          .withColumns(_.city, _.deals, _.detailsUrl, _.purchaseUrl, _.tagLine)
      dhSeq.foreach(dh => qry.withColumn(_.standardMetricsHourlyOld, dh))
      val metaMap = qry.executeMap()
      val results = for {
        (ak, a) <- metaMap
        url <- a.column(_.url)
        pubDate <- a.column(_.publishTime)
        m = a.aggregateMetricsOldFor(dmSet)
        category = a.column(_.category).getOrElse("")
        title = a.column(_.title).getOrElse(url)
      } yield {
        (ArticleAndMetrics(ak, url, title, pubDate, m, category), a)

      }

      Some(results.toSeq.sortBy(-_._1.metrics.views).take(limit))
    }
  }

  def getTopArticles(limit: Int, periods: TimeSliceResolution*): Option[Seq[ArticleAndMetrics]] = getTopArticlesByRanges(limit, periods.map(_.range): _*)

  def getTopArticlesByRanges(limit: Int, ranges: DateMidnightRange*): Option[Seq[ArticleAndMetrics]] = {
    val rangeSortKeys = ranges.map(r => ArticleRangeSortedKey.getDefaultKey(r))
    val rangeSortKeySet = rangeSortKeys.toSet
    val akms = (for {
      (arsk, aks) <- topSortedArticleKeys
      if (rangeSortKeySet.contains(arsk))
    } yield aks.take(limit)).flatten.toSet

    if (akms.isEmpty) {
      None
    }
    else {
      val dmSet = ranges.flatMap(_.daysWithin).toSet
      val dhSeq = ranges.flatMap(_.hoursWithin)
      val qry = Schema.Articles.query2
          .withKeys(akms)
          .withFamilies(_.meta)
          .withColumns(_.summary, _.city, _.deals, _.detailsUrl, _.purchaseUrl, _.tagLine)
      dhSeq.foreach(dh => qry.withColumn(_.standardMetricsHourlyOld, dh))
      val metaMap = qry.executeMap(skipCache = false, ttl = 120)
      val results = for {
        (ak, a) <- metaMap
        url <- a.column(_.url)
        pubDate <- a.column(_.publishTime)
        m = a.aggregateMetricsOldFor(dmSet)
        category = a.column(_.category).getOrElse("")
        title = a.column(_.title).getOrElse(url)
        summary = a.column(_.summary).getOrElse("")
        image = a.column(_.image).getOrElse("")
      } yield {
        ArticleAndMetrics(ak, url, title, pubDate, m, category, summary, image)
      }

      Some(results.toSeq.sortBy(-_.metrics.views).take(limit))
    }
  }

  lazy val recentArticles: Map[PublishDateAndArticleKey, ArticleKey] = family(_.recentArticles)

  lazy val recentArticlesByDate: Predef.Map[DateTime, ArticleKey] = familyKeySet(_.recentArticles).toMapAlt(_.publishDate, _.articleKey)

  lazy val recentArticleKeys: Predef.Set[ArticleKey] = familyKeySet(_.recentArticles).map(_.articleKey).toSet

  lazy val topSortedArticleKeys: Map[ArticleRangeSortedKey, scala.Seq[ArticleKey]] = family(_.topSortedArticles)

  lazy val topSortedArticles: Map[ArticleRangeSortedKey, scala.Seq[ArticleKey]] = family(_.topSortedArticles)

  def getTopArticles(arsk: ArticleRangeSortedKey, limit: Int = 250): Option[Seq[ArticleKey]] = topSortedArticles.get(arsk) match {
    case Some(aks) => if (aks.isEmpty) None else Some(aks.take(limit))
    case None => None
  }

  def getTopArticlesOrEmpty(arsk: ArticleRangeSortedKey = ArticleRangeSortedKey.getDefaultKey(), limit: Int = 250): scala.Seq[ArticleKey] = getTopArticles(arsk, limit).getOrElse(Seq.empty[ArticleKey])

  lazy val articlesWithMeta: Map[ArticleKey, ArticleRow] = Schema.Articles.query2.withKeys(getTopArticlesOrEmpty().toSet).withFamilies(_.meta).executeMap()

  def mostRecentlyPublishedArticles: Option[scala.Seq[ArticleKey]] = topSortedArticles.get(ArticleRangeSortedKey.mostRecentArticlesKey)


}

trait ArticlePublishMetrics[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  val articlePublishDailyCount: this.Fam[PublishDateAndArticleKey, Long] = family[PublishDateAndArticleKey, Long]("apcbd", rowTtlInSeconds = 60 * 60 * 24 * 31, compressed = true)

}

trait ArticlePublishMetricsRow[T <: HbaseTable[T, R, RR] with ArticlePublishMetrics[T, R], R, RR <: HRow[T, R]] {
  this: HRow[T, R] =>

  lazy val articlePublishDailyCount: Map[PublishDateAndArticleKey, Long] = family(_.articlePublishDailyCount)
}