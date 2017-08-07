package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.utilities.components.FailureResult
import scala.collection._
import com.gravity.interests.jobs.intelligence._
import com.gravity.utilities.time.{GrvDateMidnight, DateHour}
import com.gravity.utilities.grvtime._
import com.gravity.ontology.vocab.{URIType, NS}
import com.gravity.utilities.analytics.{TimeSliceResolution, DateMidnightRange}
import com.gravity.interests.jobs.intelligence.AggregableMetrics.CanTotal
import com.gravity.utilities.grvdates._
import com.gravity.interests.jobs.intelligence.operations.analytics._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scalaz.{Node =>_, _}, Scalaz._


/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object SiteTopicService extends SiteTopicService

object SectionTopicService extends TableOperations[SectionalTopicsTable, SectionTopicKey, SectionalTopicsRow] {
  val table = Schema.SectionalTopics
}

case class ReportInterest2(row: SiteTopicRow, lowerInterests: Seq[ReportInterest2], topics: Seq[ReportTopic2])

case class ReportTopic2(row: SiteTopicRow)

case class ReportNodeOld(node: Node,
                         metrics: StandardMetrics,
                         viralMetrics: ViralMetrics,
                         velocity: Double,
                         visitors: Long,
                         lowerInterests: Seq[ReportNodeOld],
                         topics: Seq[ReportNodeOld])

case class ArticleLink(id: Long, url: String, title: String, publishTime: DateTime, summary: String = "", metrics: StandardMetrics = StandardMetrics.empty)

case class TopSortedArticleKeyAndLimit(key: ArticleRangeSortedKey, limit: Int)


trait SiteTopicService extends SiteTopicOperations {

  import AggregableMetrics._

  def fetchSiteTopicRowsFromGraph(graph: StoredGraph, siteGuid: String)(query: QuerySpec): ValidationNel[FailureResult, Seq[SiteTopicRow]] = {
    val graphKeys = graph.nodes.map {
      node =>
        SiteTopicKey(siteGuid, node.id)
    }.toSet

    for {
      topics <- SiteTopicService.fetchMulti(graphKeys)(query)
    } yield topics.values.toSeq

  }

  def fetchTopArticlesFromSiteTopics(topics: Iterable[SiteTopicRow])(query: ArticleService.QuerySpec) = {
    val articleKeys = topics.flatMap(_.family(_.topSortedArticles).values.flatten).toSet

    ArticleService.fetchMulti(articleKeys)(query)
  }

  def topArticleLinks(siteTopicKeys: Set[SiteTopicKey], cacheTtlSeconds: Int, rangeSortedKeysAndLimits: TopSortedArticleKeyAndLimit*): Validation[FailureResult, Map[SiteTopicKey, Map[ArticleRangeSortedKey, Seq[ArticleLink]]]] = {
    if (siteTopicKeys.isEmpty || rangeSortedKeysAndLimits.isEmpty) {
      val whatsEmpty = if (!rangeSortedKeysAndLimits.isEmpty) {
        "`siteTopicKeys` was empty. No data can be retrieved!"
      } else if (!siteTopicKeys.isEmpty) {
        "`rangeSortedKeysAndLimits` was empty. No data can be retrieved!"
      } else {
        "`siteTopicKeys` and `rangeSortedKeysAndLimits` were empty. No data can be retrieved!"
      }
      return Failure(FailureResult("Required non-empty " + whatsEmpty))
    }

    val rskals = rangeSortedKeysAndLimits.map(_.key)
    val topicQuery = Schema.SiteTopics.query2.withKeys(siteTopicKeys).withColumnsInFamily(_.topSortedArticles, rskals.head, rskals.tail: _*)

    try {
      val skipCache = cacheTtlSeconds < 1

      val topicMap = topicQuery.executeMap(skipCache = skipCache, ttl = cacheTtlSeconds)

      if (topicMap.isEmpty) return Failure(FailureResult("There were no topics found for the `siteTopicKeys` specified!"))

      val topicsToArticleKeys = mutable.Map[SiteTopicKey, mutable.Map[ArticleRangeSortedKey, Seq[ArticleKey]]]()
      topicsToArticleKeys.sizeHint(topicMap.size)

      val articleKeys = for {
        (stKey, stRow) <- topicMap
        TopSortedArticleKeyAndLimit(arsk, limit) <- rangeSortedKeysAndLimits
        stArticleKeys <- stRow.topSortedArticles.get(arsk)
        if (!stArticleKeys.isEmpty)
        desiredKeys = stArticleKeys.take(limit)
        bucket = topicsToArticleKeys.getOrElseUpdate(stKey, mutable.Map[ArticleRangeSortedKey, Seq[ArticleKey]]())
      } yield {
        bucket.put(arsk, desiredKeys)
        desiredKeys
      }

      if (topicsToArticleKeys.isEmpty) return Failure(FailureResult("There were no top sorted article keys found for the `siteTopicKeys` and `rangeSortedKeysAndLimits` specified!"))

      def getOverallMetricsRange: Option[DateMidnightRange] = {
        var min = maxDateMidnight
        var max = minDateMidnight

        for (TopSortedArticleKeyAndLimit(k, _) <- rangeSortedKeysAndLimits) {
          if (k.isMetricsIntervalForAllTime) {
            return None
          }
          if (k.metricsRange.fromInclusive.isBefore(min.getMillis)) {
            min = k.metricsRange.fromInclusive
          }
          if (k.metricsRange.toInclusive.isAfter(max.getMillis)) {
            max = k.metricsRange.toInclusive
          }
        }

        if (min != maxDateMidnight && max != minDateMidnight) Some(DateMidnightRange(min, max)) else None
      }

      val amq = Schema.Articles.query2
        .withKeys(articleKeys.flatten.toSet)
        .withColumns(_.title, _.url, _.publishTime, _.summary)
        .withFamilies(_.standardMetricsHourlyOld)

      getOverallMetricsRange.foreach {
        case dmRange: DateMidnightRange =>
          val dhRange = dmRange.toDateHourRange
          amq.filter(
            _.or(
              _.betweenColumnKeys(_.standardMetricsHourlyOld, dhRange.fromHour, dhRange.toHour),
              _.allInFamilies(_.meta, _.text)
            )
        )
      }

      val articleMap = amq.executeMap(skipCache = skipCache, ttl = cacheTtlSeconds)

      if (articleMap.isEmpty) return Failure(FailureResult("There were no top sorted articles found for the `siteTopicKeys` and `rangeSortedKeysAndLimits` specified!"))

      val resultMap = mutable.Map[SiteTopicKey, mutable.Map[ArticleRangeSortedKey, Seq[ArticleLink]]]()
      resultMap.sizeHint(topicsToArticleKeys.size)

      for {
        stk <- siteTopicKeys
        bucketRead <- topicsToArticleKeys.get(stk)
        TopSortedArticleKeyAndLimit(arsk, _) <- rangeSortedKeysAndLimits
        aks <- bucketRead.get(arsk)
        als = for {
          ak <- aks
          a <- articleMap.get(ak)
          pt <- a.column(_.publishTime)
          sum = a.column(_.summary).getOrElse("")
          m = if (arsk.isMetricsIntervalForAllTime) {
            a.aggregateMetricsOld
          } else {
            a.aggregateMetricsOldBetween(arsk.metricsRange.fromInclusive, arsk.metricsRange.toInclusive)
          }
        } yield ArticleLink(a.rowid.articleId, a.url, a.titleOrUrl, pt, sum, m)
      } {
        val bucketWrite = resultMap.getOrElseUpdate(stk, mutable.Map[ArticleRangeSortedKey, Seq[ArticleLink]]())
        bucketWrite.put(arsk, als)
      }

      Success(resultMap)
    } catch {
      case ex: Exception => Failure(FailureResult(ex))
    }
  }

  def printReport(rpts: Seq[ReportInterest2], recursion: Int = 1) {
    def printr(msg: Any) {
      println(("\t" * recursion) + msg)
    }

    for (r <- rpts) {
      printr("Interest: " + r.row.topicName.get)
      if (r.topics.length > 0) printr("\tTopics: " + r.topics.map(t => t.row.topicName.get).mkString(", "))
      if (r.lowerInterests.length > 0)
        printReport(r.lowerInterests, recursion + 1)
    }
  }

  def printReportInterest(rpts: Seq[ReportInterest], recursion: Int = 1) {
    def printr(msg: Any) {
      println(("\t" * recursion) + msg)
    }

    for (r <- rpts) {
      printr("Interest: " + r.name)
      if (r.topics.length > 0) printr("\tTopics: " + r.topics.map(t => t.name).mkString(", "))
      if (r.lowerInterests.length > 0)
        printReportInterest(r.lowerInterests, recursion + 1)
    }

  }

  def groupedReport(siteTopics: Seq[SiteTopicRow]): Seq[ReportInterest2] = {
    if (siteTopics.isEmpty) return Seq.empty[ReportInterest2]

    val siteTopicsById = siteTopics.map(st => (st.node.id, st)).toMap
    val superGraph = siteTopics.map(_.autoGraph).reduce(_ + _)
    val graphLevels = superGraph.nodesByLevel.keySet.toSeq.sorted

    def report(nodes: Seq[Node]): Seq[ReportInterest2] = {
      nodes.flatMap((node: Node) => siteTopicsById.get(node.id) match {
        case Some(row) => {
          val lowerNodes = superGraph.incomingNodes(node)

          val lowerInterestNodes = lowerNodes.filter(_.nodeType == NodeType.Interest)
          val lowerTopicNodes = lowerNodes.filter(_.nodeType == NodeType.Topic)
          val lowerInterestRows = lowerInterestNodes.flatMap(n => siteTopicsById.get(n.id))
          val lowerTopicRows = lowerTopicNodes.flatMap(n => siteTopicsById.get(n.id))
          if (lowerInterestRows.length > 0) {
            Some(ReportInterest2(row, report(lowerInterestNodes), lowerTopicRows.map(t => ReportTopic2(t))))
          }
          else {
            Some(ReportInterest2(row, Seq(), lowerTopicRows.map(t => ReportTopic2(t))))
          }
        }
        case None => None
      })

    }

    val r = report(superGraph.nodesByLevel(graphLevels.head))
    r
  }

  def processSiteTopicsForReport(period: TimeSliceResolution, siteTopics: Seq[SiteTopicRow]): Seq[ReportNodeOld] = {
    if (siteTopics.isEmpty) return Seq.empty[ReportNodeOld]

    val range = period.range
    val siteTopicsById = siteTopics.map(st => (st.node.id, st)).toMap
    val superGraph = siteTopics.map(_.autoGraph).reduce(_ + _)
    val graphLevels = superGraph.nodesByLevel.keySet.toSeq.sorted

    def report(nodes: Seq[Node]): Seq[ReportNodeOld] = {

      def rowToReportNode(row: SiteTopicRow, node: Node, lower: Seq[ReportNodeOld] = Seq.empty[ReportNodeOld], topics: Seq[ReportNodeOld] = Seq.empty[ReportNodeOld]): ReportNodeOld = {
        val m = row.aggregateMetricsOldBetween(range.fromInclusive, range.toInclusive)
        val vm = row.aggregateViralMetricsBetween(range.fromInclusive, range.toInclusive)
        val vel = calculateViralVelocity(row, range)
        val vis = row.family(_.uniques).getOrElse(range, 0l)

        ReportNodeOld(node, m, vm, vel, vis, lower, topics)
      }

      nodes.flatMap((node: Node) => siteTopicsById.get(node.id) match {
        case Some(row) => {
          val lowerNodes = superGraph.incomingNodes(node)

          val lowerInterestNodes = lowerNodes.filter(_.nodeType == NodeType.Interest)
          val lowerTopicNodes = lowerNodes.filter(_.nodeType == NodeType.Topic)
          val lowerInterestRows = lowerInterestNodes.flatMap(n => siteTopicsById.get(n.id))
          val lowerTopicRows = lowerTopicNodes.flatMap(n => siteTopicsById.get(n.id))
          if (lowerInterestRows.length > 0) {
            Some(rowToReportNode(row, node, report(lowerInterestNodes), lowerTopicRows.map(r => rowToReportNode(r, node))))
          }
          else {
            Some(rowToReportNode(row, node, Seq(), lowerTopicRows.map(r => rowToReportNode(r, node))))
          }
        }
        case None => None
      })

    }

    val r = report(superGraph.nodesByLevel(graphLevels.head))
    r
  }

  def topic(siteGuid: String, topicUri: String): Option[SiteTopicRow] = topic(SiteTopicKey(siteGuid, topicUri))

  def topic(siteTopicKey: SiteTopicKey): Option[SiteTopicRow] = Schema.SiteTopics.query2.withKey(siteTopicKey).withFamilies(_.meta, _.standardMetricsHourlyOld).singleOption()

  def topicArticles(siteTopicKey: SiteTopicKey, range: DateMidnightRange = TimeSliceResolution.lastThirtyDays.range, maxArticles: Int = 250): Validation[FailureResult, TopicArticles] = {
    val arsk = ArticleRangeSortedKey.getDefaultKey(range)
    Schema.SiteTopics.query2.withKey(siteTopicKey).withColumns(_.topicUri, _.topicName).withColumn(_.topSortedArticles, arsk).singleOption(skipCache = false) match {
      case Some(topic) => {
        topic.family(_.topSortedArticles).get(arsk) match {
          case Some(articleKeys) => {
            val keysToUse = if (maxArticles > 0 && maxArticles < 250) articleKeys.take(maxArticles).toSet else articleKeys.toSet
            val articles = getArticleMetrics(keysToUse, range)

            Success(TopicArticles(siteTopicKey.topicId, topic.topicUri.getOrElse("NO-URI"), topic.topicName.getOrElse("NO-NAME"), articles))
          }
          case None => Success(TopicArticles(siteTopicKey.topicId, topic.topicUri.getOrElse("NO-URI"), topic.topicName.getOrElse("NO-NAME"), Seq.empty[ArticleMetrics]))
        }
      }
      case None => Failure(FailureResult(msg = "No topic found for: " + siteTopicKey.toString))
    }
  }

  def topicsWithArticles(siteKey: SiteKey,
                         topicIds: Seq[Long],
                         postTopicsProcess: (Seq[SiteTopicRow]) => Seq[TopicWithRangedMetrics],
                         postArticlesProcess: (Seq[ArticleMetrics]) => Seq[ArticleWithRangedMetrics]): Validation[FailureResult, Seq[TopicAndArticlesWithRangedMetrics]] = {

    val siteId = siteKey.siteId
    try {
      val stKeys = topicIds.map(tid => SiteTopicKey(siteId, tid)).toSet

      val siteTopics = postTopicsProcess(Schema.SiteTopics.query2.withKeys(stKeys).withFamilies(_.meta, _.standardMetricsHourlyOld, _.viralMetrics, _.viralMetricsHourly, _.topSortedArticles).executeMap(skipCache = false).values.toSeq)

      val articleKeys = new mutable.HashSet[ArticleKey]()

      for {
        st <- siteTopics
        ak <- st.articleKeys
      } {
        articleKeys.add(ak)
      }

      val articles = getArticleMetricsMap(articleKeys)

      val topicArticles = siteTopics.map(topic => {
        val arts = postArticlesProcess(topic.articleKeys.toSeq.collect(articles))
        TopicAndArticlesWithRangedMetrics(topic.topicId, topic.topicUri, topic.topicName, arts, topic.metrics, topic.viralMetrics, topic.viralVelocity)
      })
      Success(topicArticles)
    }
    catch {
      case ex: Exception => Failure(FailureResult("Failed to retrieve topics and articles for siteId: " + siteId, ex))
    }
  }


  def topicWithUniques(siteTopicKey: SiteTopicKey, fullRange: DateMidnightRange, trendRanges: Option[Set[DateMidnightRange]] = None): Validation[FailureResult, InterestUniques] = {
    val ranges = trendRanges.getOrElse(fullRange.singleDayRangesWithin)

    val query = Schema.SiteTopics.query2
      .withKey(siteTopicKey)
      .withColumns(_.topicName, _.topicUri)
      .withColumn(_.uniques, fullRange)

    ranges.foreach(r => query.withColumn(_.uniques, r))

    query.singleOption(skipCache = false) match {
      case Some(result) => {
        parseInterestUniquesResult(result, fullRange, ranges) match {
          case Some(iu) => Success(iu)
          case None => Failure(FailureResult(msg = "Resulting site topic did not contain necessary data! Specified key: " + siteTopicKey.toString))
        }
      }
      case None => Failure(FailureResult(msg = "No site topic found for: " + siteTopicKey.toString))
    }
  }

  def calculateViralVelocity(topic: SiteTopicRow, range: DateMidnightRange, counter: CounterFunc = CounterNoOp): Double = {
    val hours = range.hoursWithin
    val allVmHourly = topic.family(_.viralMetricsHourly)
    val vmHourly = hours.flatMap(h => allVmHourly get (h) map (vm => h -> vm)).toMap

    if (vmHourly.isEmpty) {
      return 0.0
    }

    val normalizedVmHourly = SiteService.site(SiteKey(SuperSiteService.igwSuperSiteGuid)) map (_.viralMetricsHourly) match {
      case Some(superVmHourly) => {
        vmHourly map {
          case (hour, siteTopicVm) =>
            val superVm = superVmHourly.getOrElse(hour, ViralMetrics.empty)

            if (siteTopicVm.total > superVm.total) {
              counter("Topic had more virality than " + SuperSiteService.igwSuperSiteGuid + " for a single hour", 1L)
              printf("Topic had more virality than IGW (%s):%n\turi: %s%n\thour: %s%n", SuperSiteService.igwSuperSiteGuid, topic.topicUri.getOrElse("NO URI"), hour.toString)
            }

            val normalizedAgainstAllVirality = if (superVm.total == 0) 0d else siteTopicVm.total.toDouble / superVm.total
            hour -> normalizedAgainstAllVirality
        }
      }
      case None => {
        counter("No such supersite " + SuperSiteService.igwSuperSiteGuid + ". SOMETHING IS WRONG!", 1L)
        vmHourly map {
          case (hour, siteTopicVm) => hour -> siteTopicVm.total.toDouble
        }
      }
    }

    implicit val totalNormalizedMetrics = new CanTotal[Double, Double] {
      override def total(metrics: Double*) = metrics.sum
    }

    val isReportForToday = range.fromInclusive == new GrvDateMidnight()
    val result = if (isReportForToday) {
      val currentHour = DateHour.currentHour
      AggregableMetrics.hourlyTrendingScoreBetween(normalizedVmHourly, currentHour.minusHours(6), currentHour.plusHours(1))
    }
    else {
      AggregableMetrics.hourlyTrendingScoreBetweenInclusive(normalizedVmHourly, range.fromInclusive, range.toInclusive)

    }

    result
  }

  def buildSiteInterestCounts(siteGuid: String, topics: Iterable[SiteTopicRow], range: DateMidnightRange): Validation[FailureResult, Traversable[InterestCounts]] = {
    try {
      val blacklist = NodeBlackListService.retrieveBlackList(siteGuid)

      val interests = for {
        topic <- topics
        topicId = topic.topicId
        if (!blacklist.contains(topicId))
        topicUri <- topic.topicUri
        if (NS.getType(topicUri) == URIType.TOPIC)
        name <- topic.topicName
        metrics = topic.aggregateMetricsOldBetween(range.fromInclusive, range.toInclusive)
        viralMetrics = topic.aggregateViralMetricsBetween(range.fromInclusive, range.toInclusive)
        velocity = calculateViralVelocity(topic, range)
        visitors = topic.family(_.uniques).getOrElse(range, 0l)
        if (!metrics.isEmpty || !viralMetrics.isEmpty)
      } yield {
        InterestCounts(topicUri, metrics.views, metrics.publishes, metrics.socialReferrers, metrics.searchReferrers, metrics.keyPageReferrers, name, viralMetrics, velocity, visitors)
      }

      Success(interests)
    }
    catch {
      case ex: Exception => Failure(FailureResult("Failed to build SiteInterestData!", ex))
    }
  }

  private def parseInterestUniquesResult(result: SiteTopicRow, fullRange: DateMidnightRange, ranges: Set[DateMidnightRange]): Option[InterestUniques] = {
    for {
      name <- result.column(_.topicName)
      uri <- result.column(_.topicUri)
      id = result.rowid.topicId
      uniqueMap = result.family(_.uniques)

      inners = (for {
        r <- ranges
        t = uniqueMap.getOrElse(r, 0l)
      } yield {
        (r.fromInclusive -> t)
      }).toMap
      total = uniqueMap.getOrElse(fullRange, {
        val withins = fullRange.singleDayRangesWithin
        if (withins.isEmpty) {
          0l
        }
        else {
          val uSeq = withins.collect(uniqueMap).toSeq
          if (uSeq.isEmpty) 0l else uSeq.max
        }
      })
    } {
      return Some(InterestUniques(id, name, uri, total, inners, fullRange))
    }
    None
  }

  private def getArticleMetrics(articleKeys: Set[ArticleKey], dmRange: DateMidnightRange) = {
    val dhRange = dmRange.toDateHourRange
    val articleRows = Schema.Articles.query2
      .withKeys(articleKeys)
      .withFamilies(_.standardMetricsHourlyOld, _.viralMetrics, _.viralMetricsHourly)
      .withColumns(_.url, _.publishTime, _.title, _.image, _.behindPaywall, _.summary)
      .filter(
      _.or(
        _.betweenColumnKeys(_.standardMetricsHourlyOld, dhRange.fromHour, dhRange.toHour),
        _.betweenColumnKeys(_.viralMetrics, dmRange.fromInclusive, dmRange.toInclusive),
        _.betweenColumnKeys(_.viralMetricsHourly, dhRange.fromHour, dhRange.toHour),
        _.allInFamilies(_.meta, _.text)
      )
    )
      .executeMap(skipCache = false).values.toSeq

    for {
      article <- articleRows
      url <- article.column(_.url)
      pubDateTime <- article.column(_.publishTime)
      title = article.column(_.title).getOrElse(url)
      image = article.column(_.image).getOrElse("")
      summary = article.column(_.summary).getOrElse("")
      paywall = article.column(_.behindPaywall).getOrElse(false)
    } yield {
      ArticleMetrics(article.rowid.articleId, url, title, pubDateTime.getMillis, article.standardMetricsOld, article.viralMetrics, article.viralMetricsHourly, image, summary, paywall)
    }
  }

  private def getArticleMetricsMap(articleKeys: Set[ArticleKey]) = {
    val articleRows = Schema.Articles.query2
      .withKeys(articleKeys)
      .withFamilies(_.meta, _.standardMetricsHourlyOld, _.viralMetrics, _.viralMetricsHourly)
      .withColumns(_.summary)
      .executeMap(skipCache = false).values.toSeq

    (for {
      article <- articleRows
      akey = article.rowid
      url <- article.column(_.url)
      pubDateTime <- article.column(_.publishTime)
      title = article.column(_.title).getOrElse(url)
      image = article.column(_.image).getOrElse("")
      summary = article.column(_.summary).getOrElse("")
      paywall = article.column(_.behindPaywall).getOrElse(false)
    } yield {
      (akey, ArticleMetrics(akey.articleId, url, title, pubDateTime.getMillis, article.standardMetricsOld, article.viralMetrics, article.viralMetricsHourly, image, summary, paywall))
    }).toMap
  }
}

case class InterestUniques(id: Long, name: String, uri: String, totalVisitors: Long, uniqueMap: Map[GrvDateMidnight, Long], range: DateMidnightRange) {
  override lazy val toString = {
    val fmt = "Uniques for \"%s\" (id: %d; uri: \'%s\") within %s:%n\ttotal: %,d%n\tDays within:%n%s%n"

    val b = new StringBuilder
    val dfmt = DateTimeFormat.forPattern("MM/dd/yyyy")

    uniqueMap.toSeq.sortBy(_._1.getMillis).foreach {
      case (dm, uns) => {
        b.append("\t\t")
        b.append(dm.toString(dfmt)).append(": ").append("%,d%n".format(uns))
      }
    }

    val innerString = b.toString()

    fmt.format(name, id, uri, range, totalVisitors, innerString)
  }
}

case class ArticleMetrics(
                           id: Long,
                           url: String,
                           title: String,
                           publishTimeStamp: Long,
                           metricsMap: Map[GrvDateMidnight, StandardMetrics],
                           viralMap: Map[GrvDateMidnight, ViralMetrics] = Map.empty[GrvDateMidnight, ViralMetrics],
                           viralHourlyMap: Map[DateHour, ViralMetrics] = Map.empty[DateHour, ViralMetrics],
                           image: String = "",
                           summary: String = "",
                           isPayWalled: Boolean = false)

case class TopicArticles(
                          topicId: Long,
                          topicUri: String,
                          topicName: String,
                          articles: Seq[ArticleMetrics],
                          metricsMap: Map[GrvDateMidnight, StandardMetrics] = Map.empty[GrvDateMidnight, StandardMetrics],
                          viralMap: Map[GrvDateMidnight, ViralMetrics] = Map.empty[GrvDateMidnight, ViralMetrics])

case class TopicWithRangedMetrics(
                                   topicId: Long,
                                   topicUri: String,
                                   topicName: String,
                                   metrics: StandardMetrics,
                                   viralMetrics: ViralMetrics,
                                   viralVelocity: Double,
                                   articleKeys: Set[ArticleKey])

case class ArticleWithRangedMetrics(
                                     url: String,
                                     title: String,
                                     image: String,
                                     publishTimeStamp: Long,
                                     metrics: StandardMetrics,
                                     viralMetrics: ViralMetrics,
                                     viralVelocity: Double,
                                     summary: String = "",
                                     isPayWalled: Boolean = false)

case class TopicAndArticlesWithRangedMetrics(
                                              topicId: Long,
                                              topicUri: String,
                                              topicName: String,
                                              articles: Seq[ArticleWithRangedMetrics],
                                              metrics: StandardMetrics,
                                              viralMetrics: ViralMetrics,
                                              viralVelocity: Double)
