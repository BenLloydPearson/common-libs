package com.gravity.interests.jobs.intelligence.operations

import java.util.concurrent.atomic.AtomicBoolean

import com.gravity.data.reporting.GmsElasticSearchService
import com.gravity.domain.aol._
import com.gravity.domain.gms.{GmsArticleStatus, UniArticleId}
import com.gravity.interests.jobs.intelligence.{ArticleKey, SiteKey}
import com.gravity.utilities._
import com.gravity.utilities.analytics.DateMidnightRange
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvcoll._
import com.gravity.utilities.grvz._
import com.gravity.utilities.web.{NegatableEnum, SortSpec}
import com.gravity.valueclasses.ValueClassesForDomain.SiteGuid

import scala.collection.Set
import scalaz.Scalaz._
import scalaz._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/
object GmsArticleIndexer {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  /** Provided for overriding to ease unit test data fixture generation. */
  protected val withMetrics = true

  val isAutoIndexingEnabled = Settings2.getBooleanOrDefault("gms.lucene.autoindex", default = true)
  val includeMetrics = Settings2.getBooleanOrDefault("gms.lucene.autoindex.include.metrics", default = true)

  private val esqs = GmsElasticSearchService()
  private val indexBuilt = new AtomicBoolean(false)
  private val currentlyRebuildingIndex = new AtomicBoolean(false)

  private val indexerHbaseCounterKey = "rebuildIndex-hbase-call-failed"

  private val loopRuntimeAvgCtr = getOrMakeAverageCounter("loop runtime avg (ms)", "GmsArticleIndexer")

  val counterCategory: String = "GmsArticleIndexer"

  def rebuildIndexLoop: ValidationNel[FailureResult, Unit] = {
    if (!isAutoIndexingEnabled) {
      indexBuilt.set(true)
      return unitSuccessNel
    }

    RebuildGmsArticleIndexJob.start()

    while (!indexBuilt.get()) {
      Thread.sleep(5)
    }

    unitSuccessNel
  }

  private val zeroIntSuccessNel: ValidationNel[FailureResult, Int] = 0.successNel[FailureResult]

  /** Rebuilds the index replacing the old one in its entirety. */
  def rebuildIndex(): ValidationNel[FailureResult, Int] = {
    if (!isAutoIndexingEnabled) return zeroIntSuccessNel   // RETHINK vs. CI

    // Atomically: if currentlyRebuildingIndex is false, set it to true, otherwise fail and return.
    if (!currentlyRebuildingIndex.compareAndSet(false, true)) {
      warn("GmsArticleIndexer currently in another rebuild index loop; if this happens too often, increase the loop time or restrategize")
      return FailureResult("Currently in another rebuild index loop").failureNel
    }

    try {
      info("Rebuilding index for GmsArticleIndexer :: STARTING")
      countPerSecond(counterCategory, "Index Rebuild Loops")

      val start = System.currentTimeMillis()

      try {
        // For Aol.com, the indexer uploads cached scoped metrics to Lucene,
        // but when we first start up, we haven't cached any metrics yet,
        // so the metrics that are indexed (which are used to sort article lists) are empty, and the articles don't sort properly.
        // Once a user has displayed some articles, this loads metrics for those articles from Hbase into the cache,
        // so then, after the next index rebuild (every 2 minutes), those articles will be correctly sorted by non-empty cached data,
        // but that first listing of the articles (and any subsequent ones before the 2-minute rebuild) are unsortable by metrics.
        //
        // For now, for non-Aol.com articles only, I'm having the indexer upload non-cached metrics to Lucene,
        // until I can see if I can make this any more efficient.

        val numArticles = GmsService.getAllGmsArticlesFromHbaseAsSgMap(withMetrics = withMetrics, pullMetricsFromCacheOnly = false) match {
          case Success(gmsMap) =>
            clearCountPerSecond(counterCategory, indexerHbaseCounterKey)
            gmsMap.foldLeft(0) { case (totalCount: Int, keyVal: (SiteGuid, Seq[AolGmsArticle])) => keyVal match {
              case (sg: SiteGuid, articles: Seq[AolGmsArticle]) =>
                updateElasticSearch(articles, sg.siteKey)
                totalCount + articles.size
            }}

          case Failure(fails) =>
            countPerSecond(counterCategory, indexerHbaseCounterKey)
            warn(fails, "Failed to `getAllGmsArticlesFromHbaseAsSgMap` while attempting to rebuild the index!")
            return fails.failure
        }

        indexBuilt.set(true)

        numArticles.successNel
      } catch {
        case ex: Exception =>
          val msg = "Failed to rebuild AOL DL article index"
          warn(ex, msg)
          nel(FailureResult(msg, ex)).failure
      } finally {
        val end = System.currentTimeMillis()
        val durMs = end - start
        loopRuntimeAvgCtr.set(durMs)

        info(s"Rebuilding index for GmsArticleIndexer :: COMPLETED, $durMs ms.")
      }
    } finally {
      currentlyRebuildingIndex.set(false)
    }
  }

  /** Updates index for a single article with the latest data from Hbase. */
  def updateGmsIndex(uniArticleId: UniArticleId, siteKey: SiteKey): ValidationNel[FailureResult, Unit] = updateGmsIndex(Seq(uniArticleId), siteKey, refresh = true)

  def updateGmsIndex(gmsArticleIds: Seq[UniArticleId], siteKey: SiteKey, refresh: Boolean = false): ValidationNel[FailureResult, Unit] = {
    if(!indexBuilt.get()) {
      val rebuildIndexResult = rebuildIndex().map(_ => ())
      rebuildIndexResult.forFail(fails => {
        warn(fails, s"GmsArticleIndexer.updateGmsIndex failed to rebuild DLUG/GMS article index in preparation for updateGmsIndex($gmsArticleIds)")
        return rebuildIndexResult
      })
    }

    try {
      val articles = GmsService.getUnorderedGmsArticlesFromHbase(gmsArticleIds, withMetrics = withMetrics, pullMetricsFromCacheOnly = true).valueOr {
        fails =>
          warn(fails, s"GmsArticleIndexer.updateGmsIndex failed to get DLUG/GMS article(s) from Hbase for updateGmsIndex($gmsArticleIds)")
          return fails.failure
      }

      // Update the successfully-retrieved GMS articles in the index.
      updateElasticSearch(articles, siteKey, refresh)

      // Note the number of keys where we were not able to retrieve the article.
      val keysWeWrote  = articles.map(keyValue).toSet
      val keysPassedIn  = gmsArticleIds.map { uniArticleId => gmsKeyValue(uniArticleId.siteKey, uniArticleId.articleKey) }
      val keysThatWeFailedToUpdate = keysPassedIn.filterNot(keysWeWrote contains)

      info(s"GmsArticleIndexer.updateGmsIndex updated ${articles.size} articles.")

      if (keysThatWeFailedToUpdate.nonEmpty)
        warn(s"GmsArticleIndexer.updateGmsIndex was unable to retrieve articles for ${keysThatWeFailedToUpdate.size} keys.")

      unitSuccessNel
    }
    catch {
      case ex: Exception =>
        val msg = s"GmsArticleIndexer.updateGmsIndex failed to update DLUG/GMS article index for $gmsArticleIds"
        val fails = nel(FailureResult(msg, ex))
        warn(fails, msg)
        fails.failure
    }
  }

  def query(pagerOpt: Option[Pager], siteGuid: Option[String] = None, channel: Option[AolDynamicLeadChannels.Type] = None,
            searchTerms: Set[String] = Set.empty, status: Set[NegatableEnum[GmsArticleStatus.Type]] = Set.empty,
            submittedUserId: Option[Int] = None, submittedDate: Option[DateMidnightRange] = None,
            deliveryMethod: Option[AolDeliveryMethod.Type] = None, aolCategory: Option[String] = None,
            aolSource: Option[String] = None, fmt: Option[String] = Option("DLUG"), contentGroupId: Option[Long] = None)
  : ValidationNel[FailureResult, AolGmsKeyQueryResult] = {
    val F = GmsElasticSearchUtility.MetaFields
    siteGuid match {
      case Some(sg) =>
        val pager = pagerOpt match {
          case Some(pgr) =>
            val cgId = contentGroupId.getOrElse(1996L)
            pgr.sort.property match {
              case AolDynamicLeadSort.ThirtyMinuteCtr.name => pgr.copy(sort = pgr.sort.copy(property = F.thirtyMinuteCtr(cgId)))
              case AolDynamicLeadSort.ImpressionsTotal.name => pgr.copy(sort = pgr.sort.copy(property = F.impressionsTotalForContentGroup(cgId)))
              case AolDynamicLeadSort.ClicksTotal.name => pgr.copy(sort = pgr.sort.copy(property = F.clicksTotalForContentGroup(cgId)))
              case AolDynamicLeadSort.CtrTotal.name => pgr.copy(sort = pgr.sort.copy(property = F.ctrTotalForContentGroup(cgId)))
              case _ => pgr
            }

          case None =>
            val sort = contentGroupId.fold(SortSpec(GmsElasticSearchUtility.MetaFields.title)) { cgId =>
              SortSpec(GmsElasticSearchUtility.MetaFields.thirtyMinuteCtr(cgId), asc = false)
            }
            Pager(1, GmsElasticSearchService.maxResultSize, sort)
        }



        esqs.queryGmsArticles(sg, pager, contentGroupId, searchTerms.toSet, status.toSet, submittedUserId, submittedDate, deliveryMethod, aolCategory, aolSource)

      case None =>
        FailureResult("siteGuid is required non-empty for ElasticSearch article queries!").failureNel
    }
  }

  /** Finds keys of articles which appear similar to a phrase. */
  def similarArticles(phrase: String, siteGuid: Option[String] = None,
                      excludeArticle: Option[AolUniArticle] = None): ValidationNel[FailureResult, Seq[UniArticleId]] = {

    siteGuid tuple excludeArticle match {
      case Some((sg: String, similarToArticle: AolGmsArticle)) =>
        val sk = SiteKey(sg)
        esqs.querySimilarByTitleGmsArticles(similarToArticle, sg).map(_.map(ak => UniArticleId.forGms(sk, ak)))

      case _ => Seq.empty[UniArticleId].successNel
    }
  }

  private def updateElasticSearch(articles: Traversable[AolGmsArticle], siteKey: SiteKey, refresh: Boolean = false): Unit = {
    if (articles.nonEmpty) {
      import scala.concurrent._
      implicit val ec = ExecutionContext.global
      Future {
        blocking {
          val resp = esqs.updateGmsArticles(articles, siteKey, refresh)
          resp.getTook -> resp.buildFailureMessage()
        }
      } onComplete {
        case scala.util.Success((elapsedTime, errors)) =>
          info(s"Updated ${articles.size} articles in ElasticSearch in $elapsedTime")

        case scala.util.Failure(ex) =>
          warn(ex, "Failed to update {0} articles in ElasticSearch!", articles.size)
      }
    }
  }

  private def dlugKeyValue(articleKey: ArticleKey) =
    articleKey.stringConverterSerialized

  private def gmsKeyValue(siteKey: SiteKey, articleKey: ArticleKey) =
    s"gms-${siteKey.stringConverterSerialized}-${articleKey.stringConverterSerialized}"

  private def keyValue(a: AolUniArticle): String = a match {
    case a: AolDynamicLeadArticle =>
      dlugKeyValue(a.articleKey)

    case a: AolGmsArticle =>
      gmsKeyValue(a.siteKey, a.articleKey)

    case _ =>
      warn("Can't Happen in GmsArticleIndexer.keyValue: Unknown GmsArticle variant.")
      "CANT-HAPPEN"
  }

}