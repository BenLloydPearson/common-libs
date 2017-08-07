package com.gravity.interests.jobs.intelligence.operations

import scala.collection._
import scalaz._
import scalaz.Scalaz._
import scalaz.std.iterable._
import com.gravity.utilities.grvz._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.components.FailureResult
import com.gravity.interests.jobs.intelligence._
import com.gravity.utilities.analytics.TimeSliceResolution
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.interests.jobs.intelligence.operations.analytics.InterestSortBy
import com.gravity.hbase.schema.PutOp

/**
* Created by IntelliJ IDEA.
* Author: Robbie Coleman
* Date: 9/26/11
* Time: 4:32 PM
*/

object SuperSiteService extends SuperSiteService

trait SuperSiteService {
//  val superSiteBaseUri = "http://insights.gravity.com/rollup/"
//  val wsjImageBase = "http://si.wsj.net/public/resources/images/"

  val igwSuperSiteGuid = "ROLL4dc0a6f860e9f7d03fbfe29124cd"

  def addSitesToSuperSite(superSiteKey: SiteKey, subSiteKeys: Set[SiteKey], subSitePutter: (SubSitePutHandler) => PutOp[SitesTable,SiteKey], makeBiDirectionalLink: Boolean = true): ValidationNel[FailureResult, Set[SiteKey]] = {
    if (subSiteKeys.contains(superSiteKey)) return FailureResult(msg = "You cannot add a subsite with the same key as its super site!").failureNel

    val superSet = Set(superSiteKey)
    SiteService.fetchMulti((superSet ++ subSiteKeys).toSet)(_.withFamilies(_.meta)) match {
      case Success(siteMap) => {
        siteMap.get(superSiteKey) match {
          case Some(superSite) => {
            val subResults = for {
              subKey <- subSiteKeys.toBuffer[SiteKey]
            } yield {
              SiteService.modifyPut(subKey)(put => {
                val handleThis = siteMap.get(subKey) match {
                  case rowOpt@Some(row) => SubSitePutHandler(subKey, rowOpt, put.value(_.superSites, row.superSites ++ superSet))
                  case None => SubSitePutHandler(subKey, None, put.value(_.superSites, superSet))
                }
                subSitePutter(handleThis)
              })
            }

            val allResults = if (makeBiDirectionalLink) {
              subResults += SiteService.modifyPut(superSiteKey)(_.value(_.subSites, superSite.subSites ++ subSiteKeys))
            } else {
              subResults
            }

            allResults.extrude match {
              case Success(_) => subSiteKeys.successNel
              case Failure(failed) => failed.failure
            }
          }
          case None => FailureResult(msg = "No site found for super siteId: " + superSiteKey.siteId).failureNel
        }
      }
      case Failure(failed) => failed.failure
    }
  }

//  def getSuperUriFromInterestKey(interestKey: String) = superSiteBaseUri + interestKey

//  def resolveImageUrl(image: String, articleUrl: String): String = image match {
//    case null => emptyString
//    case mt if (mt.isEmpty) => mt
//    case _ => if (articleUrl.startsWith("http://www.smartmoney.com/")) wsjImageBase + image else ArticleWhitelist.getPartnerName(articleUrl) match {
//      case Some(partner) => partner match {
//        case ArticleWhitelist.Partners.WSJ => wsjImageBase + image
//        case _ => image
//      }
//      case None => image
//    }
//  }

//  def getGroupedArticles(superUri: String, timePeriod: TimeSliceResolution, maxTopics: Int, maxArticles: Int): Validation[FailureResult, Seq[TopicAndArticlesWithRangedMetrics]] = {
//    val safeMaxTopics = if (maxTopics < 1) 10 else maxTopics
//    val safeMaxArticles = if (maxArticles < 1) 10 else maxArticles
//
//    val dmRange = timePeriod.range
//    val arsk = ArticleRangeSortedKey(dmRange, InterestSortBy.TotalViral, true)
//
//    def postTopics(topics: Seq[SiteTopicRow]) = {
//      val topicsAgg = for {
//        t <- topics
//        tid = t.topicId
//        uri <- t.topicUri
//        name <- t.topicName
//        articleKeys <- t.getTopArticles(arsk)
//        metrics = t.standardMetricsOld.aggregateBetween(dmRange.fromInclusive, dmRange.toInclusive)
//        viralMetrics = t.viralMetrics.aggregateBetween(dmRange.fromInclusive, dmRange.toInclusive)
//        viralVelocity = AggregableMetrics.hourlyTrendingScoreBetweenInclusive(t.viralMetricsHourly, dmRange.fromInclusive, dmRange.toInclusive)
//      } yield TopicWithRangedMetrics(tid, uri, name, metrics, viralMetrics, viralVelocity, articleKeys.toSet)
//
//      topicsAgg.sortBy(-_.viralVelocity).take(safeMaxTopics)
//    }
//
//    def postArticles(articles: Seq[ArticleMetrics]) = {
//      val articlesAgg = for {
//        a <- articles
//        metrics = a.metricsMap.aggregateBetween(dmRange.fromInclusive, dmRange.toInclusive)
//        viralMetrics = a.viralMap.aggregateBetween(dmRange.fromInclusive, dmRange.toInclusive)
//        viralVelocity = AggregableMetrics.hourlyTrendingScoreBetweenInclusive(a.viralHourlyMap, dmRange.fromInclusive, dmRange.toInclusive)
//      } yield ArticleWithRangedMetrics(a.url, a.title, resolveImageUrl(a.image, a.url), a.publishTimeStamp, metrics, viralMetrics, viralVelocity, a.summary, a.isPayWalled)
//
//      articlesAgg.sortBy(-_.viralVelocity).take(safeMaxArticles)
//    }
//
//    SiteService.getSuperSite(superUri) match {
//      case Some(site) => {
//        val siteKey = site.rowid
//        SiteReportService.getViralTrends("Topics", siteKey, timePeriod.resolution, timePeriod.year, timePeriod.point) match {
//          case Success(topics) => {
//            try {
//              val topicIds = topics.map(_.id)
//
//              SiteTopicService.topicsWithArticles(siteKey, topicIds, postTopics, postArticles) match {
//                case Success(tas) => Success(tas)
//                case Failure(failed) => Failure(FailureResult("Failed to get topics with articles due to: " + failed.message, failed.exceptionOption))
//              }
//            }
//            catch {
//              case ex: Exception => Failure(FailureResult(msg = "Failed to process the articles for topics!", ex = ex))
//            }
//          }
//          case Failure(failed) => Failure(FailureResult("Failed within SiteReportService with message: " + failed.message, failed.exceptionOption))
//        }
//
//      }
//      case None => Failure(FailureResult(msg = "No Super Site found for superUri: " + superUri))
//    }
//
//  }
}

case class SiteMetaData(guid: String, name: String, url: String)

case class SubSitePutHandler(key: SiteKey, rowOption: Option[SiteRow], putOp: PutOp[SitesTable,SiteKey]) {
  def alreadyExists: Boolean = rowOption.isDefined
}
