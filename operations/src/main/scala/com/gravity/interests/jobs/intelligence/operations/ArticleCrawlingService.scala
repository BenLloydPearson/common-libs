package com.gravity.interests.jobs.intelligence.operations

import java.net.URL

import com.gravity.interests.jobs.articles._
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.{ArticleKey, ArticleRow, IngestionTypes, Schema}
import com.gravity.utilities.ScalaMagic
import com.gravity.utilities.components.FailureResult
import org.joda.time.DateTime

import scalaz.Scalaz._
import scalaz._

object ArticleCrawlingService extends Crawler {
  implicit val conf = HBaseConfProvider.getConf.defaultConf

  def crawlAndSave(url: String, siteGuid: String = "", query: ArticleService.QuerySpec = _.withFamilies(_.meta), ingestionSource: IngestionTypes.Type = IngestionTypes.fromCrawlingService): ValidationNel[FailureResult, ArticleRow] = {
    val sg = if (ScalaMagic.isNullOrEmpty(siteGuid)) {
      Schema.Articles.query2.withKey(ArticleKey(url)).withColumn(_.siteGuid).singleOption() match {
        case Some(article) if !article.siteGuid.isEmpty => article.siteGuid
        case _ => return FailureResult("No siteGuid supplied and failed to find it in the Articles table for url: " + url).failureNel
      }
    } else {
      siteGuid
    }

    val articleType = ArticleService.detectArticleType(url, sg)

    var errors = 0

    def vAndC: Validation[CrawlRetry, CrawlSuccess] = validateAndCrawl(CrawlCandidate(sg, url, errors, ingestionSource = ingestionSource, articleType = articleType)) match {
      case Success(cs) => Success(cs)
      case failedFailType @ Failure(failType) => failType match {
        case Retry =>
          errors += 1
          vAndC
        case _ => failedFailType
      }
    }

    val crawlSuccess = vAndC match {
      case Success(cs) => cs
      case Failure(cr) => cr match {
        case Blacklisted => return FailureResult(url + " was BLACKLISTED!").failureNel
        case NoHtmlReturned => return FailureResult(url + " did not return any HTML and was attempted " + errors + " times and the maximum retries is set to: " + crawlErrorLimit).failureNel
        case Retry => return FailureResult(url + " retries failed to continue even though we only failed " + errors + " times and the maximum retries is set to: " + crawlErrorLimit).failureNel
        case RetriesExhausted => return FailureResult(url + " retries exhausted! Failed a total of: " + crawlErrorLimit + " times!").failureNel
        case AlreadyRssIngested => return AlreadyCrawledFailureResult(url).failureNel
      }
    }

    sendToHbase(crawlSuccess, query)
  }

  def hasArticleBeenCrawled(url : String) : Boolean = {
    hasArticleBeenCrawled(ArticleKey(url) )
  }

  def hasArticleBeenCrawled(url : URL) : Boolean = {
    hasArticleBeenCrawled(ArticleKey(url))
  }

  def isArticleInTable(url: String, skipCache: Boolean = true) : Boolean = {
    Schema.Articles.query2.withKey(ArticleKey(url)).withFamilies(_.text).singleOption(skipCache = skipCache).isDefined
  }

  def isInArticlesTableWithNonEmptyTitle(url: String, skipCache: Boolean = true): Boolean = {
    ArticleService.fetch(ArticleKey(url), skipCache = skipCache)(_.withColumns(_.title)) match {
      case Success(articleRow) if articleRow.column(_.title).exists(!_.isEmpty) => true
      case _ => false
    }
  }

  def isInArticlesTableWithNonEmptyTitleAndValidSiteGuid(url: String, skipCache: Boolean = true): Boolean = {
    ArticleService.fetch(ArticleKey(url), skipCache = skipCache)(_.withColumns(_.title, _.siteGuid)) match {
      case Success(articleRow) =>
        articleRow.column(_.title).exists(!_.isEmpty) && articleRow.column(_.siteGuid).exists(_ != SiteService.HIGHLIGHTER)

      case _ => false
    }
  }

  def hasArticleBeenCrawled(articleKey: ArticleKey, skipCache: Boolean = false): Boolean = {
    articlesThatHaveBeenCrawled(Set(articleKey), skipCache).contains(articleKey)
  }

  def articlesThatHaveBeenCrawled(articleKeys: Set[ArticleKey], skipCache: Boolean = false): Set[ArticleKey] = {
    Schema.ArticleCrawling.query2.withKeys(articleKeys).withColumn(_.date).multiMap(skipCache = skipCache).keys.toSet
  }

  def articleHasBeenCrawled(url : String) {
    val now = new DateTime()
    val articleKey = ArticleKey(url)
    Schema.ArticleCrawling.put(articleKey).value(_.url,url).value(_.date,now).execute()
  }

  def articleHasNotBeenCrawled(url: String) {
    articleHasNotBeenCrawled(ArticleKey(url))
  }

  def articleHasNotBeenCrawled(articleKey : ArticleKey) {
    Schema.ArticleCrawling.delete(articleKey).execute()
  }
}

object ArticleCrawlCheckerApp extends App {
  val url = "http://news.yahoo.com/blackberry-maker-rim-unveils-prototype-145500321--finance.html"
  println(ArticleCrawlingService.hasArticleBeenCrawled(url))
  println(ArticleCrawlingService.isArticleInTable(url))
}

case class AlreadyCrawledFailureResult(url: String) extends FailureResult(s"URL: `$url` already RSS ingested!", None)
