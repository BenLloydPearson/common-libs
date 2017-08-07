package com.gravity.interests.jobs.intelligence.operations.sites

import com.gravity.interests.jobs.intelligence._
import com.gravity.test.operationsTesting
import org.joda.time.DateTime
import com.gravity.interests.jobs.intelligence.operations.ArticleCrawlingService
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestEnvironment
import com.gravity.utilities.BaseScalaTest

class ArticleCrawlingServiceTest extends BaseScalaTest with operationsTesting {
  test("putGetDeleteTest") {
    val url = "http://kitties.com/ArticleCrawlingServiceTest/putGetDeleteTest"
    val articleKey = ArticleKey(url)
    val now = new DateTime
    Schema.ArticleCrawling.put(articleKey).value(_.url,url).value(_.date,now).execute()
    Schema.ArticleCrawling.query2.withKey(articleKey).withColumns(_.url,_.date).singleOption(skipCache = false) match {
      case Some(thing) => {
        val retrievedUrl = thing.column(_.url).get
        assert(url == retrievedUrl)
        val retrievedDate = thing.column(_.date).get
        assert(now == retrievedDate)
        Schema.ArticleCrawling.delete(articleKey).execute()
        Schema.ArticleCrawling.query2.withKey(articleKey).withColumn(_.url).singleOption() match {
          case Some(badThing) => { fail("record was not deleted") }
          case None => { }
        }
      }
      case None => { fail("Didn't get stored row back")  }
    }        
  }

  test("Article been crawled test") {
    val now = new DateTime()
    val url = "http://kitties.com/ArticleCrawlingServiceTest/ArticleService/hasArticleBeenCrawledTest/" + now.getMillisOfSecond
    val articleKey = ArticleKey(url)
    assert(!ArticleCrawlingService.hasArticleBeenCrawled(articleKey), "Test article was indicated as crawled. That shouldn't be!")
    ArticleCrawlingService.articleHasBeenCrawled(url)
    assert(ArticleCrawlingService.hasArticleBeenCrawled(articleKey), "Test article was NOT indicated as crawled after put! That shoudln't be!")
    ArticleCrawlingService.articleHasNotBeenCrawled(url)
    assert(!ArticleCrawlingService.hasArticleBeenCrawled(articleKey, true), "Test article was indicated as crawled after removing it. That shouldn't be!")
  }
}