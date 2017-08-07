package com.gravity.interests.jobs.intelligence.operations.sites

import com.gravity.interests.jobs.intelligence.operations.{ArticleGraphDispatcher, ArticleService}
import com.gravity.interests.jobs.intelligence.{ArticleKey, Schema}
import com.gravity.service.remoteoperations.TestRemoteOperationsDispatcher
import com.gravity.test.operationsTesting
import com.gravity.utilities.{BaseScalaTest, HashUtils}
import org.joda.time.DateTime

import scalaz._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class ArticleServiceTest extends BaseScalaTest with operationsTesting {

  val siteGuid: String = HashUtils.md5("ARTICLESERVICETESTSITEGUID394")

//  test("testOutputFromProd") {
//    val beacon = "2011-06-06 02:56:39^publish^6e1ea1b081dc6743bbe3537728eca43d^^http://www.scribd.com/doc/50260426/Lotus-Eye-Care-Hospital^^^^^^^^^Lotus Eye Care Hospital^^^^^^^1312354800000^^^^"
//
//    val mag = BeaconEvent.fromTokenizedString(beacon)
//
//    mag.getPublishedDateOption match {
//      case Some(yep) => println("Got it: " + yep)
//      case None => println("Nope")
//    }
//  }

  test("testSaveGooseArticle") {
    val article = new com.gravity.goose.Article
    val expectedUrl = "http://www.example.com/examples/article.html"

    article.finalUrl = "http://www.example.com"
    article.canonicalLink = "examples/article.html"
    article.publishDate = new DateTime().withYear(2013).withMonthOfYear(6).withDayOfMonth(10).toDate
    article.title = "Example Article"
    article.cleanedArticleText = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nam ornare velit sit amet adipiscing euismod. Pellentesque sed metus id purus pretium cursus at a velit. Vestibulum in nisi eget mauris venenatis viverra a dapibus erat. Nam vel enim nibh. Sed feugiat libero libero, quis iaculis neque placerat et. Aliquam nec lacus libero. Proin pharetra risus quis quam faucibus, sit amet porttitor tellus condimentum. Vestibulum non porta augue, ut semper nulla. Etiam tristique dolor turpis, porttitor."
    article.tags = Set("Examples", "Articles")
    val author = "John Doe"
    val authorLink = "http://www.example.com/authors/jdoe.html"
    val attributionName = "Attribution Example"
    val attributionLogo = "http://www.example.com/logos/attribution.png"
    val attributionSite = "example.com"
    val isPaid = true
    article.additionalData = Map(
      "author" -> author,
      "authorLink" -> authorLink,
      "attribution.name" -> attributionName,
      "attribution.logo" -> attributionLogo,
      "attribution.site" -> attributionSite,
      "isPaid" -> isPaid.toString
    )

    Schema.Articles.delete(ArticleKey(expectedUrl)).execute()

    TestArticleService.saveGooseArticle(article, siteGuid, _.withFamilies(_.meta, _.text), sendGraphingRequest = false) match {
      case Success(savedArticle) =>
        println("YAY! Successfully saved goose article for URL: " + expectedUrl)
        assert(expectedUrl == savedArticle.url, "canonicalLink MUST equal!")
        assert(article.publishDate == savedArticle.publishTime.toDate, "publishDate MUST equal!")
        assert(article.title == savedArticle.title,"title MUST equal!")
        assert(article.cleanedArticleText == savedArticle.content, "text MUST equal!")
        assert(article.tags.mkString("`", "`, `", "`") == savedArticle.tags.mkString("`", "`, `", "`"),"tags MUST equal!")
        assert(author == savedArticle.author, "author MUST equal!")
        assert(authorLink == savedArticle.authorLink, "authorLink MUST equal!")
        assert(attributionName == savedArticle.attributionName, "attributionName MUST equal!")
        assert(attributionLogo == savedArticle.attributionLogo, "attributionLogo MUST equal!")
        assert(attributionSite == savedArticle.attributionSite, "attributionSite MUST equal!")
        assert(isPaid ==  savedArticle.isBehindPaywall, "isPaid MUST equal!")
      case Failure(fails) =>
        println(fails.list.map(_.toString).mkString("\n"))
        fail("Awwww bummah. We failed.")
    }
  }
}

trait TestArticleGraphDispatcher extends ArticleGraphDispatcher with TestRemoteOperationsDispatcher {

  def sendByDefault: Boolean = false

  def dispatchArticleGraphRequest(articleKey: ArticleKey) {
    println("TestArticleGraphDispatcher.dispatchArticleGraphRequest(" + articleKey + ")")
  }

  def dispatchArticleRegraphRequest(articleKey: ArticleKey) {
    println("TestArticleGraphDispatcher.dispatchArticleRegraphRequest(" + articleKey + ")")
  }

  def dispatchArticleRegraphRequestViaPersistentQueue(url: String) {
    println("TestArticleGraphDispatcher.dispatchArticleRegraphRequestViaPersistentQueue(\"" + url + "\")")
  }

  def dispatchArticleIndexRequestViaPersistentQueue(url: String) {
    println("TestArticleGraphDispatcher.dispatchArticleIndexRequestViaPersistentQueue(\"" + url + "\")")
  }

  def dispatchArticleDeleteFromIndexRequestViaPersistentQueue(url: String) {
    println("TestArticleGraphDispatcher.dispatchArticleIndexRequestViaPersistentQueue(\"" + url + "\")")
  }

}

object TestArticleService extends ArticleService with TestArticleGraphDispatcher