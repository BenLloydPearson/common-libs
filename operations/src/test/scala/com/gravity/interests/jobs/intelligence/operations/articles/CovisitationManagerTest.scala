package com.gravity.interests.jobs.intelligence.operations.articles

import com.gravity.interests.jobs.intelligence.{ArticleKey, Schema}
import com.gravity.test.operationsTesting
import com.gravity.utilities.{BaseScalaTest, grvtime}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
class CovisitationManagerTest extends BaseScalaTest with operationsTesting {
  val manager = new CovisitationManager {}

//  test("testCovisitationFromBeacons") {
//    val beaconStr = makeBeaconString(grvtime.currentTime)
//    val beacon = BeaconEvent.fromTokenizedString(beaconStr)
//
//    manager.covisitFromBeacon(beacon) match {
//      case Success(successResult) => {
//        beacon.standardizedUrl match {
//          case Some(url) => {
//            Schema.Articles.query2.withKey(ArticleKey(url)).withAllColumns.singleOption().get.prettyPrint()
//          }
//          case None => Assert.fail("Should not have happened")
//        }
//      }
//      case Failure(fails) => Assert.fail("Covisit failed because of " + fails.toString)
//    }
//  }

  test("testCovisitationFetching") {
    def makeurl(name: String) = "http://testcovisitation.com/" + name

    val sourceArticleUrls = Seq("source1.html", "source2.html").map(makeurl)
    val sourceArticleKeys = sourceArticleUrls.map(ArticleKey(_))
    val destUrls = Seq("dest1.html", "dest2.html", "dest3.html", "dest4.html").map(makeurl).zipWithIndex


    destUrls.foreach {
      case (destUrl, hoursOld) =>
        val hour = grvtime.hoursAgoAsDateHour(hoursOld)
        sourceArticleKeys.foreach {
          sourceArticleKey =>
            manager.covisit(sourceArticleKey, ArticleKey(destUrl), hour)
        }
    }

    for {
      events <- manager.fetchCovisitedArticlesByKey(sourceArticleUrls.map(url => ArticleKey(url)).toSet, 2)
    } {
      events.foreach {
        event => println(event.toString)
      }
    }
  }

  test("testCovisitationSaving") {
    def makeurl(name: String) = "http://testcovisitation.com/" + name

    val covisitedArticleKeys = Seq("cart1.html", "cart2.html", "cart3.html", "cart4.html").map(makeurl).map(ArticleKey(_))

    Seq(
      "art1.html",
      "art2.html",
      "art3.html",
      "art4.html"
    ) map {
      url => makeArticle(makeurl(url))
    } map {
      articleKey =>
        manager.covisit(articleKey, covisitedArticleKeys.head)
        articleKey
    } foreach {
      ak =>
      //Schema.Articles.query2.withKey(ak).withAllColumns.singleOption().get.prettyPrint()

        val article = Schema.Articles.query2.withKey(ak).withAllColumns.singleOption().get
        println(article.covisitationEvents)
        println(article.covisitationIncoming)
        println(article.covisitationOutgoing)
    }

    //Schema.Articles.query2.withKey(covisitedArticleKeys(0)).withAllColumns.singleOption().get.prettyPrint()

    val article0 = Schema.Articles.query2.withKey(covisitedArticleKeys.head).withAllColumns.singleOption().get
    println(article0.covisitationEvents)
    println(article0.covisitationIncoming)
    println(article0.covisitationOutgoing)

  }
}
