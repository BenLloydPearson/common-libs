package com.gravity.interests.jobs.intelligence.operations.users

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.algorithms.StoredGraphMergingAlgos
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestEnvironment
import com.gravity.interests.jobs.intelligence.operations.SiteService
import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.grvz._
import org.joda.time.DateTime


/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class UserSiteServiceTest extends BaseScalaTest with operationsTesting {


  test("Clickstream Graphing") {
    val siteGuid = "TESTUSERSERVICECLICKSTREAMGRAPHING"
    val userGuid = "TESTUSERSERVICECLICKSTREAMUSERGUID"
    val domainBase = "http://testclickstreamgraphing.com"

    val article1Url = domainBase + "1.html"
    makeArticle(article1Url, siteGuid, graph = StoredGraphExamples.adornmentsAndPetsGraph_immutable)

    val article2Url = domainBase + "2.html"
    makeArticle(article2Url, siteGuid, graph = StoredGraphExamples.catsAndAppliancesGraph_immutable)

    val article3Url = domainBase + "3.html"
    makeArticle(article3Url, siteGuid, graph = StoredGraphExamples.petsAndSportsGraph_immutable)

    val mergedGraph = StoredGraphMergingAlgos.StandardMerger.merge(StoredGraphExamples.catsAndAppliancesGraph_immutable :: StoredGraphExamples.adornmentsAndPetsGraph_immutable :: StoredGraphExamples.petsAndSportsGraph_immutable :: Nil).get

    val article1Key = ArticleKey(article1Url)
    val firstDate = new DateTime()
    val firstViews = Map(firstDate -> article1Key)
    val user = makeUser(userGuid, siteGuid, viewedArticles = firstViews)

    val urow = Schema.UserSites.query2.withKey(user).withFamilies(_.clickStream, _.meta).single()

    val usg = UserSiteService.graphClickstream(urow, SchemaTypes.CounterNoOp).orDie

    val firstGraph = usg.conceptGraph.get

    val article2Key = ArticleKey(article2Url)
    val article3Key = ArticleKey(article3Url)
    val thirdViewDate = firstDate.minusDays(1)
    val secondViews = Map(ClickStreamKey(firstDate, article2Key) -> 1l, ClickStreamKey(thirdViewDate, article3Key) -> 3l)
    Schema.UserSites.put(user).valueMap(_.clickStream, secondViews).execute()

    val urow2 = Schema.UserSites.query2.withKey(user).withFamilies(_.clickStream, _.meta).single()
    val usg2 = UserSiteService.graphClickstream(urow2, SchemaTypes.CounterNoOp).orDie

    urow2.viewedArticleKeys.size should be (3)

    val secondGraph = usg2.conceptGraph.get

    secondGraph.interests.size should be (mergedGraph.interests.size)
    secondGraph.topics.size should be (mergedGraph.topics.size)
    Schema.UserSites.delete(user).execute()

  }

  //This has a dependency on the existence of the node frequency file which isn't there in a unit test
  ignore("Article Merging With Concept Graph") {
    val siteGuid = SiteService.TECHCRUNCH
    val userGuid = "TESTUSERSERVICECLICKSTREAMUSERGUIDMERGINGCONCEPT2"

    val domainBase = "http://testclickstreamgraphing-merge.com"
    val article1Url = domainBase + "concept2.html"

    makeArticle(article1Url, siteGuid, graph = StoredGraphExamples.catsAndAppliancesGraph_immutable, conceptGraph = StoredGraphExamples.catsAndAppliancesGraph_immutable)

    val user = makeUser(userGuid, siteGuid, viewedArticles = Map())
    val urow = Schema.UserSites.query2.withKey(user).withFamilies(_.meta, _.storedGraphs).single()
    val usg = UserSiteService.mergeArticleIntoUserGraph(urow, ArticleKey(article1Url), SchemaTypes.CounterNoOp).orDie

    usg.conceptGraph.isDefined should be (true)
    usg.conceptGraph.get.nodes.size shouldBe > (0)
  }

  //This has a dependency on the existence of the node frequency file which isn't there in a unit test
  ignore("Article Merging") {
    val siteGuid = "TESTUSERSERVICECLICKSTREAMGRAPHINGMERGING"
    val userGuid = "TESTUSERSERVICECLICKSTREAMUSERGUIDMERGING"
    val domainBase = "http://testclickstreamgraphing-merge.com"

    val article1Url = domainBase + "1.html"
    makeArticle(article1Url, siteGuid, graph = StoredGraphExamples.adornmentsAndPetsGraph_immutable)

    val article2Url = domainBase + "2.html"
    makeArticle(article2Url, siteGuid, graph = StoredGraphExamples.catsAndAppliancesGraph_immutable)

    val article3Url = domainBase + "3.html"
    makeArticle(article3Url, siteGuid, graph = StoredGraphExamples.petsAndSportsGraph_immutable)

    val mergedGraph = StoredGraphMergingAlgos.StandardMerger.merge(StoredGraphExamples.catsAndAppliancesGraph_immutable :: StoredGraphExamples.adornmentsAndPetsGraph_immutable :: StoredGraphExamples.petsAndSportsGraph_immutable :: Nil).get

    val user = makeUser(userGuid, siteGuid, viewedArticles = Map())

    val urow = Schema.UserSites.query2.withKey(user).withFamilies(_.meta, _.storedGraphs).single()
    val usg = UserSiteService.mergeArticleIntoUserGraph(urow, ArticleKey(article1Url), SchemaTypes.CounterNoOp).orDie
    Schema.UserSites.put(user).value(_.conceptStoredGraph, usg.conceptGraph.get).execute()

    val firstGraph = usg.conceptGraph.get

    val urow2 = Schema.UserSites.query2.withKey(user).withFamilies(_.meta, _.storedGraphs).single()
    val usg2 = UserSiteService.mergeArticleIntoUserGraph(urow2, ArticleKey(article2Url), SchemaTypes.CounterNoOp).orDie
    Schema.UserSites.put(user).value(_.conceptStoredGraph, usg2.conceptGraph.get).execute()

    val urow3 = Schema.UserSites.query2.withKey(user).withFamilies(_.meta, _.storedGraphs).single()
    val usg3 = UserSiteService.mergeArticleIntoUserGraph(urow3, ArticleKey(article3Url), SchemaTypes.CounterNoOp).orDie
    Schema.UserSites.put(user).value(_.conceptStoredGraph, usg3.conceptGraph.get).execute()

    val urow4 = Schema.UserSites.query2.withKey(user).withColumns(_.conceptStoredGraph).single()

    val secondGraph = urow4.conceptGraph

    secondGraph.interests.size should be (mergedGraph.interests.size)
    secondGraph.topics.size should be (mergedGraph.topics.size)
    Schema.UserSites.delete(user).execute()

    secondGraph.prettyPrint()

  }

  test("Boolean Value On Do Not Track") {

    val userGuid = "USERSERVICETESTBLAHBLAH"
    val siteGuid = "BLAHBLAHMOREUSERTESTBLAH"
    val userKey = UserSiteKey(userGuid, siteGuid)
    Schema.UserSites.put(userKey).value(_.doNotTrack, true).execute()

    val resultantUser = Schema.UserSites.query2.withKey(userKey).withColumns(_.doNotTrack).single()

    resultantUser.column(_.doNotTrack).get should be (true)


    val userGuid2 = "USERSERVICETESTBLAHBLAH2"
    val siteGuid2 = "BLAHBLAHMOREUSERTESTBLAH2"
    val userKey2 = UserSiteKey(userGuid2, siteGuid2)
    Schema.UserSites.put(userKey).value(_.doNotTrack, false).execute()

    val resultantUser2 = Schema.UserSites.query2.withKey(userKey).withColumns(_.doNotTrack).single()

    resultantUser2.column(_.doNotTrack).get should be (false)

  }

  test("Accept Reject Articles") {
    var success = 0
    var failure = 0
    var firstFailure = -1

    for (i <- 0 until 10) {
      try {
        println("Cleaning up before we begin to run test: testAcceptRejectArticles")
        val userGuid2 = "USERSERVICETESTtestAcceptRejectArticles_UG"
        val siteGuid2 = "USERSERVICETESTtestAcceptRejectArticles_SG"
        val userKey = UserSiteKey(userGuid2, siteGuid2)

        Schema.UserSites.delete(userKey).execute()

        println("Putting in userkey " + userKey)
        Schema.UserSites.put(userKey).value(_.userGuid, userGuid2).value(_.siteGuid, siteGuid2).execute()


        val article1 = ArticleKey("http://blah.com/USERSERVICETESTtestAcceptRejectArticles_art1")
        val article2 = ArticleKey("http://blah.com/USERSERVICETESTtestAcceptRejectArticles_art2")
        val article3 = ArticleKey("http://blah.com/USERSERVICETESTtestAcceptRejectArticles_art3")
        val article4 = ArticleKey("http://blah.com/USERSERVICETESTtestAcceptRejectArticles_art4")
        val article5 = ArticleKey("http://blah.com/USERSERVICETESTtestAcceptRejectArticles_art5")
        val article6 = ArticleKey("http://blah.com/USERSERVICETESTtestAcceptRejectArticles_art6")
        val article7 = ArticleKey("http://blah.com/USERSERVICETESTtestAcceptRejectArticles_art7")

        Schema.UserSites.query2.withKey(userKey).withColumn(_.userGuid).singleOption()

        println("Printing out the user row and the userRelationships family ('ur') BEFORE actions should be empty:")
        Schema.UserSites.query2.withKey(userKey).withFamilies(_.meta, _.userRelationships).single().prettyPrint()

        val val1 = UserSiteService.rejectArticle(userKey, article1)
        val1.isSuccess should be (true)
        val val2 = UserSiteService.rejectArticle(userKey, article3)
        val2.isSuccess should be (true)

        UserSiteService.acceptArticle(userKey, article3) should be('success)
        UserSiteService.acceptArticle(userKey, article5) should be('success)
        UserSiteService.acceptArticle(userKey, article6) should be('success)
        UserSiteService.rejectArticle(userKey, article6) should be('success)
        UserSiteService.actOnArticle(userKey, article7, UserRelationships.stared) should be('success)
        UserSiteService.actOnArticle(userKey, article7, UserRelationships.stared, isDelete = true) should be('success)

        println()
        println("Article Keys:")
        println("article1: " + article1)
        println("article2: " + article2)
        println("article3: " + article3)
        println("article4: " + article4)
        println("article5: " + article5)
        println("article6: " + article6)
        println("article7: " + article7)
        println()

        println("Printing out the user row and the userRelationships family ('ur') AFTER actions should be populated:")
        Schema.UserSites.query2.withKey(userKey).withFamilies(_.meta, _.userRelationships).single().prettyPrint()

        val userArticleActions = UserSiteService.getUserArticleActions(userKey) | UserArticleActions.zero
        val rejectedList = userArticleActions.rejectedArticles
        val acceptedList = userArticleActions.acceptedArticles

        println("\niteration i: " + i + "\n")

        rejectedList should contain(article1)
        rejectedList should not(contain(article2))
        rejectedList should not(contain(article4))

        rejectedList should not(contain(article3))
        acceptedList should contain(article3)

        rejectedList should not(contain(article5))
        acceptedList should contain(article5)

        acceptedList should not(contain(article6))
        rejectedList should contain(article6)

        acceptedList should not(contain(article7))
        rejectedList should not(contain(article7))
        success += 1
      } catch {
        case ex:Throwable => {
          ex.printStackTrace()
          failure += 1
          if (firstFailure == -1) {
            firstFailure = i
          }
        }
      }
    }

    println("success: " + success)
    println("failures: " + failure)
    println("first failure: " + firstFailure)
  }
}