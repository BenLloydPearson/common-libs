package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.operations.FieldConverters.CampaignMetaResponseConverter
import com.gravity.interests.jobs.intelligence._
import com.gravity.service.remoteoperations.TestRemoteOperationsClient
import com.gravity.test.operationsTesting
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.BaseScalaTest
import scala.collection.Map
import scalaz._, Scalaz._


/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 8/11/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class ArticleDataLiteRequestResponseTest extends BaseScalaTest with operationsTesting {
  val numServers = 6

  val servers = for(i <- 0 until numServers) yield TestRemoteOperationsClient.createServer("REMOTE_RECOS", Seq(new CacheComponent))

//  test("test campaign meta") {
//
////    val numSites = 5
////    val numCampaigns = 50
////    val totalCampaigns = numSites * numCampaigns
////
////    withSites(numSites) { sites =>
////      withCampaigns(numCampaigns, sites) { campaigns =>
////         val rows = campaigns.campaigns.map(_.campaign)
////        val map = rows.map(row => row.campaignKey -> row).toMap
////
////        val bytes = CampaignMetaResponseConverter.toBytes(CampaignMetaResponse(map))
////        println("got " + bytes.length)
////      }
////    }
//    val bytes = CampaignMetaResponseConverter.toBytes(CampaignMetaResponse(CampaignService.allCampaignMeta))
//    println("got " + bytes.length)
//
//  }

  test("test server requests") {
    val numServers = 6
    val numArticles = 100
    val servers = for(i <- 0 until numServers) yield TestRemoteOperationsClient.createServer("REMOTE_RECOS", Seq(new CacheComponent))

    withSites(1) { sites =>
      withArticles(numArticles, sites) { articles =>
        val articleKeys = articles.articles.map(art => ArticleKey(art.url)).toSet
        val (successes, failures) = ArticleDataLiteService.getFromCacheServer(articleKeys)
        assertResult(0)(failures.size)
        assertResult(numArticles)(successes.size)
        articleKeys.foreach(key => assert(successes.contains(key)))
      }
    }
    servers.map(_.stop())
  }



  test("test success serialization") {
    val cacher = Schema.Articles.cache.asInstanceOf[IntelligenceSchemaCacher[ArticlesTable, ArticleKey, ArticleRow]]

    withSites(1) { sites =>
      withArticles(20, sites) { articles =>
        val articleKeys = articles.articles.map(art => ArticleKey(art.url)).toSet
        val articleMapValidation = ArticleDataLiteService.get(articleKeys, false)
        articleMapValidation match {
          case Success(articleMap) =>
            val validationInBytes = articleMap.flatMap { case (key, value) =>
              cacher.toBytesTransformer(value) match {
                case Success(rowBytes) =>
                  Some(key -> rowBytes)
                case Failure(fails) =>
                  fail("Failed to serialize row " + key.articleId + ": " + fails)
                  None
              }
            }
            val response = ArticleDataLiteResponse(validationInBytes.successNel)
            response.getValidation(response.byteses.head) match {
              case Success(responseMap) =>
                assertResult(articleMap.size)(responseMap.size)
                responseMap.foreach { case (key, valueDation) => {
                  valueDation match {
                    case Success(responseRow) =>
                      articleMap.get(key) match {
                        case Some(articleRow) => assertResult(articleRow.toString)(responseRow.toString)
                        case None => fail("key in response didn't exist in original")
                      }
                    case Failure(fails) => fail(fails.toString())
                  }

                }
                }
              case Failure(fails) =>
                fail(fails.toString())
            }
          case Failure(fails) => fail(fails.toString())
        }
      }
    }
  }

  test("test failure serialization") {
    val failureResult =
      FailureResult("this is a test. but not of the emergency broadcasting system", new Exception("and this also has nothing to do with broadcasting"))
    val failureNel = failureResult.failureNel[Map[ArticleKey, Array[Byte]]]
    val response = ArticleDataLiteResponse(failureNel)
    response.getValidation(response.byteses.head) match {
      case Success(map) => fail("that was supposed to fail")
      case Failure(fails) => assertResult(failureResult.toString)(fails.head.toString)
    }
  }
}
