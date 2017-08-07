package com.gravity.interests.jobs.intelligence.operations

import com.gravity.data.configuration.{ConfigurationQueryService, DlPlacementSetting}
import com.gravity.domain.GrvDuration
import com.gravity.domain.aol._
import com.gravity.domain.gms.{GmsAlgoSettings, GmsArticleStatus}
import com.gravity.interests.jobs.intelligence.{PublishDateAndArticleKey, ArticleRow, ArticleKey, Schema}
import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvcoll._
import org.joda.time.DateTime
import play.api.libs.json._

import scala.collection._
import scalaz.syntax.std.option._
import scalaz.{Failure, NonEmptyList, Success}

/**
 * Created with IntelliJ IDEA.
 * Author: robbie
 * Date: 12/16/14
 * Time: 11:34 AM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 *  \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
class AolDynamicLeadServiceTest extends BaseScalaTest with operationsTesting {
//  val secondaryLinks = List(AolLink(makeExampleUrl("link1"), "link1"), AolLink(makeExampleUrl("link2"), "link2"))
//
//  com.gravity.grvlogging.updateLoggerToTrace("com.gravity.interests.jobs.intelligence.operations.AolDynamicLeadService")
//
//  def failForFails(message: String, fails: NonEmptyList[FailureResult]): Unit = {
//    fail(message + " " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND "))
//  }
//
//  def ifOldStyleDlugEnabled[T](thunk: => T): Unit = {
//    if (GmsAlgoSettings.aolComDlugUsesMultisiteGms == false)
//      thunk
//    else
//      info("Skipped, old-style DLUG is disabled.")
//  }
//
//  test("expiring articles") {
//    ifOldStyleDlugEnabled {
//      val startDate = new DateTime().minusHours(3).withMillis(0L).some
//      val duration = GrvDuration(0, 0, 2).some
//
//      createDlArticle("Expiring Article", duration = duration) match {
//        case Success(dlArticle) =>
//          dlArticle.duration should equal (duration)
//          dlArticle.startDate should be ('empty)
//          dlArticle.endDate should be ('empty)
//
//          AolDynamicLeadService.updateDlArticleStatus(dlArticle.articleKey, GmsArticleStatus.Live, 307L).valueOr {
//            case fails: NonEmptyList[FailureResult] => failForFails("Failed to update status to Live", fails)
//          }
//
//          AolDynamicLeadService.getArticle(dlArticle.articleKey.articleId) match {
//            case Success(updatedDlArticle) =>
//              trace("Updated article: {0}", updatedDlArticle)
//              updatedDlArticle.articleStatus should equal (GmsArticleStatus.Live)
//              updatedDlArticle.startDate should be ('defined)
//
//            case Failure(fails) => failForFails("Failed to retrieve DL Article to check its update", fails)
//          }
//
//          AolDynamicLeadService.checkIfRecommendableAndUpdateIfNeeded(dlArticle.articleKey, dlArticle.url, GmsArticleStatus.Live, startDate, None, duration, executeUpdate = true) match {
//            case Success(isStillRecommendable) =>
//              isStillRecommendable should be (false)
//
//            case Failure(fails) => failForFails("Failed to checkIfRecommendableAndUpdateIfNeeded", fails)
//          }
//
//        case Failure(fails) => failForFails("Failed to create unit: ", fails)
//      }
//    }
//  }
//
//  test("test channels") {
//    ifOldStyleDlugEnabled {
//      def checkIfArticleIsInChannelCampaign(ak: ArticleKey, channel: AolDynamicLeadChannels.Type): Boolean = {
//        Schema.Campaigns.query2.withKey(channel.campaignKey).withFamilies(_.recentArticles).singleOption().exists(_.recentArticles.values.toSet.contains(ak))
//      }
//
//      val initialChannels = Set(AolDynamicLeadChannels.Home, AolDynamicLeadChannels.News, AolDynamicLeadChannels.Entertainment)
//      createDlArticle("test channels", initialChannels, secondaryLinks) match {
//        case Success(article) =>
//          article.channels should equal (initialChannels)
//
//          withClue("Article should be bound to the `Home` channel campaign") {
//            checkIfArticleIsInChannelCampaign(article.articleKey, AolDynamicLeadChannels.Home) should be (true)
//          }
//
//          withClue("Article should be bound to the `News` channel campaign") {
//            checkIfArticleIsInChannelCampaign(article.articleKey, AolDynamicLeadChannels.News) should be (true)
//          }
//
//          withClue("Article should be bound to the `Entertainment` channel campaign") {
//            checkIfArticleIsInChannelCampaign(article.articleKey, AolDynamicLeadChannels.Entertainment) should be (true)
//          }
//
//          // now let's remove just the Entertainment channel
//          val channelsSansEntertainment = Set(AolDynamicLeadChannels.Home, AolDynamicLeadChannels.News)
//
//          val fields = AolDynamicLeadModifyFields(article.title, article.categoryText, article.categoryUrl,
//            article.aolCategoryId.getOrElse(0), article.aolCategorySlug.getOrElse(""), article.sourceText, article.sourceUrl,
//            article.summary, article.headline, article.secondaryHeader, article.secondaryLinks, article.imageUrl,
//            article.showVideoIcon, article.aolCampaign, article.startDate, article.endDate, None, None,
//            article.aolRibbonImageUrl, article.aolImageSource,
//            article.channelImage.map(i => ImageWithCredit(i, article.channelImageSource)), channelsSansEntertainment,
//            article.narrowBandImage, article.channelsToPinned)
//
//          AolDynamicLeadService.saveDynamicLeadArticle(article.url, fields, 0L).value match {
//            case Success(modifiedArticle) =>
//              modifiedArticle.channels should equal (channelsSansEntertainment)
//
//              withClue("Article should be bound to the `Home` channel campaign") {
//                checkIfArticleIsInChannelCampaign(article.articleKey, AolDynamicLeadChannels.Home) should be (true)
//              }
//
//              withClue("Article should be bound to the `News` channel campaign") {
//                checkIfArticleIsInChannelCampaign(article.articleKey, AolDynamicLeadChannels.News) should be (true)
//              }
//
//              withClue("Article should NOT be bound to the `Entertainment` channel campaign") {
//                checkIfArticleIsInChannelCampaign(article.articleKey, AolDynamicLeadChannels.Entertainment) should be (false)
//              }
//
//            case Failure(fails) =>
//              fail("Failed to update unit to remove one channel of 2: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND "))
//          }
//
//          // now let's test removing all channels
//          val fieldsForNoChannels = AolDynamicLeadModifyFields(article.title, article.categoryText, article.categoryUrl,
//            article.aolCategoryId.getOrElse(0), article.aolCategorySlug.getOrElse(""), article.sourceText, article.sourceUrl,
//            article.summary, article.headline, article.secondaryHeader, article.secondaryLinks, article.imageUrl,
//            article.showVideoIcon, article.aolCampaign, article.startDate, article.endDate, None, None,
//            article.aolRibbonImageUrl, article.aolImageSource,
//            article.channelImage.map(i => ImageWithCredit(i, article.channelImageSource)), Set.empty[AolDynamicLeadChannels.Type],
//            article.narrowBandImage, article.channelsToPinned, Set(AolDynamicLeadFieldNames.Channels))
//
//          AolDynamicLeadService.saveDynamicLeadArticle(article.url, fieldsForNoChannels, 0L).value match {
//            case Success(articleThatShouldHaveFailed) =>
//              fail("All DL Articles MUST have at least one channel. This article should have failed: " + articleThatShouldHaveFailed)
//
//            case Failure(fails) =>
//              fails.list should have size (1)
//              fails.head should equal (AolDynamicLeadService.noChannelsFailureResult)
//
//          }
//
//        case Failure(fails) =>
//          failForFails("Failed to create unit: ", fails)
//      }
//    }
//  }
//
//  test("test updating pins") {
//    ifOldStyleDlugEnabled {
//      // pinnedArticlesBySitePlacementIdMap is disabled when aolComDlugUsesMultisiteGms is true.
//      if (!GmsAlgoSettings.aolComDlugUsesMultisiteGms) {
//        def stripAdFromResults(pinMap: Map[Int, ArticleKey], channel: AolDynamicLeadChannels.Type): Map[Int, ArticleKey] = DlPlacementSetting.adUnitSlot(channel) match {  // adUnitSlot's use is avoided by aolComDlugUsesMultisiteGms algoSetting.
//          case Some(adSlot) => pinMap - adSlot
//          case None => pinMap
//        }
//
//        // create 10 units per channel (50 total)
//        val homeUnits = createDlArticlesInChannel("Home Article updating", 10, secondaryLinks = secondaryLinks)
//        val newsUnits = createDlArticlesInChannel("News Article updating", 10, Set(AolDynamicLeadChannels.News), secondaryLinks = secondaryLinks)
//        val entertainmentUnits = createDlArticlesInChannel("Entertainment Article updating", 10, Set(AolDynamicLeadChannels.Entertainment), secondaryLinks = secondaryLinks)
//        val financeUnits = createDlArticlesInChannel("Finance Article updating", 10, Set(AolDynamicLeadChannels.Finance), secondaryLinks = secondaryLinks)
//        val lifestyleUnits = createDlArticlesInChannel("Lifestyle Article updating", 10, Set(AolDynamicLeadChannels.Lifestyle), secondaryLinks = secondaryLinks)
//        val sportsUnits = createDlArticlesInChannel("Sports Article updating", 10, Set(AolDynamicLeadChannels.Sports), secondaryLinks = secondaryLinks)
//
//        // create maps of pins for each channel
//        val homePins = stripAdFromResults(homeUnits.take(20).zipWithIndex.map(tup => tup._2 -> tup._1.articleKey).toMap, AolDynamicLeadChannels.Home)
//        val newsPins = stripAdFromResults(newsUnits.take(8).zipWithIndex.map(tup => tup._2 -> tup._1.articleKey).toMap, AolDynamicLeadChannels.News)
//        val entertainmentPins = stripAdFromResults(entertainmentUnits.take(5).zipWithIndex.map(tup => tup._2 -> tup._1.articleKey).toMap, AolDynamicLeadChannels.Entertainment)
//        val financePins = stripAdFromResults(financeUnits.take(1).zipWithIndex.map(tup => tup._2 -> tup._1.articleKey).toMap, AolDynamicLeadChannels.Finance)
//        val lifestylePins = stripAdFromResults(lifestyleUnits.take(2).zipWithIndex.map(tup => tup._2 -> tup._1.articleKey).toMap, AolDynamicLeadChannels.Lifestyle)
//        val sportsPins = stripAdFromResults(sportsUnits.take(3).zipWithIndex.map(tup => tup._2 -> tup._1.articleKey).toMap, AolDynamicLeadChannels.Sports)
//
//        // combined expected map
//        val expectedPins = {
//          val homeMap = convertDlPinsToPinMap(AolDynamicLeadChannels.Home, homePins)
//          val newsMap = convertDlPinsToPinMap(AolDynamicLeadChannels.News, newsPins)
//          val entertainmentMap = convertDlPinsToPinMap(AolDynamicLeadChannels.Entertainment, entertainmentPins)
//          val financeMap = convertDlPinsToPinMap(AolDynamicLeadChannels.Finance, financePins)
//          val lifestyleMap = convertDlPinsToPinMap(AolDynamicLeadChannels.Lifestyle, lifestylePins)
//          val sportsMap = convertDlPinsToPinMap(AolDynamicLeadChannels.Sports, sportsPins)
//
//          val allKeys = homeMap.keySet ++ newsMap.keySet ++ entertainmentMap.keySet ++ financeMap.keySet ++ lifestyleMap.keySet ++ sportsMap.keySet
//
//          (for (ak <- allKeys) yield {
//            ak -> (homeMap.getOrElse(ak, Nil) ++ newsMap.getOrElse(ak, Nil) ++ entertainmentMap.getOrElse(ak, Nil) ++ financeMap.getOrElse(ak, Nil) ++ lifestyleMap.getOrElse(ak, Nil) ++ sportsMap.getOrElse(ak, Nil))
//          }).toMap
//        }
//
//        // set pins for each channel
//        AolDynamicLeadService.updateChannelPins(AolDynamicLeadChannels.Home, homePins).valueOr(fails => fail("Failed to pin NotSet: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))
//        AolDynamicLeadService.updateChannelPins(AolDynamicLeadChannels.News, newsPins).valueOr(fails => fail("Failed to pin News: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))
//        AolDynamicLeadService.updateChannelPins(AolDynamicLeadChannels.Entertainment, entertainmentPins).valueOr(fails => fail("Failed to pin Entertainment: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))
//        AolDynamicLeadService.updateChannelPins(AolDynamicLeadChannels.Finance, financePins).valueOr(fails => fail("Failed to pin Finance: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))
//        AolDynamicLeadService.updateChannelPins(AolDynamicLeadChannels.Lifestyle, lifestylePins).valueOr(fails => fail("Failed to pin Lifestyle: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))
//        AolDynamicLeadService.updateChannelPins(AolDynamicLeadChannels.Sports, sportsPins).valueOr(fails => fail("Failed to pin Sports: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))
//
//        PermaCacher.restart()
//
//        // pull all to verify what was stored
//        val allPins = ConfigurationQueryService.queryRunner.getAllPinnedArticles
//        val articleKeyToPinsMap = allPins.groupBy(_.articleKey)
//          .mapValues(_.map(row => ChannelToPinnedSlot(row.channel, row.slot)))
//
//        // validate MySql Data
//        withClue("Expected Pin Mappings Should Equal resulting pin mappings") {
//          articleKeyToPinsMap should equal (expectedPins)
//        }
//
//        val dlArticleMap = AolDynamicLeadService.getAllArticlesFromHbase().map(_.map(dla => dla.articleKey -> dla).toMap).valueOr {
//          fails =>
//            fail("Failed retrieve all DL Units!: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND "))
//            Map.empty[ArticleKey, AolDynamicLeadArticle]
//        }
//
//        println("MySql Data Verification complete. Now for GrvMap data from hbase...")
//
//        val allKeys = homeUnits.map(_.articleKey).toSet ++
//          newsUnits.map(_.articleKey).toSet ++
//          entertainmentUnits.map(_.articleKey).toSet ++
//          financeUnits.map(_.articleKey).toSet ++
//          lifestyleUnits.map(_.articleKey).toSet ++
//          sportsUnits.map(_.articleKey).toSet
//
//        // validate each GrvMap based pin data
//        for {
//          ak <- allKeys
//          expectedChannelPins = expectedPins.getOrElse(ak, Nil)
//        } {
//          dlArticleMap.get(ak) match {
//            case Some(dlArticle) =>
//              withClue("Expected Pin Mappings Should Equal resulting GrvMap pin mappings for:" + ak) {
//                dlArticle.channelsToPinned.sortBy(c2p => c2p.channel.id -> c2p.slot) should equal (expectedChannelPins.sortBy(c2p => c2p.channel.id -> c2p.slot))
//              }
//
//            case None =>
//              fail("Generated DL Unit Should Be Present In `AolDynamicLeadService.getArticles()` result. Not Found: " + ak)
//          }
//        }
//      }
//    }
//  }
//
//  test("unpinning article") {
//    ifOldStyleDlugEnabled {
//      // pinnedArticlesBySitePlacementIdMap is disabled when aolComDlugUsesMultisiteGms is true.
//      if (!GmsAlgoSettings.aolComDlugUsesMultisiteGms) {
//        val homeUnits = createDlArticlesInChannel("Home Article unpinning", 10, secondaryLinks = secondaryLinks)
//
//        homeUnits should have length (10)
//
//        val allKeysForThisTest = homeUnits.map(_.articleKey).toSet
//
//        val firstUnit = homeUnits.head
//        AolDynamicLeadService.updateChannelPins(AolDynamicLeadChannels.Home, Map(1 -> firstUnit.articleKey)).valueOr(fails => fail("Failed to pin NotSet: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))
//
//        // verify MySql for this pin.
//        val allPins = ConfigurationQueryService.queryRunner.getAllPinnedArticles
//
//        allPins should have length (1)
//        val pinnedHead = allPins.head
//        pinnedHead.articleKey should equal(firstUnit.articleKey)
//        pinnedHead.channel should equal(AolDynamicLeadChannels.Home)
//        pinnedHead.slot should equal(1)
//
//        PermaCacher.restart()
//
//        val dlArticleMap = AolDynamicLeadService.getAllArticlesFromHbase().map(_.map(dla => dla.articleKey -> dla).toMap).valueOr {
//          fails =>
//            failForFails("Failed retrieve all DL Units!: ", fails)
//            Map.empty[ArticleKey, AolDynamicLeadArticle]
//        }
//
//        dlArticleMap.get(firstUnit.articleKey) match {
//          case Some(dlArticle) =>
//            dlArticle.channelsToPinned should have length (1)
//            val c2p = dlArticle.channelsToPinned.head
//            c2p.channel should equal(AolDynamicLeadChannels.Home)
//            c2p.slot should equal(1)
//          case None =>
//            fail("Pinned article was not found in hbase result!")
//        }
//
//        AolDynamicLeadService.updateChannelPins(AolDynamicLeadChannels.Home, Map.empty[Int, ArticleKey]).valueOr(fails => failForFails("Failed to UNpin NotSet: ", fails))
//
//        val postUnpinningAllPins = ConfigurationQueryService.queryRunner.getAllPinnedArticles
//        postUnpinningAllPins should have length (0)
//
//        PermaCacher.restart()
//
//        val dlArticleMapAfter = AolDynamicLeadService.getAllArticlesFromHbase().map(_.map(dla => dla.articleKey -> dla).toMap).valueOr {
//          fails =>
//            fail("Failed RE-retrieve all DL Units!: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND "))
//            Map.empty[ArticleKey, AolDynamicLeadArticle]
//        }
//
//        for ((ak, dlArticle) <- dlArticleMapAfter.filterKeys(allKeysForThisTest.contains)) {
//          withClue("Nothing should be pinned in GrvMap since we unpinned. DL Article: " + dlArticle.toString) {
//            val homePins = dlArticle.channelsToPinned.filter(_.channel == AolDynamicLeadChannels.Home)
//            homePins should have length (0)
//          }
//        }
//      }
//    }
//  }
//
//  test("grvMap channel to pin") {
//    ifOldStyleDlugEnabled {
//      // pinnedArticlesBySitePlacementIdMap is disabled when aolComDlugUsesMultisiteGms is true.
//      if (!GmsAlgoSettings.aolComDlugUsesMultisiteGms) {
//        /*
//           TITLE                             CHANNELS               PIN INFO
//           Non-Pinned Home Article 1         Home                   --
//           Non-Pinned Home Article 2         Home                   --
//           Pinned Home Article 1             Home, Entertainment    Home -> 3
//           Pinned Home Article 2             Home, Entertainment    Home -> 7
//           Pinned Entertainment Article 1    Home, Entertainment    Entertainment -> 2
//           Pinned Entertainment Article 2    Home, Entertainment    Entertainment -> 7, Home -> 8
//         */
//        val nonPinnedHomeArticles = createDlArticlesInChannel("Non-Pinned Home Article", 2, Set(AolDynamicLeadChannels.Home))
//        val pinnedHomeArticles = createDlArticlesInChannel("Pinned Home Article", 2, Set(AolDynamicLeadChannels.Home, AolDynamicLeadChannels.Entertainment))
//        val pinnedEntertainmentArticles = createDlArticlesInChannel("Pinned Entertainment Article", 2, Set(AolDynamicLeadChannels.Home, AolDynamicLeadChannels.Entertainment))
//
//        AolDynamicLeadService.updateChannelPins(
//          AolDynamicLeadChannels.Home,
//          Seq(3, 7, 8).zip(pinnedHomeArticles :+ pinnedEntertainmentArticles.last).toMap.mapValues(_.articleKey)
//        ) should be('success)
//
//        AolDynamicLeadService.updateChannelPins(
//          AolDynamicLeadChannels.Entertainment,
//          Seq(2, 7).zip(pinnedEntertainmentArticles).toMap.mapValues(_.articleKey)
//        ) should be('success)
//
//        val articles = nonPinnedHomeArticles ++ pinnedHomeArticles ++ pinnedEntertainmentArticles
//        val aks = articles.map(_.articleKey).toSet
//
//        def validateDlugArticlesFromHbase(afterStateChange: Boolean) {
//          PermaCacher.restart()
//
//          val articlesFromHbaseV = AolDynamicLeadService.getSelectedArticlesFromHbase(aks.toSeq, withMetrics = false)
//          articlesFromHbaseV should be('success)
//          articlesFromHbaseV.foreach {
//            case articlesFromHbase =>
//              articlesFromHbase should have length articles.length
//              val articlesByKey = articlesFromHbase.mapBy(_.articleKey)
//
//              val nonPinnedHomeArticle1 = articlesByKey(nonPinnedHomeArticles.head.articleKey)
//              val nonPinnedHomeArticle2 = articlesByKey(nonPinnedHomeArticles.last.articleKey)
//              nonPinnedHomeArticle1.title should be("Non-Pinned Home Article 1")
//              nonPinnedHomeArticle2.title should be("Non-Pinned Home Article 2")
//              nonPinnedHomeArticle1.channels should be(Set(AolDynamicLeadChannels.Home))
//              nonPinnedHomeArticle2.channels should be(Set(AolDynamicLeadChannels.Home))
//              nonPinnedHomeArticle1.channelsToPinned should be('empty)
//              nonPinnedHomeArticle2.channelsToPinned should be('empty)
//
//              val pinnedHomeArticle1 = articlesByKey(pinnedHomeArticles.head.articleKey)
//              val pinnedHomeArticle2 = articlesByKey(pinnedHomeArticles.last.articleKey)
//              pinnedHomeArticle1.title should be("Pinned Home Article 1")
//              pinnedHomeArticle2.title should be("Pinned Home Article 2")
//              pinnedHomeArticle1.channels should be(Set(AolDynamicLeadChannels.Home, AolDynamicLeadChannels.Entertainment))
//              pinnedHomeArticle2.channels should be(Set(AolDynamicLeadChannels.Home, AolDynamicLeadChannels.Entertainment))
//              pinnedHomeArticle1.channelsToPinned should be(List(ChannelToPinnedSlot(AolDynamicLeadChannels.Home, 3)))
//              if(afterStateChange)
//                pinnedHomeArticle2.channelsToPinned should be('empty)
//              else
//                pinnedHomeArticle2.channelsToPinned should be(List(ChannelToPinnedSlot(AolDynamicLeadChannels.Home, 7)))
//
//              val pinnedEntertainmentArticle1 = articlesByKey(pinnedEntertainmentArticles.head.articleKey)
//              val pinnedEntertainmentArticle2 = articlesByKey(pinnedEntertainmentArticles.last.articleKey)
//              pinnedEntertainmentArticle1.title should be("Pinned Entertainment Article 1")
//              pinnedEntertainmentArticle2.title should be("Pinned Entertainment Article 2")
//              pinnedEntertainmentArticle1.channels should be(Set(AolDynamicLeadChannels.Home, AolDynamicLeadChannels.Entertainment))
//              pinnedEntertainmentArticle2.channels should be(Set(AolDynamicLeadChannels.Home, AolDynamicLeadChannels.Entertainment))
//              pinnedEntertainmentArticle1.channelsToPinned should be(List(ChannelToPinnedSlot(AolDynamicLeadChannels.Entertainment, 2)))
//              pinnedEntertainmentArticle2.channelsToPinned should be(List(ChannelToPinnedSlot(AolDynamicLeadChannels.Entertainment, 7),
//                ChannelToPinnedSlot(AolDynamicLeadChannels.Home, 8)))
//          }
//        }
//        validateDlugArticlesFromHbase(afterStateChange = false)
//
//        // Unpin one of the home articles and ensure structural integrity all around
//        AolDynamicLeadService.updateChannelPins(
//          AolDynamicLeadChannels.Home,
//          Seq(3, 8).zip(pinnedHomeArticles.head +: pinnedEntertainmentArticles.tail).toMap.mapValues(_.articleKey)
//        ) should be('success)
//        validateDlugArticlesFromHbase(afterStateChange = true)
//      }
//    }
//  }
//
//  test("min articles") {
//    ifOldStyleDlugEnabled {
//      val channelToTest = AolDynamicLeadChannels.Home
//
//      val minArticles = DlPlacementSetting.minArticles(channelToTest)
//
//      minArticles should equal (4)
//    }
//  }
//
//  test("read AolChannelRibbon from legacy JSON") {
//    ifOldStyleDlugEnabled {
//      val expected = AolChannelRibbon.buildChannelRibbon(AolDynamicLeadChannels.Food)._2
//
//      val jsonString = """{"imageUrl":"http://www.aol.com/channel/image/food","linkUrl":"http://www.aol.com/channel/food.jpg"}"""
//
//      Json.fromJson[AolChannelRibbon](Json.parse(jsonString)) match {
//        case JsSuccess(channelRibbon, _) => channelRibbon.channel should equal(expected.channel)
//        case JsError(whoops) => fail(whoops.mkString(" AND "))
//      }
//    }
//  }
}
