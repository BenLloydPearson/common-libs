package com.gravity.interests.jobs.intelligence.operations

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.gravity.data.configuration.DlPlacementSetting
import com.gravity.domain.GrvDuration
import com.gravity.domain.aol._
import com.gravity.domain.gms.{GmsArticleStatus, GmsRoute, UniArticleId}
import com.gravity.domain.recommendations.ContentGroup
import com.gravity.domain.rtb.gms.ContentGroupIdToPinnedSlot
import com.gravity.interests.jobs.intelligence._
import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvcoll._
import org.joda.time.DateTime

import scalaz.Scalaz._
import scalaz._

case object GmsServiceTest

class GmsServiceTest extends BaseScalaTest with operationsTesting {
  import com.gravity.logging.Logging._

  val uniquer = new AtomicLong(0L)

  val secondaryLinks = List(AolLink(makeExampleUrl("link1"), "link1"), AolLink(makeExampleUrl("link2"), "link2"))

  com.gravity.grvlogging.updateLoggerToTrace("com.gravity.interests.jobs.intelligence.operations.GmsServiceTest")

  def failForFails(message: String, fails: NonEmptyList[FailureResult]): Unit = {
    fail(message + " " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND "))
  }

  def shouldBeSame(have: List[ContentGroupIdToPinnedSlot], want: List[ContentGroupIdToPinnedSlot]) = {
    have.sortBy(c2p => c2p.contentGroupId -> c2p.slot) should be(want.sortBy(c2p => c2p.contentGroupId -> c2p.slot))
  }

  def buildAolGmsModifyFields(siteKey: SiteKey, contentGroupIds: Set[Long], numStart: Int): AolGmsModifyFields = {
    val num     = new AtomicInteger(numStart)
    def nextInt = num.incrementAndGet

    def nextStr(id: String) = s"$id-$nextInt"
    def nextUrl(id: String) = makeExampleUrl(nextStr(id))

    def nextBool: Boolean = (nextInt & 1) != 0
    def nextDateTime = new DateTime(0).plusHours(nextInt).withMillis(0L)
    def nextGrvDuration = GrvDuration(nextInt, nextInt, nextInt)
    def nextGmsArticleStatus = GmsArticleStatus.values.drop(if (nextBool) 1 else 0).head

    def nextSecondaryLinks = List(
      AolLink(makeExampleUrl(s"link1-$nextInt"), s"link1-$nextInt"),
      AolLink(makeExampleUrl(s"link2-$nextInt"), s"link2-$nextInt")
    )

    def nextImageWithCredit = ImageWithCredit(nextStr("image"), nextStr("credit").some)

    AolGmsModifyFields(
      siteKey,
      nextStr("title"),
      nextStr("category"),
      nextUrl("category"),
      nextInt,
      nextStr("categoryString"),
      nextStr("source"),
      nextUrl("source"),
      nextStr("summary"),
      nextStr("headline"),
      nextStr("secondaryHeader"),
      nextSecondaryLinks,
      nextUrl("image"),
      nextBool,
      nextDateTime.some,
      nextDateTime.some,
      nextGrvDuration.some,
      nextGmsArticleStatus.some,
      nextUrl("aolRibbonImageUrl").some,
      nextStr("aolImageSource").some,
      nextImageWithCredit.some,
      nextUrl("narrowBandImage").some,
      nextStr("channelPageTitle").some,
      nextUrl("highResImage").some,
      contentGroupIds = contentGroupIds,
      fieldsToClear = Set.empty
    )
  }

  def articleShouldMatchModFields(gmsArticle: AolGmsArticle, modFields: AolGmsModifyFields) = {
    gmsArticle.title should be(modFields.title)
    gmsArticle.aolCategory should be(modFields.category)
    gmsArticle.aolCategoryLink should be(modFields.categoryLink)
    gmsArticle.aolCategoryId should be(modFields.categoryId.some)
    gmsArticle.aolCategorySlug should be(modFields.categorySlug.some)
    gmsArticle.sourceText should be(modFields.source)
    gmsArticle.sourceUrl should be(modFields.sourceLink)
    gmsArticle.summary should be(modFields.summary)
    gmsArticle.headline should be(modFields.headline)
    gmsArticle.secondaryHeader should be(modFields.secondaryHeader)
    gmsArticle.secondaryLinks should be(modFields.secondaryLinks)
    gmsArticle.imageUrl should be(modFields.image)
    gmsArticle.showVideoIcon should be(modFields.showVideoIcon)
    gmsArticle.startDate should be(modFields.startDate)
    gmsArticle.endDate should be(modFields.endDate)
    gmsArticle.duration should be(modFields.duration)
    gmsArticle.status.some should be(modFields.statusOption)

    gmsArticle.aolRibbonImageUrl should be(modFields.aolRibbonImageUrl)
    gmsArticle.aolImageSource should be(modFields.aolImageSource)
    gmsArticle.channelImage.map(img => ImageWithCredit(img, gmsArticle.channelImageSource)) should be(modFields.channelImage)
    gmsArticle.narrowBandImage should be(modFields.narrowBandImage)
    gmsArticle.channelPageTitle should be(modFields.channelPageTitle)
    gmsArticle.highResImage should be(modFields.highResImage)
    gmsArticle.contentGroupIds should be(modFields.contentGroupIds)
    modFields.fieldsToClear should be(Set.empty)

    /*
                contentGroupIds: Set[Long] = Set.empty[Long],
                fieldsToClear: Set[String] = Set.empty[String]
     */
  }

  test("all fields faithfully saved/updated") {
    val numStart = new AtomicInteger(0)

    withCleanup {
      withSitesCampaignsAndContentGroups(numSites = 2, numCampaigns = 2, isGmsManaged = true) { allInfo =>
        for {
          (siteRow, campaignRow, contentGroup) <- allInfo
          siteKey = siteRow.siteKey
        } {
          val aolGmsModFields1 = buildAolGmsModifyFields(siteKey, Set(contentGroup.id), numStart.incrementAndGet())
          val articleUrl       = makeExampleUrl(aolGmsModFields1.title)

          saveTestGmsArticle(articleUrl, aolGmsModFields1, userId = 0L, isNewlySubmitted = true) match {
            case Success(savedGmsArticle1) =>
              GmsService.getGmsArticleFromHbaseWithoutMetrics(GmsRoute(siteKey), ArticleKey(articleUrl)) match {
                case Success(readSavedGmsArticle1) =>
                  articleShouldMatchModFields(readSavedGmsArticle1, aolGmsModFields1)

                  val aolGmsModFields2 = buildAolGmsModifyFields(siteKey, Set(contentGroup.id), numStart.incrementAndGet())

                  saveTestGmsArticle(articleUrl, aolGmsModFields2, userId = 0L, isNewlySubmitted = false) match {
                    case Success(savedGmsArticle2) =>
                      GmsService.getGmsArticleFromHbaseWithoutMetrics(GmsRoute(siteKey), ArticleKey(articleUrl)) match {
                        case Success(readUpdatedGmsArticle) =>
                          articleShouldMatchModFields(readUpdatedGmsArticle, aolGmsModFields2)

                        case Failure(fails) => failForFails("Failed to retreive updated unit: ", fails)
                      }

                    case Failure(fails) => failForFails("Failed to update unit: ", fails)
                  }

                case Failure(fails) => failForFails("Failed to retreive created unit: ", fails)
              }

            case Failure(fails) => failForFails("Failed to create unit: ", fails)
          }
        }
      }
    }
  }

  test("expiring articles") {
    withCleanup {
      withSitesCampaignsAndContentGroups(numSites = 2, numCampaigns = 2, isGmsManaged = true) { allInfo =>
        for {
          (siteRow, campaignRow, contentGroup) <- allInfo
          siteKey = siteRow.siteKey
        } {
          val startDate = new DateTime().minusHours(3).withMillis(0L).some
          val duration = GrvDuration(0, 0, 2).some

          saveTestGmsArticle(siteKey, s"Expiring Article #${uniquer.incrementAndGet}", Set(contentGroup.id), duration = duration) match {
            case Success(dlArticle) =>
              dlArticle.duration should equal(duration)
              dlArticle.startDate should be('empty)
              dlArticle.endDate should be('empty)

              val uniArticleId = UniArticleId.forGms(siteKey, dlArticle.articleKey)
              val gmsRoute = GmsRoute(siteKey)

              GmsService.updateDlArticleStatus(uniArticleId, GmsArticleStatus.Live, 307L, false).valueOr {
                case fails: NonEmptyList[FailureResult] => failForFails("Failed to update status to Live", fails)
              }

              GmsService.getGmsArticleFromHbaseWithoutMetrics(gmsRoute, dlArticle.articleKey) match {
                case Success(updatedDlArticle) =>
                  trace("Updated article: {0}", updatedDlArticle)
                  updatedDlArticle.articleStatus should equal(GmsArticleStatus.Live)
                  updatedDlArticle.startDate should be('defined)

                case Failure(fails) => failForFails("Failed to retrieve DL Article to check its update", fails)
              }

              GmsService.checkIfRecommendableAndUpdateIfNeeded(uniArticleId, dlArticle.url, GmsArticleStatus.Live, startDate, None, duration, executeUpdate = true) match {
                case Success(isStillRecommendable) =>
                  isStillRecommendable should be(false)

                case Failure(fails) => failForFails("Failed to checkIfRecommendableAndUpdateIfNeeded", fails)
              }

            case Failure(fails) => failForFails("Failed to create unit: ", fails)
          }
        }
      }
    }
  }

  test("creating a new article should succeed if ArticleRow or GMS data for site does not exist") {
    withCleanup {
      withSitesCampaignsAndContentGroups(numSites = 2, numCampaigns = 2, isGmsManaged = true) { allInfo =>
        val campaignsAndContentGroupsBySite: Map[SiteKey, Seq[(SiteRow, CampaignRow, ContentGroup)]] = allInfo.groupBy(_._1.siteKey)

        for {
          (siteKey, siteInfo) <- campaignsAndContentGroupsBySite
        } {
          val cgIdUseMe +: cgIdOthers = siteInfo.map(_._3.id)

          // For the first siteGuid, the ArticleRow won't exist yet.
          // For the second siteGuid, the ArticleRow will exist, but won't yet have any GMS data for that site.
          saveTestGmsArticle(siteKey, s"Non-Unique Title", Set(cgIdUseMe), secondaryLinks).isSuccess should be(true)
        }
      }
    }
  }

  test("creating a new article should fail if GMS article already exists for site") {
    withCleanup {
      withSitesCampaignsAndContentGroups(numSites = 2, numCampaigns = 2, isGmsManaged = true) { allInfo =>
        val campaignsAndContentGroupsBySite: Map[SiteKey, Seq[(SiteRow, CampaignRow, ContentGroup)]] = allInfo.groupBy(_._1.siteKey)

        for {
          (siteKey, siteInfo) <- campaignsAndContentGroupsBySite
        } {
          val cgIdUseMe +: cgIdOthers = siteInfo.map(_._3.id)

          // Creating a previously non-existent article should succeed.
          saveTestGmsArticle(siteKey, s"Non-Unique Title", Set(cgIdUseMe), secondaryLinks).isSuccess should be(true)

          // Creating an already-existing article should fail.
          saveTestGmsArticle(siteKey, s"Non-Unique Title", Set(cgIdUseMe), secondaryLinks).isSuccess should be(false)
        }
      }
    }
  }

  test("modifying a non-existent article should fail, but should succeed if article already exists") {
    withCleanup {
      withSitesCampaignsAndContentGroups(numSites = 2, numCampaigns = 2, isGmsManaged = true) { allInfo =>
        val campaignsAndContentGroupsBySite: Map[SiteKey, Seq[(SiteRow, CampaignRow, ContentGroup)]] = allInfo.groupBy(_._1.siteKey)

        for {
          (siteKey, siteInfo) <- campaignsAndContentGroupsBySite
        } {
          val cgIdUseMe +: cgIdOthers = siteInfo.map(_._3.id)

          // Modifying a non-existent article should fail.
          saveTestGmsArticle(siteKey, s"Non-Unique Title", Set(cgIdUseMe), secondaryLinks, isNewlySubmitted = false).isSuccess should be(false)

          // Creating a previously non-existent article should succeed.
          saveTestGmsArticle(siteKey, s"Non-Unique Title", Set(cgIdUseMe), secondaryLinks, isNewlySubmitted = true ).isSuccess should be(true)

          // Modifying an already-existing article should succeed.
          saveTestGmsArticle(siteKey, s"Non-Unique Title", Set(cgIdUseMe), secondaryLinks, isNewlySubmitted = false).isSuccess should be(true)
        }
      }
    }
  }

  test("test contentGroups") {
    withCleanup {
      withSitesCampaignsAndContentGroups(numSites = 2, numCampaigns = 4, isGmsManaged = true) { allInfo =>
        val campaignsAndContentGroupsBySite: Map[SiteKey, Seq[(SiteRow, CampaignRow, ContentGroup)]] = allInfo.groupBy(_._1.siteKey)

        for {
          (siteKey, siteInfo) <- campaignsAndContentGroupsBySite
        } {
          val cgIdIdle +: cgIdToKill +: cgIdMembers = siteInfo.map(_._3.id)
          val initialContentGroupIds = (cgIdToKill +: cgIdMembers).toSet

          val campaignKeysByCgId = siteInfo.map(tup => tup._3.id -> tup._2.campaignKey).toMap

          def checkIfArticleIsInContentGroupCampaign(ak: ArticleKey, contentGroupId: Long, expected: Boolean): Unit = {
            withClue(s"Expected article $ak's membership in contentGroup $contentGroupId's, campaign to be $expected") {
              val campaignKey = campaignKeysByCgId(contentGroupId)

              Schema.Campaigns.query2.withKey(campaignKey).withFamilies(_.recentArticles).singleOption().exists(_.recentArticles.values.toSet.contains(ak)) should be(expected)
            }
          }

          saveTestGmsArticle(siteKey, s"Test ContentGroups #${uniquer.incrementAndGet}", initialContentGroupIds, secondaryLinks) match {
            case Success(article) =>
              article.contentGroupIds should equal(initialContentGroupIds)

              initialContentGroupIds.foreach { cgId =>
                checkIfArticleIsInContentGroupCampaign(article.articleKey, cgId, true)
              }

              Seq(cgIdIdle).foreach { cgId =>
                checkIfArticleIsInContentGroupCampaign(article.articleKey, cgId, false)
              }

              // now let's remove just the cgIdToKill contentGroup
              val contentGroupsSansKilled = initialContentGroupIds - cgIdToKill

              val fields = AolGmsModifyFields(siteKey, article.title, article.categoryText, article.categoryUrl,
                article.aolCategoryId.getOrElse(0), article.aolCategorySlug.getOrElse(""), article.sourceText, article.sourceUrl,
                article.summary, article.headline, article.secondaryHeader, article.secondaryLinks, article.imageUrl,
                article.showVideoIcon, article.startDate, article.endDate, None, None,
                article.aolRibbonImageUrl, article.aolImageSource,
                article.channelImage.map(i => ImageWithCredit(i, article.channelImageSource)),
                article.narrowBandImage, contentGroupIds = contentGroupsSansKilled)

              GmsService.saveGmsArticle(article.url, fields, 0L).value match {
                case Success(modifiedArticle) =>
                  modifiedArticle.contentGroupIds should equal(contentGroupsSansKilled)

                  contentGroupsSansKilled.foreach { cgId =>
                    checkIfArticleIsInContentGroupCampaign(article.articleKey, cgId, true)
                  }

                  Seq(cgIdIdle, cgIdToKill).foreach { cgId =>
                    checkIfArticleIsInContentGroupCampaign(article.articleKey, cgId, false)
                  }

                case Failure(fails) =>
                  fail("Failed to update unit to remove one contentGroup of 3: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND "))
              }

              // now let's test removing all contentGroups
              val fieldsForNoContentGroups = AolGmsModifyFields(article.siteKey, article.title, article.categoryText, article.categoryUrl,
                article.aolCategoryId.getOrElse(0), article.aolCategorySlug.getOrElse(""), article.sourceText, article.sourceUrl,
                article.summary, article.headline, article.secondaryHeader, article.secondaryLinks, article.imageUrl,
                article.showVideoIcon, article.startDate, article.endDate, None, None,
                article.aolRibbonImageUrl, article.aolImageSource,
                article.channelImage.map(i => ImageWithCredit(i, article.channelImageSource)),
                article.narrowBandImage, contentGroupIds = Set.empty[Long], fieldsToClear = Set(AolGmsFieldNames.ContentGroupIds))

              GmsService.saveGmsArticle(article.url, fieldsForNoContentGroups, 0L).value match {
                case Success(articleThatShouldHaveFailed) =>
                  fail("All GMS Articles MUST have at least one contentGroup. This article should have failed: " + articleThatShouldHaveFailed)

                case Failure(fails) =>
                  fails.list should have size (1)
                  fails.head should equal(GmsService.noContentGroupsFailureResult)

              }

            case Failure(fails) =>
              failForFails("Failed to create unit: ", fails)
          }
        }
      }
    }
  }

  def stripAdFromResults(pinMap: Map[Int, ArticleKey], contentGroup: Long): Map[Int, ArticleKey] = DlPlacementSetting.gmsAdUnitSlot(contentGroup.some, None) match {
    case Some(adSlot) => pinMap - adSlot
    case None => pinMap
  }

  test("test updating pins") {
    GmsServiceTest.synchronized {
      withCleanup {
        withSitesCampaignsAndContentGroups(numSites = 2, numCampaigns = 10, isGmsManaged = true) { allInfo =>
          val campaignsAndContentGroupsBySite: Map[SiteKey, Seq[(SiteRow, CampaignRow, ContentGroup)]] = allInfo.groupBy(_._1.siteKey)

          for {
            (siteKey, siteInfo) <- campaignsAndContentGroupsBySite
          } {
            val allCgIds = siteInfo.map(_._3.id)
            val testCgIdHome +: testCgIdNews +: testCgIdEntertainment +: testCgIdFinance +: testCgIdLifestyle +: testCgIdSports +: testOtherCgIds = allCgIds

            // create 10 units per contentGroup (50 total)
            val homeUnits = createTestGmsArticlesInContentGroup(siteKey, s"Home Article updating #${uniquer.incrementAndGet}", 10, Set(testCgIdHome), secondaryLinks = secondaryLinks)
            val newsUnits = createTestGmsArticlesInContentGroup(siteKey, s"News Article updating #${uniquer.incrementAndGet}", 10, Set(testCgIdNews), secondaryLinks = secondaryLinks)
            val entertainmentUnits = createTestGmsArticlesInContentGroup(siteKey, s"Entertainment Article updating #${uniquer.incrementAndGet}", 10, Set(testCgIdEntertainment), secondaryLinks = secondaryLinks)
            val financeUnits = createTestGmsArticlesInContentGroup(siteKey, s"Finance Article updating #${uniquer.incrementAndGet}", 10, Set(testCgIdFinance), secondaryLinks = secondaryLinks)
            val lifestyleUnits = createTestGmsArticlesInContentGroup(siteKey, s"Lifestyle Article updating #${uniquer.incrementAndGet}", 10, Set(testCgIdLifestyle), secondaryLinks = secondaryLinks)
            val sportsUnits = createTestGmsArticlesInContentGroup(siteKey, s"Sports Article updating #${uniquer.incrementAndGet}", 10, Set(testCgIdSports), secondaryLinks = secondaryLinks)

            // create maps of pins for each contentGroup
            val homePins = stripAdFromResults(homeUnits.take(20).zipWithIndex.map(tup => tup._2 -> tup._1.articleKey).toMap, testCgIdHome)
            val newsPins = stripAdFromResults(newsUnits.take(8).zipWithIndex.map(tup => tup._2 -> tup._1.articleKey).toMap, testCgIdNews)
            val entertainmentPins = stripAdFromResults(entertainmentUnits.take(5).zipWithIndex.map(tup => tup._2 -> tup._1.articleKey).toMap, testCgIdEntertainment)
            val financePins = stripAdFromResults(financeUnits.take(1).zipWithIndex.map(tup => tup._2 -> tup._1.articleKey).toMap, testCgIdFinance)
            val lifestylePins = stripAdFromResults(lifestyleUnits.take(2).zipWithIndex.map(tup => tup._2 -> tup._1.articleKey).toMap, testCgIdLifestyle)
            val sportsPins = stripAdFromResults(sportsUnits.take(3).zipWithIndex.map(tup => tup._2 -> tup._1.articleKey).toMap, testCgIdSports)

            // combined expected map
            val expectedPins = {
              val homeMap = convertGmsPinsToPinMap(testCgIdHome, homePins)
              val newsMap = convertGmsPinsToPinMap(testCgIdNews, newsPins)
              val entertainmentMap = convertGmsPinsToPinMap(testCgIdEntertainment, entertainmentPins)
              val financeMap = convertGmsPinsToPinMap(testCgIdFinance, financePins)
              val lifestyleMap = convertGmsPinsToPinMap(testCgIdLifestyle, lifestylePins)
              val sportsMap = convertGmsPinsToPinMap(testCgIdSports, sportsPins)

              val allKeys = homeMap.keySet ++ newsMap.keySet ++ entertainmentMap.keySet ++ financeMap.keySet ++ lifestyleMap.keySet ++ sportsMap.keySet

              (for (ak <- allKeys) yield {
                ak -> (homeMap.getOrElse(ak, Nil) ++ newsMap.getOrElse(ak, Nil) ++ entertainmentMap.getOrElse(ak, Nil) ++ financeMap.getOrElse(ak, Nil) ++ lifestyleMap.getOrElse(ak, Nil) ++ sportsMap.getOrElse(ak, Nil))
              }).toMap
            }

            // set pins for each contentGroup
            GmsService.pinArticles(siteKey, testCgIdHome, homePins).valueOr(fails => fail("Failed to pin NotSet: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))
            GmsService.pinArticles(siteKey, testCgIdNews, newsPins).valueOr(fails => fail("Failed to pin News: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))
            GmsService.pinArticles(siteKey, testCgIdEntertainment, entertainmentPins).valueOr(fails => fail("Failed to pin Entertainment: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))
            GmsService.pinArticles(siteKey, testCgIdFinance, financePins).valueOr(fails => fail("Failed to pin Finance: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))
            GmsService.pinArticles(siteKey, testCgIdLifestyle, lifestylePins).valueOr(fails => fail("Failed to pin Lifestyle: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))
            GmsService.pinArticles(siteKey, testCgIdSports, sportsPins).valueOr(fails => fail("Failed to pin Sports: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))

            PermaCacher.restart()
            GmsPinnedDataCache.clear()

            // pull all to verify what was stored
            GmsService.getPinnedArticleSlotsForContentGroups(allCgIds.toSet).map(_.values.flatten.toList) match {
              case Success(allPins) =>
                val articleKeyToPinsMap = allPins.groupBy(_.articleKey)
                  .mapValues(_.map(row => ContentGroupIdToPinnedSlot(row.contentGroupId, row.slotPosition)))

                // validate MySql Data
                withClue("Expected Pin Mappings Should Equal resulting pin mappings") {
                  articleKeyToPinsMap should equal(expectedPins)
                }

              case Failure(fails) =>
                failForFails("Failed to getPinnedArticleSlotsForContentGroups: ", fails)
            }

            val dlArticleMap = GmsService.getAllGmsArticlesFromHbase().map(_.filter(_._1.siteKey == siteKey).map(tup => tup._2.articleKey -> tup._2).toMap).valueOr {
              fails =>
                fail("Failed retrieve all DL Units!: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND "))
                Map.empty[ArticleKey, AolGmsArticle]
            }

            println("MySql Data Verification complete. Now for GrvMap data from hbase...")

            val allKeys = homeUnits.map(_.articleKey).toSet ++
              newsUnits.map(_.articleKey).toSet ++
              entertainmentUnits.map(_.articleKey).toSet ++
              financeUnits.map(_.articleKey).toSet ++
              lifestyleUnits.map(_.articleKey).toSet ++
              sportsUnits.map(_.articleKey).toSet

            // validate each GrvMap based pin data
            for {
              ak <- allKeys
              expectedContentGroupPins = expectedPins.getOrElse(ak, Nil)
            } {
              dlArticleMap.get(ak) match {
                case Some(dlArticle) =>
                  withClue("Expected Pin Mappings Should Equal resulting GrvMap pin mappings for:" + ak) {
                    shouldBeSame(dlArticle.contentGroupsToPinned, expectedContentGroupPins)
                  }

                case None =>
                  fail("Generated DL Unit Should Be Present In `GmsService.getArticles()` result. Not Found: " + ak)
              }
            }
          }
        }
      }
    }
  }

  test("unpinning article") {
    GmsServiceTest.synchronized {
      withCleanup {
        withSitesCampaignsAndContentGroups(numSites = 2, numCampaigns = 10, isGmsManaged = true) { allInfo =>
          val campaignsAndContentGroupsBySite: Map[SiteKey, Seq[(SiteRow, CampaignRow, ContentGroup)]] = allInfo.groupBy(_._1.siteKey)

          for {
            (siteKey, siteInfo) <- campaignsAndContentGroupsBySite
          } {
            val allCgIds = siteInfo.map(_._3.id)
            val testCgIdHome +: testCgIdNews +: testCgIdEntertainment +: testCgIdFinance +: testCgIdLifestyle +: testCgIdSports +: testOtherCgIds = allCgIds

            // create 10 units per contentGroup (50 total)
            val homeUnits = createTestGmsArticlesInContentGroup(siteKey, s"Home Article unpinning #${uniquer.incrementAndGet}", 10, Set(testCgIdHome), secondaryLinks = secondaryLinks)

            homeUnits should have length (10)

            val allKeysForThisTest = homeUnits.map(_.articleKey).toSet

            val firstUnit = homeUnits.head
            GmsService.pinArticles(siteKey, testCgIdHome, Map(1 -> firstUnit.articleKey)).valueOr(fails => fail("Failed to pin NotSet: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND ")))
            GmsPinnedDataCache.clear()

            // verify MySql for this pin.
            val allPins = GmsService.getPinnedArticleSlotsForContentGroups(allCgIds.toSet).map(_.values.flatten.toList).valueOr {
              fails =>
                failForFails("Failed retrieve all pinned GMS Units!: ", fails)
                Nil
            }

            allPins should have length (1)
            val pinnedHead = allPins.head
            pinnedHead.articleKey should equal (firstUnit.articleKey)
            pinnedHead.contentGroupId should equal (testCgIdHome)
            pinnedHead.slotPosition should equal (1)

            PermaCacher.restart()
            GmsPinnedDataCache.clear()

            val dlArticleMap = GmsService.getAllGmsArticlesFromHbase().map(_.map(tup => tup._2.articleKey -> tup._2).toMap).valueOr {
              fails =>
                failForFails("Failed retrieve all DL Units!: ", fails)
                Map.empty[ArticleKey, AolGmsArticle]
            }

            dlArticleMap.get(firstUnit.articleKey) match {
              case Some(dlArticle) =>
                dlArticle.contentGroupsToPinned should have length (1)
                val c2p = dlArticle.contentGroupsToPinned.head
                c2p.contentGroupId should equal (testCgIdHome)
                c2p.slot should equal (1)
              case None =>
                fail("Pinned article was not found in hbase result!")
            }

            GmsService.pinArticles(siteKey, testCgIdHome, Map.empty[Int, ArticleKey]).valueOr(fails => failForFails("Failed to UNpin NotSet: ", fails))
            GmsPinnedDataCache.clear()

            val postUnpinningAllPins = GmsService.getPinnedArticleSlotsForContentGroups(allCgIds.toSet).map(_.values.flatten.toList).valueOr {
              fails =>
                failForFails("Failed retrieve all pinned GMS Units!: ", fails)
                Nil
            }
            postUnpinningAllPins should have length (0)

            PermaCacher.restart()
            GmsPinnedDataCache.clear()

            val dlArticleMapAfter = GmsService.getAllGmsArticlesFromHbase().map(_.map(tup => tup._2.articleKey -> tup._2).toMap).valueOr {
              fails =>
                fail("Failed RE-retrieve all DL Units!: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND "))
                Map.empty[ArticleKey, AolGmsArticle]
            }

            for ((ak, dlArticle) <- dlArticleMapAfter.filterKeys(allKeysForThisTest.contains)) {
              withClue("Nothing should be pinned in GrvMap since we unpinned. DL Article: " + dlArticle.toString) {
                val homePins = dlArticle.contentGroupsToPinned.filter(_.contentGroupId == testCgIdHome)
                homePins should have length (0)
              }
            }
          }
        }
      }
    }
  }

  test("grvMap contentGroup to pin") {
    /*
       TITLE                             ContentGroupS               PIN INFO
       Non-Pinned Home Article 1         Home                   --
       Non-Pinned Home Article 2         Home                   --
       Pinned Home Article 1             Home, Entertainment    Home -> 3
       Pinned Home Article 2             Home, Entertainment    Home -> 7
       Pinned Entertainment Article 1    Home, Entertainment    Entertainment -> 2
       Pinned Entertainment Article 2    Home, Entertainment    Entertainment -> 7, Home -> 8
     */

    GmsServiceTest.synchronized {
      withCleanup {
        withSitesCampaignsAndContentGroups(numSites = 2, numCampaigns = 10, isGmsManaged = true) { allInfo =>
          val campaignsAndContentGroupsBySite: Map[SiteKey, Seq[(SiteRow, CampaignRow, ContentGroup)]] = allInfo.groupBy(_._1.siteKey)

          for {
            (siteKey, siteInfo) <- campaignsAndContentGroupsBySite
          } {
            val allCgIds = siteInfo.map(_._3.id)
            val testCgIdHome +: testCgIdNews +: testCgIdEntertainment +: testCgIdFinance +: testCgIdLifestyle +: testCgIdSports +: testOtherCgIds = allCgIds

            val nonPinnedHomeBaseTitle = s"Non-Pinned Home Article #${uniquer.incrementAndGet}"
            val nonPinnedHomeArticles  = createTestGmsArticlesInContentGroup(siteKey, nonPinnedHomeBaseTitle, 2, Set(testCgIdHome))

            val pinnedHomeBaseTitle = s"Pinned Home Article #${uniquer.incrementAndGet}"
            val pinnedHomeArticles  = createTestGmsArticlesInContentGroup(siteKey, pinnedHomeBaseTitle, 2, Set(testCgIdHome, testCgIdEntertainment))

            val pinnedEntertainmentBaseTitle = s"Pinned Entertainment Article #${uniquer.incrementAndGet}"
            val pinnedEntertainmentArticles  = createTestGmsArticlesInContentGroup(siteKey, pinnedEntertainmentBaseTitle, 2, Set(testCgIdHome, testCgIdEntertainment))

            GmsService.pinArticles(siteKey,
              testCgIdHome,
              Seq(3, 7, 8).zip(pinnedHomeArticles :+ pinnedEntertainmentArticles.last).toMap.mapValues(_.articleKey)
            ) should be('success)

            GmsService.pinArticles(siteKey,
              testCgIdEntertainment,
              Seq(2, 7).zip(pinnedEntertainmentArticles).toMap.mapValues(_.articleKey)
            ) should be('success)

            val articles = nonPinnedHomeArticles ++ pinnedHomeArticles ++ pinnedEntertainmentArticles
            val aks = articles.map(_.articleKey).toSet

            def validateGmsArticlesFromHbase(afterStateChange: Boolean) {
              PermaCacher.restart()
              GmsPinnedDataCache.clear()

              val articlesFromHbaseV = GmsService.getSelectedGmsArticlesFromHbase(aks.toSeq, GmsRoute(siteKey))
              articlesFromHbaseV should be('success)
              articlesFromHbaseV.foreach {
                case articlesFromHbase =>
                  articlesFromHbase should have length articles.length
                  val articlesByKey = articlesFromHbase.mapBy(_.articleKey)

                  val nonPinnedHomeArticle1 = articlesByKey(nonPinnedHomeArticles.head.articleKey)
                  val nonPinnedHomeArticle2 = articlesByKey(nonPinnedHomeArticles.last.articleKey)
                  nonPinnedHomeArticle1.title should be(s"$nonPinnedHomeBaseTitle 1")
                  nonPinnedHomeArticle2.title should be(s"$nonPinnedHomeBaseTitle 2")
                  nonPinnedHomeArticle1.contentGroupIds should be(Set(testCgIdHome))
                  nonPinnedHomeArticle2.contentGroupIds should be(Set(testCgIdHome))
                  nonPinnedHomeArticle1.contentGroupsToPinned should be('empty)
                  nonPinnedHomeArticle2.contentGroupsToPinned should be('empty)

                  val pinnedHomeArticle1 = articlesByKey(pinnedHomeArticles.head.articleKey)
                  val pinnedHomeArticle2 = articlesByKey(pinnedHomeArticles.last.articleKey)
                  pinnedHomeArticle1.title should be(s"$pinnedHomeBaseTitle 1")
                  pinnedHomeArticle2.title should be(s"$pinnedHomeBaseTitle 2")
                  pinnedHomeArticle1.contentGroupIds should be(Set(testCgIdHome, testCgIdEntertainment))
                  pinnedHomeArticle2.contentGroupIds should be(Set(testCgIdHome, testCgIdEntertainment))
                  pinnedHomeArticle1.contentGroupsToPinned should be(List(ContentGroupIdToPinnedSlot(testCgIdHome, 3)))
                  if(afterStateChange)
                    pinnedHomeArticle2.contentGroupsToPinned should be('empty)
                  else
                    pinnedHomeArticle2.contentGroupsToPinned should be(List(ContentGroupIdToPinnedSlot(testCgIdHome, 7)))

                  val pinnedEntertainmentArticle1 = articlesByKey(pinnedEntertainmentArticles.head.articleKey)
                  val pinnedEntertainmentArticle2 = articlesByKey(pinnedEntertainmentArticles.last.articleKey)
                  pinnedEntertainmentArticle1.title should be(s"$pinnedEntertainmentBaseTitle 1")
                  pinnedEntertainmentArticle2.title should be(s"$pinnedEntertainmentBaseTitle 2")
                  pinnedEntertainmentArticle1.contentGroupIds should be(Set(testCgIdHome, testCgIdEntertainment))
                  pinnedEntertainmentArticle2.contentGroupIds should be(Set(testCgIdHome, testCgIdEntertainment))
                  pinnedEntertainmentArticle1.contentGroupsToPinned should be(List(ContentGroupIdToPinnedSlot(testCgIdEntertainment, 2)))

                  shouldBeSame(
                    pinnedEntertainmentArticle2.contentGroupsToPinned,
                    List(ContentGroupIdToPinnedSlot(testCgIdEntertainment, 7), ContentGroupIdToPinnedSlot(testCgIdHome, 8))
                  )
              }
            }
            validateGmsArticlesFromHbase(afterStateChange = false)

            // Unpin one of the home articles and ensure structural integrity all around
            GmsService.pinArticles(siteKey,
              testCgIdHome,
              Seq(3, 8).zip(pinnedHomeArticles.head +: pinnedEntertainmentArticles.tail).toMap.mapValues(_.articleKey)
            ) should be('success)
            validateGmsArticlesFromHbase(afterStateChange = true)
          }
        }
      }
    }
  }

//  test("min articles") {
//    val contentGroupToTest = testCgIdHome
//
//    val minArticles = DlPlacementSetting.minArticles(contentGroupToTest)
//
//    minArticles should equal (4)
//  }
//
//  test("read AolContentGroupRibbon from legacy JSON") {
//    val expected = AolContentGroupRibbon.buildContentGroupRibbon(testCgIdFood)._2
//
//    val jsonString = """{"imageUrl":"http://www.aol.com/contentGroup/image/food","linkUrl":"http://www.aol.com/contentGroup/food.jpg"}"""
//
//    Json.fromJson[AolContentGroupRibbon](Json.parse(jsonString)) match {
//      case JsSuccess(contentGroupRibbon, _) => contentGroupRibbon.contentGroup should equal(expected.contentGroup)
//      case JsError(whoops) => fail(whoops.mkString(" AND "))
//    }
//  }
}
