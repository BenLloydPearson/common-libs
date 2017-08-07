package com.gravity.domain.user

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.{AlgoSettingsData, ArticleRecoData, ClickEvent}
import com.gravity.utilities.time.DateHour
import com.gravity.utilities.{BaseScalaTest, grvtime}
import org.joda.time.DateTime

/**
 * Created by agrealish14 on 4/21/16.
 */
class UserTest extends BaseScalaTest {

  test("Test User Existing Clickstream") {

    val r = 1 to 10000

    val clickStream = r.map(i => {

      ClickStreamKey(DateHour(new DateTime()).minusHours(i), ClickType.clicked, ArticleKey("ak"+i)) -> 1L
    }).toMap

    val siteGuids = Seq[String]("sg1", "sg2")

    val siteToClickStream = siteGuids.map(sg => {

      sg -> clickStream
    }).toMap

    val user = User(userGuid = "ug1", siteToClickStream = siteToClickStream)

    assert(user.userGuid == "ug1")

    assert(user.siteToClickStreamMap.size == 2)

    val cs = user.siteToClickStreamMap.get("sg1")

    assert(cs.isDefined)
    assert(cs.get.clickStreamMap.size == cs.get.MAX_ITEMS)
  }

  test("Test User Add Click Events to Clickstream") {

    val r = 1 to 10000

    val clickStream = r.map(i => {

      ClickStreamKey(DateHour(new DateTime()).minusHours(i), ClickType.clicked, ArticleKey("ak"+i)) -> 1L
    }).toMap

    val siteGuids = Seq[String]("sg1", "sg2")

    val siteToClickStream = siteGuids.map(sg => {

      sg -> clickStream
    }).toMap

    val user = User(userGuid = "ug1", siteToClickStream = siteToClickStream)

    assert(user.userGuid == "ug1")

    assert(user.siteToClickStreamMap.size == 2)

    val cs1 = user.siteToClickStreamMap.get("sg1")

    assert(cs1.isDefined)
    assert(cs1.get.clickStreamMap.size == cs1.get.MAX_ITEMS)

    val articleRecoData: ArticleRecoData = ArticleRecoData.empty.copy(key = ArticleKey("articleKey"))

    val click: ClickEvent = ClickEvent.empty.copy(date = System.currentTimeMillis(), pubSiteGuid = "sg1", article = articleRecoData,
      isMaintenance = true)

    r.foreach(i => {user.add(click)})

    assert(user.siteToClickStreamMap.size == 2)

    val cs2 = user.siteToClickStreamMap.get("sg1")

    assert(cs2.isDefined)
    assert(cs2.get.clickStreamMap.size == cs2.get.MAX_ITEMS)
  }

  test("Test SiteUserClickStream Existing Clickstream") {

    val r = 1 to 10000

    val clickStream = r.map(i => {

      ClickStreamKey(DateHour(new DateTime()).minusHours(i), ClickType.clicked, ArticleKey("ak"+i)) -> 1L
    }).toMap

    val cs = SiteUserClickStream(userGuid = "ug1", siteGuid = "sg1", clickStream = clickStream)

    assert(cs.clickStreamMap.size == cs.MAX_ITEMS)
  }

  test("Test SiteUserClickStream Add Click Events to Clickstream") {

    val r = 1 to 10000

    val clickStream = r.map(i => {

      ClickStreamKey(DateHour(new DateTime()).minusHours(i), ClickType.clicked, ArticleKey("ak"+i)) -> 1L
    }).toMap

    val cs = SiteUserClickStream(userGuid = "ug1", siteGuid = "sg1", clickStream = clickStream)

    assert(cs.clickStreamMap.size == cs.MAX_ITEMS)

    val articleRecoData: ArticleRecoData = ArticleRecoData.empty.copy(key = ArticleKey("articleKey"))

    val click: ClickEvent = ClickEvent.empty.copy(date = System.currentTimeMillis(), article = articleRecoData, isMaintenance = true)

    r.foreach(i => {cs.add(click)})

    assert(cs.clickStreamMap.size == cs.MAX_ITEMS)
  }

}
