package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.SchemaTypes._
import org.junit.Test
import org.junit.Assert._
import org.joda.time.DateTime
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestEnvironment

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class IntelligenceSchemaTest extends HBaseTestEnvironment {
 @Test def testClickType() {
   val userActions = Set[String]("beacon", "viewed", "addtocart", "purchased", "shared")
   userActions.foreach(action => assertTrue(ClickType.isValidAction(action)))
   userActions.foreach(action => assertTrue(ClickType.isUserAction(action)))
   userActions.foreach(action => assertFalse(ClickType.isSiteAction(action)))
  val siteActions = Set[String]("publish")
   siteActions.foreach(action => assertTrue(ClickType.isValidAction(action)))
   siteActions.foreach(action => assertFalse(ClickType.isUserAction(action)))
   siteActions.foreach(action => assertTrue(ClickType.isSiteAction(action)))
   assertFalse(ClickType.isValidAction("bad"))
   assertFalse(ClickType.isUserAction("bad"))
   assertFalse(ClickType.isSiteAction("bad"))
 }


  @Test def testGetIdOfUser() {
    val userGuid = "37b5ea1fffad73496715ef3a266b95fa"
    val siteGuid = "18f3b45caef80572d421f68a979bd6dc"

    val userSiteKey = UserSiteKey(userGuid,siteGuid)
    println(UserSiteKeyConverter.toByteString(userSiteKey))
  }

  @Test def testIdFromHbase() {
    val id = "\\xB2~\\xEF\\x12\\xE7\\xA3m)\\xB6\\xFB\\xBA=\\xE2'|E"

    println(UserSiteKeyConverter.fromByteString(id))
  }

  @Test def printSitesSchema() {
    println(Schema.Sites.createScript())
  }


  @Test def testStandardMetricsAddition() {
    var metrics = StandardMetrics.empty

    metrics = metrics + StandardMetrics.OneView
    assertEquals("Metrics should have one view now!", StandardMetrics(1,0,0,0,0), metrics)

    metrics = metrics + StandardMetrics.OnePublish
    assertEquals("Metrics should have one view & one publish now!", StandardMetrics(1,0,0,0,1), metrics)

    metrics = metrics + StandardMetrics.OneSocial
    assertEquals("Metrics should have one view, one publish & one social now!", StandardMetrics(1,1,0,0,1), metrics)

    metrics = metrics + StandardMetrics.OneSearch
    assertEquals("Metrics should have one view, one publish, one social & one search now!", StandardMetrics(1,1,1,0,1), metrics)

    metrics = metrics + StandardMetrics.OneKeyPage
    assertEquals("Metrics should have one view, one publish, one social, one search & one key page now!", StandardMetrics(1,1,1,1,1), metrics)

    metrics = metrics + StandardMetrics.OneView + StandardMetrics.OneView
    assertEquals("Metrics should have three views, one publish, one social, one search & one key page now!", StandardMetrics(3,1,1,1,1), metrics)

    metrics = metrics + metrics
    assertEquals("Metrics should have six views, two publishes, two socials, two searches & two key pages now!", StandardMetrics(6,2,2,2,2), metrics)
  }
}