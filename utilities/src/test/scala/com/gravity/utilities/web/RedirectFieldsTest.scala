package com.gravity.utilities.web

import com.gravity.utilities.eventlogging.FieldValueRegistry
import com.gravity.utilities.grvfields
import org.joda.time.DateTime
import org.junit.Assert._
import org.junit.Test
import com.gravity.utilities.FieldConverters._
import scalaz.{Failure, Success}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 8/3/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class RedirectFieldsTest {

  @Test def testFieldReadWrite2() {
    val testFields = GravRedirectFields2(List(GravRedirect(new DateTime(), "site1", "adv1", "click1", "", "campaign"),GravRedirect(new DateTime(), "site2", "adv1", "click2", "auction2", "campaign2"),GravRedirect(new DateTime(), "site2", "adv1", "click2", "auction2", "campaign2")))
    val fieldString = grvfields.toDelimitedFieldString(testFields)
    println(fieldString)
    FieldValueRegistry.getInstanceFromString[GravRedirectFields2](fieldString) match {
      case Success(a: GravRedirectFields2) =>
        assertTrue(a.isInstanceOf[GravRedirectFields2])
        assertEquals(fieldString, grvfields.toDelimitedFieldString(a))
        println(a)
      case Failure(fails) => assertTrue(fails.toString(), false)
    }
  }

  @Test def testFieldExpiration() {
    val testField1 = GravRedirect(new DateTime().minusDays(31), "site1", "adv2", "click1", "auction1", "campaign1")
    val testField2 = GravRedirect(new DateTime(), "site2", "adv2", "click2", "auction2", "campaign2")
    val testField3 = GravRedirect(new DateTime(), "site3", "adv3", "click3", "auction3", "campaign3")

    val testFields1 = GravRedirectFields2(List(testField1, testField2))
    val testFields2 = GravRedirectFields2(testField3, testFields1)
    assert(testFields2.redirects.size == 2)
    assert(!testFields2.redirects.contains(testField1))
    assert(testFields2.redirects.contains(testField2))
    assert(testFields2.redirects.contains(testField3))
  }

  @Test def testSiteFilter() {
    val testField1 = GravRedirect(new DateTime(), "site2", "adv1", "click2", "auction2", "campaign2")
    val testField2 = GravRedirect(new DateTime(), "site2", "adv1", "click3", "auction3", "campaign3")
    val testFields1 = GravRedirectFields2(testField1)
    val testFields2 = GravRedirectFields2(testField2, testFields1)
    assert(testFields2.redirects.size == 1)
    assert(testFields2.redirects.contains(testField2))
  }

  @Test def testFieldMaxSize() {
    val fieldsSeq = for(i <- 0 until 35) yield GravRedirect(new DateTime(), "site" + i, "adv" + i, "click" + i, "auction" + i, "campaign " + i)
    val fields = GravRedirectFields2(fieldsSeq.toList)
    assert(fields.redirects.size == 35)
    val testFields = GravRedirectFields2(GravRedirect(new DateTime(), "siten", "advn", "clickn", "auctionn", "campaign"), fields)
    println(testFields)
    assert(testFields.redirects.size == GravRedirectFields2.maxSize)

  }

  @Test def testGetFieldsForBeacon() {
    val fields = GravRedirectFields2(List(GravRedirect(new DateTime(), "site1", "adv1", "click1", "", "campaign1"),GravRedirect(new DateTime(), "site2", "adv2", "click2", "auction2", "campaign2"),GravRedirect(new DateTime(), "site2", "adv2", "click2", "auction2", "campaign2")))
    val siteGuid = "site2"

    val results = {
      val thisSiteRedirects = fields.redirects.filter(redirect => redirect.fromSiteGuid == siteGuid)
      if(thisSiteRedirects.nonEmpty) {
        val thisRedirect = thisSiteRedirects.takeRight(1).head
        (1, thisRedirect.auctionId)
      }
      else {
        (0, "")
      }
    }
    assert(results == (1, "auction2"))
  }

  @Test def testLowerCase() {
    val fields = GravRedirectFields2(List(GravRedirect(new DateTime(), "site1", "adv1", "click1", "", "campaign1"),GravRedirect(new DateTime(), "site2", "adv2", "click2", "auction2", "campaign2"),GravRedirect(new DateTime(), "site2", "adv2", "click2", "auction2", "campaign2")))
    val fieldString = grvfields.toDelimitedFieldString(fields).toLowerCase
    FieldValueRegistry.getInstanceFromString[GravRedirectFields2](fieldString) match {
      case Success(a: GravRedirectFields2) =>
        assertTrue(a.isInstanceOf[GravRedirectFields2])
        assertEquals(fieldString, grvfields.toDelimitedFieldString(a).toLowerCase)
        println(a)
      case Failure(fails) => assertTrue(fails.toString(), false)
    }
  }
}
