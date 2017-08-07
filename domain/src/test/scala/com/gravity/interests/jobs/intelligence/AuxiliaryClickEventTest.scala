package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.utilities.eventlogging.FieldValueRegistry
import com.gravity.utilities.grvfields._
import com.gravity.utilities.{BaseScalaTest, grvfields}
import org.joda.time.DateTime
import org.junit.Assert._

import scalaz.{Failure, Success}


class AuxiliaryClickEventTest extends BaseScalaTest {
  val testEvent: AuxiliaryClickEvent = AuxiliaryClickEvent(
    new DateTime(),
    "testImpressionHash",
    "testClickHash",
    "testUserGuid",
    "http://testpageurl.com/",
    "http://testreferrer.com/",
    "testUserAgent",
    "123.34.56.78"
  )

  import com.gravity.domain.FieldConverters._

  test("field read write") {
    val fieldString = grvfields.toDelimitedFieldString(testEvent)
    FieldValueRegistry.getInstanceFromString[AuxiliaryClickEvent](fieldString) match {
      case Success(a: AuxiliaryClickEvent) =>
        assertTrue(a.isInstanceOf[AuxiliaryClickEvent])
        assertEquals(fieldString, a.toDelimitedFieldString)
      case Failure(fails) => assertTrue(fails.toString(), false)
    }
  }
}
