package com.gravity.utilities.web

import com.gravity.utilities.{grvtime, BaseScalaTest}

/**
  * Created by tdecamp on 3/2/16.
  * {{insert neat ascii diagram here}}
  */
class GrvTimeNewTest extends BaseScalaTest {
  test("Julian day parsing succeeds for specific day of year") {
    val julianDaysAndExpectedResult = List(
       ("2015_098", 1428476400000l) // Apr 8
      ,("2012_265", 1348210800000l) // Sep 21
      ,("2013_098", 1365404400000l) // Apr 8
      ,("2016_001", 1451635200000l) // Jan 1
      ,("2016_170", 1466233200000l) // Jun 18
      ,("2016_366", 1483171200000l) // Dec 31
    )

    julianDaysAndExpectedResult.foreach { case (jdayString, jdayMillis) =>
      grvtime.fromJulian(jdayString) match {
        case Some(dateMidnight) =>
          withClue(s"Julian string '$jdayString' should result in ${jdayMillis}ms. Actual result: ") {
            dateMidnight.getMillis should be (jdayMillis)
          }
        case None =>
          fail(s"Unable to parse '$jdayString' as GrvDateMidnight")
      }
    }
  }

  test("Julian day parsing returns None on invalid string") {
    val invalidStrings = List(
      "20160_101"
      ,"2016_0101"
      ,"Something"
      ,""
    )

    invalidStrings.foreach { is =>
      withClue(s"Invalid Julian Date String '$is' should result in an empty result: ") {
        grvtime.fromJulian(is) should be ('empty)
      }
    }
  }
}
