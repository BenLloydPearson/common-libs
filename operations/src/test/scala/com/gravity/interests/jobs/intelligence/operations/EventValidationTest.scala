package com.gravity.interests.jobs.intelligence.operations

import org.junit.Assert._
import com.gravity.utilities.{BaseScalaTest, grvio}
import scalaz.{Failure, Success}

/**
 * Created with IntelliJ IDEA.
 * User: cstelzmuller
 * Date: 9/18/13
 * Time: 12:28 PM
 */
class EventValidationTest extends BaseScalaTest {

  test("testEventValidationEventFromString") {
    var validClicks = 13
    var validImpressionsServed = 17
    var validImpressionsViewed = 17
    var errorClicks = 1
    var errorImpressionsServed = 1
    var errorImpressionsViewed = 1

    grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/testdata/ev_click.csv") {line =>
      EventValidation.eventFromString(line) match {
        case Success(event: ClickEvent) => {
          validClicks -= 1
        }
        case Failure(fail) => {
          println(fail.toString())
          errorClicks -= 1
        }
        case _ => //ignoring other events
      }
    }

    grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/testdata/ev_impressionServed.csv") {line =>
      EventValidation.eventFromString(line) match {
        case Success(event: ImpressionEvent) => {
          validImpressionsServed -= 1
        }
        case Failure(fail) => {
          errorImpressionsServed -= 1
        }
        case _ => //ignoring other events
      }
    }

    grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/testdata/ev_impressionViewed.csv") {line =>
      EventValidation.eventFromString(line) match {
        case Success(event: ImpressionViewedEvent) => {
          validImpressionsViewed -= 1
        }
        case Failure(fail) => {
          errorImpressionsViewed -= 1
        }
        case _ => //Ignoring other events
      }
    }

    assertEquals("Incorrect number of parsed clicks!", 0, validClicks)
    assertEquals("Incorrect number of click errors!", 0, errorClicks)
    assertEquals("Incorrect number of parsed impressions served!", 0, validImpressionsServed)
    assertEquals("Incorrect number of impression served errors!", 0, errorImpressionsServed)
    assertEquals("Incorrect number of parsed impressions viewed!", 0, validImpressionsViewed)
    assertEquals("Incorrect number of impression viewed errors!", 0, errorImpressionsViewed)

  }

  test("testEventValidationIsValidImpressionServedEvent") {
    var testUsers = 1
    var notWellFormed = 3
    var bots = 6
    var validImpressions = 4
    var errorImpressions = 1
    var invalidUsers = 1
    var invalidIps = 2

    grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/testdata/ev_impressionServed.csv") {line =>
      EventValidation.isValidImpressionServedEvent(line) match {
        case Success(e) => {
          validImpressions -= 1
        }
        case Failure(fail) => {
          for (result <- fail.list) {
            println(result.message + ": " + line)
            if (result.message == "Is not impression served event") {errorImpressions -= 1}
            if (result.message == "User is test user") {testUsers -= 1}
            if (result.message == "Bot") {bots -= 1}
            if (result.message == "Bad impression log date") {notWellFormed -= 1}
            if (result.message == "Invalid article id") {notWellFormed -= 1}
            if (result.message == "Invalid campaign key") {notWellFormed -= 1}
            if (result.message == "Invalid user") {invalidUsers -= 1}
            if (result.message == "Invalid IP") {invalidIps -= 1}
          }
        }
      }
    }

    assertEquals("Incorrect number of valid impressions!", 0, validImpressions)
    assertEquals("Incorrect number of test users!", 0, testUsers)
    assertEquals("Incorrect number of bots!", 0, bots)
    assertEquals("Incorrect number of ill formed impressions!", 0, notWellFormed)
    assertEquals("Incorrect number of impression parse errors!", 0, errorImpressions)
    assertEquals("Incorrect number of invalid user errors!", 0, invalidUsers)
    assertEquals("Incorrect number of invalid ips!", 0, invalidIps)
  }

  test("testEventValidationIsValidClickEvent") {
    var testUsers = 1
    var notWellFormed = 3
    var bots = 3
    var validClicks = 3
    var errorClicks = 1
    var invalidUsers = 1
    var invalidIps = 2

    grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/testdata/ev_click.csv") {line =>
      EventValidation.isValidClickEvent(line) match {
        case Success(e) => {
          validClicks -= 1
        }
        case Failure(fail) => {
          for (result <- fail.list) {
            if (result.message == "Is not click event") {errorClicks -= 1}
            if (result.message == "User is test user") {testUsers -= 1}
            if (result.message == "Bot") {bots -= 1}
            if (result.message == "Bad impression log date" || result.message ==  "Bad click log date") {notWellFormed -= 1}
            if (result.message == "Bad date clicked") {notWellFormed -= 1}
            if (result.message == "Bad impression hash") {notWellFormed -= 1}
            if (result.message == "Invalid user") {invalidUsers -= 1}
            if (result.message == "Invalid IP") {invalidIps -= 1}
          }
        }
      }
    }

    assertEquals("Incorrect number of valid clicks!", 0, validClicks)
    assertEquals("Incorrect number of test users!", 0, testUsers)
    assertEquals("Incorrect number of bots!", 0, bots)
    assertEquals("Incorrect number of ill formed clicks!", 0, notWellFormed)
    assertEquals("Incorrect number of click parse errors!", 0, errorClicks)
    assertEquals("Incorrect number of invalid user errors!", 0, invalidUsers)
    assertEquals("Incorrect number of invalid ips!", 0, invalidIps)
  }

  test("testEventValidationIsValidImpressionViewedEvent") {

    var testUsers = 1
    var notWellFormed = 3
    var bots = 3
    var validImpressionsViewed = 7
    var errorImpressionsViewed = 1
    var invalidUsers = 1
    var invalidIps = 2

    grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/testdata/ev_impressionViewed.csv") {line =>
      EventValidation.isValidImpressionViewedEvent(line) match {
        case Success(e) => {
          validImpressionsViewed -= 1
          println("success: " + line)
        }
        case Failure(fail) => {
          for (result <- fail.list) {
            println(result.message + ": " + line)
            if (result.message == "Is not impression viewed event") {errorImpressionsViewed -= 1}
            if (result.message == "User is test user") {testUsers -= 1}
            if (result.message == "Bot") {bots -= 1}
            if (result.message == "Bad md5 on the hashHex") {notWellFormed -= 1}
            if (result.message == "Bad date viewed") {notWellFormed -= 1}
            if (result.message == "Bad md5 on the pageViewIdHash") {notWellFormed -= 1}
            if (result.message == "Invalid user") {invalidUsers -= 1}
            if (result.message == "Invalid IP") {invalidIps -= 1}
          }
        }
      }
    }

    assertEquals("Incorrect number of valid impressions viewed!", 0, validImpressionsViewed)
    assertEquals("Incorrect number of test users!", 0, testUsers)
    assertEquals("Incorrect number of bots!", 0, bots)
    assertEquals("Incorrect number of ill formed impressions viewed!", 0, notWellFormed)
    assertEquals("Incorrect number of impression viewed parse errors!", 0, errorImpressionsViewed)
    assertEquals("Incorrect number of invalid user errors!", 0, invalidUsers)
    assertEquals("Incorrect number of invalid ips!", 0, invalidIps)
  }


  test("testEventValidationIsValidBeacon") {

    var testUsers = 2
    var bots = 2
    var notWellFormed = 1
    var errorBeacons = 3
    var validBeacons = 17
    var invalidIps = 1

    grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/testdata/ev_validatedBeacons.csv") {line =>
      EventValidation.isValidBeacon(line) match {
        case Success(e) => {
          validBeacons -= 1
          println("success: " + line)
        }
        case Failure(fail) => {
          for (result <- fail.list) {
            println(result.message + ": " + line)
            if (result.message == "Is not a beacon") {errorBeacons -= 1}
            if (result.message == "User is test user") {testUsers -= 1}
            if (result.message == "Bot") {bots -= 1}
            if (result.message == "Bad date") {notWellFormed -= 1}
            if (result.message == "Invalid IP") {invalidIps -= 1}
          }
        }
      }
    }

    assertEquals("Incorrect number of valid beacons!", 0, validBeacons)
    assertEquals("Incorrect number of test users!", 0, testUsers)
    assertEquals("Incorrect number of bots!", 0, bots)
    assertEquals("Incorrect number of well formed!", 0, notWellFormed)
    assertEquals("Incorrect number of beacon parse errors!", 0, errorBeacons)
    assertEquals("Incorrect number of invalid ips!", 0, invalidIps)
  }
}
