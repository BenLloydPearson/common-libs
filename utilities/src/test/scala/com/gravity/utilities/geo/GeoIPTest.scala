package com.gravity.utilities.geo

import org.junit.Assert._
import org.junit.Test

/**
 * User: mtrelinski
 */

class GeoIPTest {

  val testIPString = "108.13.230.170"
  val testIPAsInt = 1812850346l

  @Test
  def nothing() {
    assert(true, "good!")
  }

  @Test def testGeoLocationEmptyToString() {
    val expected = "!!!!!!!!!"
    val actual = GeoLocation.empty.toString

    assertEquals(expected, actual)
  }

  @Test def testGeoLocationIsEmpty() {
    val expected = true
    val actual = GeoLocation.empty.isEmpty

    assertEquals(expected, actual)
  }

  @Test def testIpAddressIsInvalid() {
    val actual = GeoDatabase.findByIpAddress("JASONWASHERE")

    assertEquals(None, actual)
  }

  @Test def testIpAddressIsValid() {
    val actual = GeoDatabase.findByIpAddress("64.236.138.3")

    assertEquals(true, actual.isDefined)
  }

  @Test def testIpAddressIsEmpty() {
    val actual = GeoDatabase.findByIpAddress("")

    assertEquals(None, actual)
  }

  @Test def testUS() {
    val actual = GeoDatabase.findByIpAddress("US")

    assertEquals(Some(GeoLocation.defaultUsGeoLocation), actual)
  }

  @Test def testIntl() {
    val actual = GeoDatabase.findByIpAddress("-US")

    assertEquals(Some(GeoLocation.defaultIntlGeoLocation), actual)
  }
}
