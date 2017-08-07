package com.gravity.utilities.geo

import com.gravity.utilities.BaseScalaTest
import org.scalatest.FunSuite

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class GeoEntryTest extends BaseScalaTest {

  def findById(id: String): GeoEntry = GeoDatabase.findById(id).getOrElse(fail("no database?"))

  test("zoom in canonicalize") {
    assertResult(List("NorthAmerica", "US", "US-MN"))(findById("US-MN").zoomIn.map(_.id))
  }

  test("zoom out canonicalize") {
    assertResult(List("US-MN", "US", "NorthAmerica"))(findById("US-MN").zoomOut.map(_.id))
  }

  test("isWithin") {
    assertResult(true)(findById("US").isWithin(findById("NorthAmerica")))
    assertResult(true)(findById("US-MN").isWithin(findById("NorthAmerica")))
    assertResult(true)(findById("CA").isWithin(findById("NorthAmerica")))
    assertResult(true)(findById("US-MN").isWithin(findById("US")))
    assertResult(false)(findById("US").isWithin(findById("Africa")))
    assertResult(false)(findById("US-MN").isWithin(findById("Africa")))
  }

}
