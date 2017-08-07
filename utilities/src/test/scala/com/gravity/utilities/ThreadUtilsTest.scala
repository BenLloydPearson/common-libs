package com.gravity.utilities

import org.junit.Test
import org.junit.Assert._

/**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: 5/17/11
 * Time: 9:04 PM
 */

class ThreadUtilsTest {
  @Test def testRunConcurrently() {

    val r = 0 to 1000

    val results = ThreadUtils.runConcurrently(() => {r.foreach(_ * 5)}, () => {r.foreach(_ * 5)}, () => {r.foreach(_ * 5)}, () => {r.foreach(_ * 5)}, () => {r.reverse.foreach(5 / _)})

    assertNotNull("Results should not be null!", results)
    assertEquals("Results length should be 5!", 5, results.length)
    println("Number of results: " + results.length)

    results.zipWithIndex.foreach({
      case (result,i) => {
        val expected = i < results.length - 1
        assertEquals("Result %d of %d should be %s!".format(i+1, results.length, expected), expected, result.succeeded)
        println(result)
      }
    })
  }
}
