package com.gravity.utilities

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 6/7/11
 * Time: 3:02 PM
 */
import org.junit.Test
import org.junit.Assert._
import java.util.Date
import com.gravity.utilities.grvdates._

class grvdatesTest {
  @Test def testParseDate() {
    val date: Date = parseDate("M/d/yyyy", "3/6/2011").get
    println(formatDate("MM/dd/yyyy", date))
  }
}