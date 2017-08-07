package com.gravity.domain

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 3/6/14
 * Time: 11:24 AM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 * \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */

import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import org.junit.Assert._
import org.junit.Test

class MicroDollarTest {
  @Test def testToAndFromDollarValue() {
    val twentyFiveDollarsAndFiftySixCentsDollarValue = DollarValue(2556l)
    println(s"DollarValue String: $twentyFiveDollarsAndFiftySixCentsDollarValue")

    val twentyFiveDollarsAndFiftySixCentsMicroDollar = MicroDollar.fromDollarValue(twentyFiveDollarsAndFiftySixCentsDollarValue)

    println(s"MicroDollar String: $twentyFiveDollarsAndFiftySixCentsMicroDollar")

    assertEquals(twentyFiveDollarsAndFiftySixCentsDollarValue, twentyFiveDollarsAndFiftySixCentsMicroDollar.dollarValue)
  }

  @Test def testOperations() {
    val baseMicroDollarValue = MicroDollar(25560000l)

    val expectedTimes5 = MicroDollar(127800000l)

    assertEquals("Multiplied by 5", expectedTimes5, baseMicroDollarValue * 5)

    val expectedTimesTwentyFiveCentsInMicroDollars = MicroDollar(6390000000000l)

    assertEquals("Multiplied by 25 cents (in MicroDollars)", expectedTimesTwentyFiveCentsInMicroDollars, baseMicroDollarValue * MicroDollar(250000l))
  }

  @Test def testToAndFromMicroDollarString() {
    val stringValue = "25560000MD"
    val expected = MicroDollar(25560000l)

    MicroDollar.parse(stringValue) match {
      case Some(actual) =>
        assertEquals("Parse From String Equals", expected, actual)
        assertEquals("To String Equals", stringValue, actual.toString)
      case None => fail(s"Failed to even parse `$stringValue` as a MicroDollar instance!")
    }
  }

  @Test def testToAndFromPenniesString() {
    val stringPennyValue = "2556"
    val expected = MicroDollar(25560000l)

    MicroDollar.parse(stringPennyValue) match {
      case Some(actual) =>
        assertEquals("Parse From String Equals", expected, actual)
        assertEquals("To String Equals", stringPennyValue, actual.dollarValue.pennies.toString)
      case None => fail(s"Failed to even parse `$stringPennyValue` as a MicroDollar instance!")
    }
  }
}