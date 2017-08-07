package com.gravity.utilities; /**
 * User: chris
 * Date: Jul 25, 2010
 * Time: 7:58:03 PM
 */

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class StringsTest {
  private static Logger logger = LoggerFactory.getLogger(StringsTest.class);

  @Test
  public void testCountUpperCase() {
    Assert.assertEquals(3,Strings.countUpperCase("Baron John Rosen"));
    Assert.assertEquals(3,Strings.countUpperCase("LOL"));
    Assert.assertEquals(2,Strings.countUpperCase("Chris_Bissell"));
    Assert.assertEquals(2,Strings.countUpperCase("John Dunne"));
    Assert.assertEquals(0,Strings.countUpperCase("efrem zimbalist jr"));

  }

  @Test
  public void testCountNonLetters() {
    Assert.assertEquals(2,Strings.countNonLetters(":)"));
    Assert.assertEquals(2,Strings.countNonLetters("Hi there peeps :)"));
  }

  @Test
  public void testNumericStringToLong() {
    String numeric = "3235313338663935633531646566653030313237633663633265653064303133";
    long asLong = Strings.convertNumericStringToLong(numeric);
    logger.info("{} is represented as {} as a long", numeric, asLong);
    String hex = "25138f95c51defe00127c6cc2ee0d013";
    BigDecimal fromHex = Strings.convertHexStringToBigDecimal(hex);
    logger.info("{} is represented as {} as a BigDecimal", hex, fromHex);

    //Assert.assertEquals(2671636877380808672l, fromHex);
  }

  @Test
  public void testUnpackFromHex() {
    String numeric = "3235313338663935633531646566653030313237633663633265653064303133";
    String hexValue = "25138f95c51defe00127c6cc2ee0d013";
    String result = Strings.unpackFromHex(hexValue);
    Assert.assertEquals(numeric, result);
  }

  @Test(expected = NumberFormatException.class)
  public void testUnpackFromHexWithAnInvalidHex() {
    String hexValue = "http://cc.bingj.com/cache.aspx?q=ass japanese";
    try {
      Strings.unpackFromHex(hexValue);
    } catch (NumberFormatException e) {
//      logger.error("Expected exception caught and logged.", e);
    }
    Strings.unpackFromHex(hexValue);
  }

  @Test
  public void testPackToHex() {
    String numeric = "3235313338663935633531646566653030313237633663633265653064303133";
    String hexValue = "25138f95c51defe00127c6cc2ee0d013";
    String result = Strings.packToHex(numeric);
    Assert.assertEquals(hexValue, result);
  }
}
