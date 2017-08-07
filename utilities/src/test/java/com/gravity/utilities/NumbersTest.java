package com.gravity.utilities; /**
 * User: chris
 * Date: Sep 15, 2010
 * Time: 1:47:05 PM
 */

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class NumbersTest {
  private static final Logger logger = LoggerFactory.getLogger(NumbersTest.class);
    private static final double defaultDelta = 0.1;

  @Test
  public void testNormalize() {
    int maxValue = 20;
    int original = 15;
    float score = Numbers.normalize(original,maxValue);
    Assert.assertEquals(0.75f, score, defaultDelta);

    int original2 = 0;
    float score2 = Numbers.normalize(original2,maxValue);
    Assert.assertEquals(0f,score2, defaultDelta);

    int original3 = 20;
    float score3 = Numbers.normalize(original3,maxValue);
    Assert.assertEquals(1f,score3, defaultDelta);

  }

  @Test
  public void testInvertAndNormalize() {
    int maxValue = 20;
    int original = 15;

    float score = Numbers.invertAndNormalize(original,maxValue);

    Assert.assertEquals(0.25f,score, defaultDelta);

    int original2 = 5;
    float score2 = Numbers.invertAndNormalize(original2,maxValue);

    Assert.assertEquals(0.75f,score2, defaultDelta);
  }

  @Test
  public void testPower() {
    int initial = 2;
    long result = Numbers.power(initial, 3);
    Assert.assertEquals(8l, result);

    result = Numbers.power(initial, 8);
    Assert.assertEquals(256l, result);

    result = Numbers.power(initial, 14);
    Assert.assertEquals(16384l, result);

    result = Numbers.power(initial, 0);
    Assert.assertEquals(1l, result);

    long initialL = 2l;
    long resultL = Numbers.power(initialL, 3);
    Assert.assertEquals(8l, resultL);

    resultL = Numbers.power(initialL, 8);
    Assert.assertEquals(256l, resultL);

    resultL = Numbers.power(initialL, 14);
    Assert.assertEquals(16384l, resultL);

    resultL = Numbers.power(initialL, 0);
    Assert.assertEquals(1l, resultL);

    BigDecimal initialBD = new BigDecimal(2);
    BigDecimal resultBD = Numbers.power(initialBD, 3);
    Assert.assertEquals(new BigDecimal(8), resultBD);

    resultBD = Numbers.power(initialBD, 8);
    Assert.assertEquals(new BigDecimal(256), resultBD);

    resultBD = Numbers.power(initialBD, 14);
    Assert.assertEquals(new BigDecimal(16384), resultBD);

    resultBD = Numbers.power(initialBD, 0);
    Assert.assertEquals(new BigDecimal(1), resultBD);
  }
}
