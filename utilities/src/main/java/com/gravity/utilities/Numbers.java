package com.gravity.utilities; /**
 * User: chris
 * Date: Sep 15, 2010
 * Time: 1:44:49 PM
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class Numbers {
  private static final Logger logger = LoggerFactory.getLogger(Numbers.class);

  /**
   * Normalizes a number to a float between 0 and 1 where 1 is equivalent to the maximum value passed in.
   * @param number Number to normalize
   * @param maxValue Maximum possible value of number
   * @return
   */
  public static float normalize(int number, int maxValue) {
    if(number == 0) {
      return 0f;
    }
    float score = 1f / ((float)maxValue / (float)number);
    return score;
  }

  /**
   * Preforms division and returns the ceiling of the resulting quotient
   * @param dividend the number to be divided
   * @param divisor the number to divide by
   * @return the quotient that is one more if it does not divide evenly
   */
  public static int ceilingDivide(int dividend, int divisor) {
    int integerQuotient = dividend / divisor;
    if (dividend % divisor == 0) {
      return integerQuotient;
    }

    return ++integerQuotient;
  }

  /**
   * Take a 
   * @return
   */
  public static float invertAndNormalize(int number, int maxValue) {
    if(number > maxValue) {
      number = maxValue;
    }

    int inverse = maxValue - number;

    if(inverse == 0) {
      return 0f;
    }

    return normalize(inverse,maxValue);
  }

  public static long power(int n, int exponent) {
    if (exponent == 0) {
      return 1;
    }
    if (exponent % 2 == 0) {
      int x2 = exponent/2;
      long n2 = power(n, x2);
      return n2 * n2;
    }

    long result = n;
    for (int i = 1; i < exponent; i++) {
      result *= n;
    }

    return result;
  }

  public static long power(long n, int exponent) {
    if (exponent == 0) {
      return 1;
    }
    if (exponent % 2 == 0) {
      int x2 = exponent/2;
      long n2 = power(n, x2);
      return n2 * n2;
    }

    long result = n;
    for (int i = 1; i < exponent; i++) {
      result *= n;
    }

    return result;
  }

  static final BigDecimal BD1 = new BigDecimal(1);
  public static BigDecimal power(BigDecimal n, int exponent) {
    if (exponent == 0) {
      return BD1;
    }
    if (exponent % 2 == 0) {
      int x2 = exponent/2;
      BigDecimal n2 = power(n, x2);
      return n2.multiply(n2);
    }

    BigDecimal result = n;
    for (int i = 1; i < exponent; i++) {
      result = result.multiply(n);
    }

    return result;
  }
}
