package com.gravity.utilities; /**
 * User: chris
 * Date: Jul 25, 2010
 * Time: 7:57:02 PM
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

public class Strings {
  private static Logger logger = LoggerFactory.getLogger(Strings.class);

  public static long convertNumericStringToLong(String numericValue) throws NumberFormatException {
    BigDecimal big;
    big = new BigDecimal(numericValue);
    return big.longValue();
  }

  static final BigDecimal BD_16 = new BigDecimal(16);
  static final String HEX_PREFIX = "0x";
  public static long convertHexStringToLong(String hexValue) {
    return convertHexStringToBigDecimal(hexValue).longValue();
  }

  public static BigDecimal convertHexStringToBigDecimal(String hexValue) {
    hexValue = stripHexPrefix(hexValue);
    BigDecimal big = new BigDecimal(0);
    char[] parts = hexValue.toCharArray();
    int lenMinus1 = parts.length - 1;
    for (int i = lenMinus1; i >= 0; i--) {
      String s = HEX_PREFIX + parts[i];
      BigDecimal cur = new BigDecimal(Integer.decode(s));
      int power = lenMinus1 - i;
      BigDecimal multiplier = Numbers.power(BD_16, power);
      BigDecimal product = cur.multiply(multiplier);
      big = big.add(product);
    }

    return big;
  }

  private static String stripHexPrefix(String hexValue) {
    if (hexValue.startsWith(HEX_PREFIX)) {
      hexValue = hexValue.substring(2);
    }
    return hexValue;
  }

  private static final Set<Character> NUMERIC_CHARACTER_SET = generateNumericCharacterSet();

  private static Set<Character> generateNumericCharacterSet() {
    Set<Character> result = new HashSet<Character>(10);
    for(int i = 0; i < 10; i++) {
      result.add(Integer.toString(i).charAt(0));
    }

    return result;
  }

  public static String unpackFromHex(String hexValue) throws NumberFormatException {
    if (IsNullOrEmpty(hexValue)) {
      return null;
    }
    hexValue = stripHexPrefix(hexValue);

    char[] parts = hexValue.toCharArray();
    char[] buffer = new char[parts.length*2];
    for (int i = 0, j = 0; i < parts.length; i++, j+=2) {
      char c = parts[i];
      switch (c) {
        case 'a':
          buffer[j] = '6';
          buffer[j+1] = '1';
          break;
        case 'b':
          buffer[j] = '6';
          buffer[j+1] = '2';
          break;
        case 'c':
          buffer[j] = '6';
          buffer[j+1] = '3';
          break;
        case 'd':
          buffer[j] = '6';
          buffer[j+1] = '4';
          break;
        case 'e':
          buffer[j] = '6';
          buffer[j+1] = '5';
          break;
        case 'f':
          buffer[j] = '6';
          buffer[j+1] = '6';
          break;
        default:
          if (false == NUMERIC_CHARACTER_SET.contains(c)) {
            throw new NumberFormatException(String.format("Character '%s' is not a hexidecimal character! Full hexidecimal string was: '%s'", c, hexValue));
          }
          buffer[j] = '3';
          buffer[j+1] = c;
      }
    }

    return new String(buffer);
  }

  public static String packToHex(String value) {
    if (IsNullOrEmpty(value)) {
      return null;
    }

    char[] chars = value.toCharArray();
    int halfLength = chars.length / 2;
    char[] buffer = new char[halfLength];
    for (int i = 0, j = 0; i < halfLength; i++, j+=2) {
      char p = chars[j];
      char n = chars[j+1];
      if (p == '6') {
        switch (n) {
          case '1':
            buffer[i] = 'a';
            break;
          case '2':
            buffer[i] = 'b';
            break;
          case '3':
            buffer[i] = 'c';
            break;
          case '4':
            buffer[i] = 'd';
            break;
          case '5':
            buffer[i] = 'e';
            break;
          case '6':
            buffer[i] = 'f';
            break;
        }
      } else if (p == '3') {
        buffer[i] = n;
      } else {
        throw new RuntimeException(
            String.format("Invalid format! Every odd digit must be a '3' or a '6'! Failed on pair # %d '%s%s' of full value: '%s'.",
              i, p, n, value));
      }
    }

    return new String(buffer);
  }

  public static int countUpperCase(String candidate) {
    int uppers = 0;
    for(char c : candidate.toCharArray()) {
      if(Character.isUpperCase(c)) {
        uppers++;
      }
    }
    return uppers;
  }

  public static int countNonLetters(String candidate) {
    int nonLetters = 0;
    for(char c : candidate.toCharArray()) {
      if(!Character.isSpaceChar(c) && !Character.isUpperCase(c) && !Character.isLowerCase(c) && !(c == '\'')) {
        nonLetters++;
      }
    }
    return nonLetters;
  }

  public static String combineNonEmptyStrings(String delim, String... parts) {
    if (parts == null) return null;
    StringBuilder sb = new StringBuilder();
    int i = 1;
    for (String part : parts) {
      if (part != null && part.length() > 0) {
        sb.append(part);
        if (delim != null && i < parts.length)
          sb.append(delim);
      }
      i++;
    }

    return sb.toString();
  }

  public static String escapeDoubleQuotes(String input) {
    if (input == null) return null;
    return input.replaceAll("\\\"", "\\\\\"");
  }

  public static boolean IsNullOrEmpty(String input) {
    if (input == null) return true;
    if (input.length() == 0) return true;
    return false;
  }

  public static boolean equals(String one, String two) {
    if (one == null && two == null)
      return true;

    if (one == null)
      return false;

    return one.equals(two);
  }

  public static int compare(String s1, String s2, boolean ignoreCase) {
    if (s1 == s2) return 0;
    if (s1 == null) return 1;
    if (s2 == null) return -1;
    if (ignoreCase) return s1.compareToIgnoreCase(s2);
    return s1.compareTo(s2);
  }
}
