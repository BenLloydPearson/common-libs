package com.gravity.utilities; /**
 * User: chris
 * Date: Sep 1, 2010
 * Time: 9:45:58 PM
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Hash {
  private static final Logger logger = LoggerFactory.getLogger(MD5Hash.class);

  public static BigInteger hashString(String data) {
    try {
      MessageDigest instance = MessageDigest.getInstance("MD5");
      instance.update(data.getBytes("UTF-8"));
      byte[] bytes = instance.digest();
      instance.reset();
      BigInteger bi = new BigInteger(bytes);
      return bi;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static String convertToHex(byte[] data) {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < data.length; i++) {
      int halfbyte = (data[i] >>> 4) & 0x0F;
      int two_halfs = 0;
      do {
        if ((0 <= halfbyte) && (halfbyte <= 9)) {
          buf.append((char) ('0' + halfbyte));
        } else {
          buf.append((char) ('a' + (halfbyte - 10)));
        }
        halfbyte = data[i] & 0x0F;
      } while (two_halfs++ < 1);
    }
    return buf.toString();
  }

  public static String MD5(String text) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    byte[] md5hash;
      md.update(text.getBytes());
    md5hash = md.digest();
    return convertToHex(md5hash);
  }


}
