package com.gravity.utilities; /**
 * User: chris
 * Date: Sep 1, 2010
 * Time: 9:46:38 PM
 */

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MD5HashTest {
  private static final Logger logger = LoggerFactory.getLogger(MD5HashTest.class);

  @Test
  public void testOutputAgainstKnownResults() {
    String result = MD5Hash.MD5("http://www.printerfoods.com/printer-supplies/printer-accessories/inks-toners/laser/c54xx543x544-hy-return-prog-cyan-toner-cart/");
    String result2 = MD5Hash.MD5("http://mediamarketing-news.blogspot.com/2010/08/inc-september-2010.html?utm_source=feedburner&utm_medium=twitter&utm_campaign=Feed%3A+media-and-marketing+%28media++and+marketing%29");
    Assert.assertEquals("273a59491d3d558393099bb4e8760272",result);
    Assert.assertEquals("ba8be44237d45c8a87560b7cc313dd58",result2);
  }
}
