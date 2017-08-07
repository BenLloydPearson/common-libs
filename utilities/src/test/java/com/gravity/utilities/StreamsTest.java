package com.gravity.utilities;

import org.junit.Assert;
import junit.framework.TestCase;

import java.io.InputStream;
import java.io.OutputStream;

public class StreamsTest extends TestCase{

	public void testStreamToString() {
		InputStream is = getClass().getResourceAsStream("/dummystring.txt");

		String result = Streams.streamToString(is);
		String result2 = result.trim();
		Assert.assertEquals("mary had a little lamb", result2);
		
	}

  public void testStringBuilderToOutputStream() {
    InputStream is = getClass().getResourceAsStream("/name-date-dummydata.csv");

    String expected = Streams.streamToString(is);
    StringBuilder sb = new StringBuilder(expected);

    OutputStream os = Streams.stringBuilderToOutputStream(sb);
    String result = os.toString();

    Assert.assertEquals(expected, result);
  }
}
