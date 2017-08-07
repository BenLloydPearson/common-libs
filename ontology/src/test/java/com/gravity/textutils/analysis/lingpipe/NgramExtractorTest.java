package com.gravity.textutils.analysis.lingpipe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.Multiset;

public class NgramExtractorTest
{
  private static final String text = "I once read that pipers pecked pickled peppers,"
    + " but Long Cat reigns supreme over the John Birch Society!"
    + "  I also heard that Dick Cheney watches Sex and the City." 
    + "    px=0.314 but only px is kept!*@#$";

  @Test
  public void testNgramExtractor()
  {
    NgramExtractor ne = new NgramExtractor();
    Multiset<String> ngrams = ne.extract(text, 2, 3);
    // for (String g : ngrams) {
    // System.out.println(g);
    // }
    assertEquals(45, ngrams.size());
    assertTrue(ngrams.contains("read piper"));
    assertTrue(ngrams.contains("john birch"));
    assertTrue(ngrams.contains("john birch societi"));
    assertTrue(ngrams.contains("watch sex citi"));

    // add
    ngrams.add("john birch");
    int i = 0;
    int j = 0;
    for (String g : ngrams) {
      if ("john birch".equals(g))
        j++;
      i++;
    }
    assertEquals(46, ngrams.size());
    assertEquals(46, i);
    assertEquals(2, j);
  }

  @Test
  public void test2gram()
  {
    NgramExtractor ne = new NgramExtractor();
    Multiset<String> ngrams = ne.extract(text, 2, 2);
    assertEquals(23, ngrams.size());
    assertTrue(ngrams.contains("read piper"));
    assertTrue(ngrams.contains("john birch"));
  }

  @Test
  public void test3gram()
  {
    NgramExtractor ne = new NgramExtractor();
    Multiset<String> ngrams = ne.extract(text, 3, 3);
    assertEquals(22, ngrams.size());
    assertTrue(ngrams.contains("john birch societi"));
    assertTrue(ngrams.contains("watch sex citi"));
  }
  
  @Test
  public void testPuncAndDigits()
  {
    NgramExtractor ne = new NgramExtractor();
    Multiset<String> ngrams = ne.extract("&quot;Anarchistic|socialism.&quot; &quot;''[[An Anarchist FAQ]]''by Various Authors", 3, 3);
    assertEquals(7, ngrams.size());
    assertTrue(ngrams.contains("social quot quot"));
    assertTrue(ngrams.contains("faq variou author"));
  }
}
