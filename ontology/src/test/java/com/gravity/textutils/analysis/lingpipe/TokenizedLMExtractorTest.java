package com.gravity.textutils.analysis.lingpipe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.Multiset;

public class TokenizedLMExtractorTest
{
  private static final String text = "I once read that pipers pecked pickled peppers,"
      + " but Long Cat reigns supreme over the John Birch Society!"
      + "  I also heard that Dick Cheney watches Sex and the City." 
      + "    px=0.314 but only px is kept!*@#$";

  @Test
  public void test2gram()
  {
    TokenizedLMExtractor tlme = new TokenizedLMExtractor();
    Multiset<String> ngrams = tlme.extract(text, 2, 10000);
    assertEquals(23, ngrams.size());
    assertTrue(ngrams.contains("john birch"));
    assertTrue(ngrams.contains("read piper"));
  }

  @Test
  public void test3gram()
  {
    TokenizedLMExtractor tlme = new TokenizedLMExtractor();
    Multiset<String> ngrams = tlme.extract(text, 3, 10000);
    assertEquals(22, ngrams.size());
    assertTrue(ngrams.contains("john birch societi"));
    assertTrue(ngrams.contains("watch sex citi"));
  }

}
