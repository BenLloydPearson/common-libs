package com.gravity.textutils.analysis.lingpipe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.Multiset;

public class ExtractorsTest
{
  private static final String text = "I once read that pipers pecked pickled peppers,"
    + " but Long Cat reigns supreme over the John Birch Society!"
    + "  I also heard that Dick Cheney watches Sex and the City." 
    + "    px=0.314 but only px is kept!*@#$";

  @Test
  public void testExtractors()
  {
    testNgram(2, 10000);
    testNgram(3, 10000);
  }
  
  public void testNgram(int ngramSize, int numOfFreqTerms)
  {
    NgramExtractor ne = new NgramExtractor();
    Multiset<String> neNgrams = ne.extract(text, ngramSize, ngramSize);
    TokenizedLMExtractor tlme = new TokenizedLMExtractor();
    Multiset<String> tlmeNgrams = tlme.extract(text, ngramSize, numOfFreqTerms);

    assertEquals(neNgrams.size(), tlmeNgrams.size());
    for (String ngram : neNgrams)
      assertTrue(tlmeNgrams.contains(ngram));
  }

}
