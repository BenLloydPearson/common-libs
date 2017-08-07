package com.gravity.textutils.analysis.lingpipe;

import com.aliasi.tokenizer.EnglishStopTokenizerFactory;
import com.aliasi.tokenizer.IndoEuropeanTokenizerFactory;
import com.aliasi.tokenizer.LowerCaseTokenizerFactory;
import com.aliasi.tokenizer.PorterStemmerTokenizerFactory;
import com.aliasi.tokenizer.Tokenizer;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class NgramExtractor
{
  /**
   * This class applies lingpipe's IndoEuropeanTokenizerFactory,
   * LowerCaseTokenizerFactory, EnglishStoptokenizerFactory,
   * PorterStemmerTokenizerFactor to generate tokens in a string array. It then
   * group these tokens in ngram of appropriate size.
   * 
   * @param text
   *          input text
   * @param minGramSize
   *          minimum size of ngram which must be greater 0.
   * @param maxGramSize
   *          maximum isze of ngram which must be >= minGramSize.
   * @return Multiset of ngrams
   */
  public Multiset<String> extract(String text, int minGramSize, int maxGramSize)
  {
    if (minGramSize < 1 || minGramSize > maxGramSize)
      throw new IllegalArgumentException(String.format("Incorrect arguments: minGramSize=%d, maxGramSize=%d",
          minGramSize, maxGramSize));
    
    // remove punctutation, number and shrink mutliple whitespace characteres into " "
    text = text.replaceAll("\\p{Punct}|\\d", " ");
    text = text.replaceAll("\\s+", " ");
    
    // Chaining Token Factories
    IndoEuropeanTokenizerFactory ieTf = new IndoEuropeanTokenizerFactory();
    LowerCaseTokenizerFactory lowTf = new LowerCaseTokenizerFactory(ieTf);
    EnglishStopTokenizerFactory stopTf = new EnglishStopTokenizerFactory(lowTf);
    PorterStemmerTokenizerFactory portTf = new PorterStemmerTokenizerFactory(stopTf);
    Tokenizer tokenizer = portTf.tokenizer(text.toCharArray(), 0, text.length());
    String[] tokens = tokenizer.tokenize();

    // compose ngrams
    Multiset<String> grams = HashMultiset.create();
    for (int ns = minGramSize; ns <= maxGramSize; ns++) {
      for (int i = 0; i < tokens.length - ns + 1; i++) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < ns; j++) {
          sb.append(tokens[i + j]);
          // Add a space between words
          if (j < ns - 1)
            sb.append(" ");
        }
        grams.add(sb.toString());
      }
    }
    return grams;

  }
}
