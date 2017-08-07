package com.gravity.textutils.analysis.lingpipe;

import java.util.SortedSet;

import com.aliasi.lm.TokenizedLM;
import com.aliasi.tokenizer.EnglishStopTokenizerFactory;
import com.aliasi.tokenizer.IndoEuropeanTokenizerFactory;
import com.aliasi.tokenizer.LowerCaseTokenizerFactory;
import com.aliasi.tokenizer.PorterStemmerTokenizerFactory;
import com.aliasi.util.ScoredObject;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class TokenizedLMExtractor
{
  public Multiset<String> extract(String text, int ngramOrder, int numOfFreqTerms)
  {
    // remove punctutation, number and shrink mutliple whitespace characteres
    // into " "
    text = text.replaceAll("\\p{Punct}|\\d", "");
    text = text.replaceAll("\\s+", " ");

    // Chaining Token Factories
    IndoEuropeanTokenizerFactory ieTf = new IndoEuropeanTokenizerFactory();
    LowerCaseTokenizerFactory lowTf = new LowerCaseTokenizerFactory(ieTf);
    EnglishStopTokenizerFactory stopTf = new EnglishStopTokenizerFactory(lowTf);
    PorterStemmerTokenizerFactory portTf = new PorterStemmerTokenizerFactory(stopTf);

    TokenizedLM backgroundModel = new TokenizedLM(portTf, ngramOrder);
    backgroundModel.train(text);

    SortedSet<ScoredObject<String[]>> ngrams = backgroundModel.frequentTermSet(ngramOrder, numOfFreqTerms);
    // compose ngrams
    Multiset<String> ngramSet = HashMultiset.create();
    for (ScoredObject<String[]> so : ngrams) {
      String[] strs = so.getObject();
      StringBuilder sb = new StringBuilder();
      int i = 0;
      for (String str : strs) {
        if (i++ > 0)
          sb.append(" ");
        sb.append(str);
      }
      ngramSet.add(sb.toString());
    }
    return ngramSet;
  }

}
