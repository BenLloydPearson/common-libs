package com.gravity.textutils.analysis; /**
 * User: chris
 * Date: Jun 10, 2010
 * Time: 11:38:54 AM
 */

import com.google.common.collect.Multiset;
import com.gravity.utilities.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public abstract class LegacyPhraseExtractor {
  private static Logger logger = LoggerFactory.getLogger(LegacyPhraseExtractor.class);

  public abstract Multiset<String> extract(String text, int minGramSize, int maxGramSizeInclusive, ExtractionLevel none);

  public abstract Multiset<String> extract(String text, int minGramSize, int maxGramSizeInclusive, ExtractionLevel none, Set<String> matchedGrams);

  protected HashSet<String> loadWordsFromFile(String path) {
    try {
      final HashSet<String> wordsSet = new HashSet<String>();

      Streams.perLine(getClass().getResourceAsStream(path), new Streams.PerLine() {
        @Override
        public void line(String line) {
          if (line.length() > 0) {
            wordsSet.add(line.toLowerCase().trim());
          }
        }
      });

      return wordsSet;
    } catch (Exception exc) {
      throw new RuntimeException(exc);
    }
  }
}
