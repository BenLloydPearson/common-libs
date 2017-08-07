package com.gravity.textutils.analysis;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.gravity.utilities.ArrayUtils;
import com.gravity.utilities.Settings;
import opennlp.tools.chunker.ChunkerME;
import opennlp.tools.chunker.ChunkerModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.apache.commons.lang.StringUtils;

import java.util.*;

public class AnandPhraseExtractorV2 extends LegacyPhraseExtractor {
  //private static Logger logger = LoggerFactory.getLogger(AnandPhraseExtractorV2.class);

  private SentenceDetectorME sentenceDetector;
  private POSTaggerME posTagger;
  private ChunkerME chunker;
  private HashSet<String> stopWords;
  private HashSet<String> commonUnigrams;
  private HashSet<String> blacklistedGrams;

  private int PhraseWindow = 3;

  private ArrayList<String> stopCrossWords = new ArrayList<String>();
  private String[] stopCrossWordsArray = new String[] {"after", "where", "when", "for", "at", "to", "with", "earlier"};

  public AnandPhraseExtractorV2(String path) {
    try {
      SentenceModel sentenceModel = new SentenceModel(getClass().getResourceAsStream("en-sent.bin"));
      sentenceDetector = new SentenceDetectorME(sentenceModel);

      POSModel posModel = new POSModel(getClass().getResourceAsStream("en-pos-maxent.bin"));
      posTagger = new POSTaggerME(posModel);

      ChunkerModel chunkerModel = new ChunkerModel(getClass().getResourceAsStream("en-chunker.bin"));
      chunker = new ChunkerME(chunkerModel);

      // Load the stop words and the stop cross words
      stopWords = loadWordsFromFile("stopwords");
      Collections.addAll(stopCrossWords, stopCrossWordsArray);

      // Load the common unigrams
      commonUnigrams = loadWordsFromFile("commonunigrams");

      blacklistedGrams = loadWordsFromFile("blacklisted_grams2.txt");
    }
    catch (Exception exc) {
      throw new RuntimeException(exc);
    }
  }

  public static final ThreadLocal<AnandPhraseExtractorV2> Extractor = new ThreadLocal<AnandPhraseExtractorV2>() {
    @Override
    protected AnandPhraseExtractorV2 initialValue() {
      return new AnandPhraseExtractorV2(Settings.NLP_DIRECTORY);
    }
  };

  public boolean isStopWord(String word) {
    return stopWords.contains(word.toLowerCase());
  }

  public String removePunctuation(String text) {
    return text.replaceAll("[^\\p{L}]", "");
  }

  public String removePunctuationFromEndOfWord(String word) {
    return word.replaceAll("[^\\p{L}]$", "");
  }

  public String removeUrls(String text) {
    return text.replaceAll("/\\w+://[^\\s]+?(?=\\.?(?= |$))/", "");
  }

  public String[] getTags(String[] tokens) {
    return posTagger.tag(tokens);
  }

  public String[] getChunks(String[] tokens, String[] posTags) {
    return chunker.chunk(tokens, posTags);
  }

  public String prepareSentenceForTagging(String sentence) {
    String buffer = sentence.replaceAll("/--+/", " -- ");
    buffer = buffer.replaceAll("/\\{|\\[/", "(");
    buffer = buffer.replaceAll("/\\}|\\]/", ")");
    // Accents and ticks
    buffer = buffer.replace("\u2019", "'");
    buffer = buffer.replace("`", "'");
    // Double quote variants
    buffer = buffer.replace("\u201C", "\"");
    buffer = buffer.replace("\u201D", "\"");
    // Back to normal
    buffer = buffer.replaceAll("/([\\w])\\.\\.+([\\w])/", "$1 , $2");
    buffer = buffer.replaceAll("/(^| )(\"|\\(|\\)|;|-|:|-|\\*|,)+/", " , ");
    buffer = buffer.replaceAll("/(\"|\\(|\\)|;|-|:|-|\\*|,)( |$)/", " , ");
    buffer = buffer.replaceAll("/(\\.+ |')/", " $1");
    buffer = buffer.replaceAll("/ / /", " , ");
    buffer = buffer.replaceAll("/(,|\\.) *,/", " $1 ");
    buffer = buffer.replaceAll("/(,| )+$/", "");
    buffer = buffer.replaceAll("/^(,| )+/", "");
    buffer = buffer.replaceAll("/((?:\\.|!|\\?)+)$/", " $1");
    buffer = buffer.replaceAll("/(!|\\?|\\.)+/", " $1 ");
    buffer = buffer.replaceAll("/\\s+/", " ");
    return buffer;
  }

  public String prepareTextForSentenceDetection(String text) {
    String buffer = text.replaceAll("/\\r(\\n?)/", "\\n");
    buffer = buffer.replaceAll("/^\\s+$/", "");
    buffer = buffer.replaceAll("/\\n\\n+/", ".\\n.\\n");
    buffer = buffer.replaceAll("/\\n/", " ");
    buffer = buffer.replaceAll("/(\\d+)\\./", ". $1 . ");
    buffer = buffer.trim();
    return buffer;
  }

  public ArrayList<String> extractSentences(String text) {
    ArrayList<String> sentences = new ArrayList<String>();

    for (String sentence : sentenceDetector.sentDetect(prepareTextForSentenceDetection(text))) {
      sentence = sentence.trim();
      if (sentence.length() > 0 && !sentence.matches("/^(\\.|!|\\?)+$/")) {
        sentences.add(sentence);
      }
    }

    return sentences;
  }

  private Multiset<String> extractNgrams(String text, int windowSize) {
    Multiset<String> grams = HashMultiset.create();

    String[] splitText = text.split("\\s+");

    if (splitText.length > 0) {
      for (int i = 0; i < splitText.length; i++) {
        ArrayList<String> currentPhraseL = new ArrayList<String>();
        String currentPhrase = "";

        // Unigrams are a special case
        if (windowSize == 1) {
          grams.add(removePunctuation(splitText[i]));
        }
        else {
          if (i + windowSize <= splitText.length) {
            for (int j = i; j < i + windowSize; j++) currentPhraseL.add(removePunctuation(splitText[j]));
            for (String currentWord : currentPhraseL) currentPhrase += currentWord + " ";
            grams.add(currentPhrase.trim());
          }
        }
      }
    }

    return grams;
  }

  public boolean detectCanCross(String token, String posTag, String chunk) {
    return (
        !posTag.matches(",") &&
            !stopCrossWords.contains(token.toLowerCase()) &&
            !chunk.equals("B-VP") &&
            !token.matches("<\\w+>")
    );
  }

  private Multiset<String> extractPhrases(String text, int minGramSize, int maxGramSizeInclusive, Set<String> matchedGrams) {

    HashSet<String> phraseReductions = new HashSet<String>();
    Multiset<String> phrases = HashMultiset.create();
    ArrayList<PhraseExtraction> extractions = new ArrayList<PhraseExtraction>();

    ArrayList<String> sentences = extractSentences(text);

    for (String sentence : sentences) {
      sentence = removeUrls(sentence);
      sentence = prepareSentenceForTagging(sentence);

      String[] tokens = sentence.split(" ");

      // Remove ending punctuation from every token in the sentence
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = removePunctuationFromEndOfWord(tokens[i]);
      }

      // Sanitize tokens (@, w/[a-z])
      
      String[] posTags = posTagger.tag(tokens);
      String[] chunks = chunker.chunk(tokens, posTags);

      // Calculate the phrase boundaries
      List<LegacyPhraseBoundary> boundaries = calculateBoundaries(tokens, posTags, chunks);

      extractions.addAll(extractPhrasesByWindow(boundaries, tokens, PhraseWindow));
    }

    // Upgrade extractions to textutils
    for (int i = 0; i < extractions.size(); i++) {
      PhraseExtraction ext = extractions.get(i);
      String bestFit = ext.toString();
      for (int j = i+1; j < extractions.size(); j++) {
        String secondExtraction = extractions.get(j).toString();
        if (secondExtraction.contains(bestFit)) {
          bestFit = secondExtraction;
        }
        else {
          break;
        }
      }
      phraseReductions.add(bestFit);
    }

    // Set the correct multiset counts
    for (String phrase : phraseReductions) {
      // Skip if the phrase is a common unigram
      if (commonUnigrams.contains(phrase.toLowerCase()) || blacklistedGrams.contains(phrase.toLowerCase())) {
        continue;
      }
      // Check the minimum and maximum splits
      String[] phraseText = phrase.split(" ");
      if (phraseText.length >= minGramSize && phraseText.length <= maxGramSizeInclusive && !matchedGrams.contains(phrase.toString())) {
        int count = StringUtils.countMatches(text, phrase);
        phrases.add(phrase.toString());
        phrases.setCount(phrase.toString(), count);        
      }
    }

    return phrases;
  }

  public Multiset<String> reducePhrases(String text, HashSet<String> phraseReductions, int minGramSize, int maxGramSizeInclusive, Set<String> matchedGrams) {
    Multiset<String> phrases = HashMultiset.create();
    for (String phrase : phraseReductions) {
      // Skip if the phrase is a common unigram
      if (commonUnigrams.contains(phrase.toLowerCase()) || blacklistedGrams.contains(phrase.toLowerCase())) {
        continue;
      }
      // Check the minimum and maximum splits
      String[] phraseText = phrase.split(" ");
      if (phraseText.length >= minGramSize && phraseText.length <= maxGramSizeInclusive && !matchedGrams.contains(phrase.toString())) {
        int count = StringUtils.countMatches(text, phrase);
        phrases.add(phrase.toString());
        phrases.setCount(phrase.toString(), count);
      }
    }

    return phrases;
  }

  public List<PhraseExtraction> compileExtractions(String text) {
    ArrayList<PhraseExtraction> extractions = new ArrayList<PhraseExtraction>();

    ArrayList<String> sentences = extractSentences(text);

    for (String sentence : sentences) {
      sentence = removeUrls(sentence);
      sentence = prepareSentenceForTagging(sentence);

      String[] tokens = sentence.split(" ");

      // Remove ending punctuation from every token in the sentence
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = removePunctuationFromEndOfWord(tokens[i]);
      }

      // Sanitize tokens (@, w/[a-z])

      String[] posTags = posTagger.tag(tokens);
      String[] chunks = chunker.chunk(tokens, posTags);

      // Calculate the phrase boundaries
      List<LegacyPhraseBoundary> boundaries = calculateBoundaries(tokens, posTags, chunks);

      extractions.addAll(extractPhrasesByWindow(boundaries, tokens, PhraseWindow));
    }

    return extractions;
  }

  public static Set<String> upgradeExtractions(List<PhraseExtraction> extractions) {
    Set<String> phraseReductions = new HashSet<String>();
    for (int i = 0; i < extractions.size(); i++) {
      PhraseExtraction ext = extractions.get(i);
      String bestFit = ext.toString();
      for (int j = i+1; j < extractions.size(); j++) {
        String secondExtraction = extractions.get(j).toString();
        if (secondExtraction.contains(bestFit)) {
          bestFit = secondExtraction;
        }
      }
      phraseReductions.add(bestFit);
    }

    return phraseReductions;
  }

  public List<LegacyPhraseBoundary> calculateBoundaries(String[] tokens, String[] posTags, String[] chunks) {
    List<LegacyPhraseBoundary> boundaries = new ArrayList<LegacyPhraseBoundary>();
    for (int i = 0; i < tokens.length; i++) {
      String token = tokens[i];
      String posTag = posTags[i];
      String chunk = chunks[i];
      LegacyPhraseBoundary boundary = new LegacyPhraseBoundary();

      // Check if the token can cross
      boundary.setCanCross(
        !posTag.matches(",") &&
        !stopCrossWords.contains(token.toLowerCase()) &&
        !chunk.equals("B-VP") &&
        !token.matches("<\\w+>")
      );

      // Check if the token can start
      boundary.setCanStart(chunk.equals("B-NP"));
      if (i > 0) {
        if (chunk.equals("I-NP") && posTags[i-1].matches("DT|WDT|PRP|JJR|JJS|CD")) boundary.setCanStart(true);
      }

      // Check if the token can end
      if (i != tokens.length-1) {
        boundary.setCanEnd(!chunks[i+1].matches("I-"));
      }
      else {
        boundary.setCanEnd(true);
      }

      // Check the tags
      if (posTag.equals("CC")) {
        if (i > 0) {
          LegacyPhraseBoundary tempBoundary = boundaries.get(i-1);
          tempBoundary.setCanEnd(false);
          boundaries.set(i-1, tempBoundary);
        }
        boundary.setCanCross(false);
      }

      if (i > 0 && posTags[i-1].equals("CC")) {
        boundary.setCanStart(true);
      }

      // Stop word check
      if (isStopWord(token)) {
        boundary.setCanEnd(false);
        boundary.setCanStart(false);
      }
      if (i != tokens.length-1) {
        // Some stop words have single quotes that are exploded when we chunk - must scan ahead
        String combinedToken = token + tokens[i+1];
        if (isStopWord(combinedToken)) {
          boundary.setCanEnd(false);
          boundary.setCanStart(false);
        }
      }

      // Dangler quotes
      if (i > 0 && token.matches("^'")) {
        boundary.setCanStart(false);
        LegacyPhraseBoundary tempBoundary = boundaries.get(i-1);
        tempBoundary.setCanEnd(false);
        boundaries.set(i-1, tempBoundary);
      }

      boundary.setCanStart(
        boundary.isCanStart() &&
        !posTag.matches("CC|PRP|IN|DT|PRP\\$|WP|WP\\$|TO|EX|JJR|JJS")
      );

      boundary.setCanEnd(
        boundary.isCanEnd() &&
        posTag.matches("NN|NNS|NNP|NNPS|FW|CD")
      );

      boundaries.add(boundary);
    }

    return boundaries;
  }

  public static Collection<PhraseExtraction> extractPhrasesByWindow(List<LegacyPhraseBoundary> boundaries, String[] tokens, int phraseWindow) {
    ArrayList<PhraseExtraction> extractions = new ArrayList<PhraseExtraction>();
    int i = 0;
    int j = 0;

    while (i < tokens.length) {
      if (!boundaries.get(i).isCanStart() || !boundaries.get(i).isCanCross()) {
        i++;
        continue;
      }
      if (j < i) {
        j = i;
      }
      if (j == tokens.length || !boundaries.get(j).isCanCross() || (j >= i + phraseWindow)) {
        i++;
        j = i;
        continue;
      }
      if (!boundaries.get(j).isCanEnd()) {
        j++;
        continue;
      }
      
      PhraseExtraction extraction = new PhraseExtraction(ArrayUtils.subArray(tokens, i, j + 1));

      if (extraction.isSignificant()) {
        extractions.add(extraction);
      }

      j++;
    }
    return extractions;
  }

  @Override
  public Multiset<String> extract(String text, int minGramSize, int maxGramSizeInclusive, ExtractionLevel none) {
    Set<String> matchedGrams = new HashSet<String>();
    return extract(text, minGramSize, maxGramSizeInclusive, none, matchedGrams);
  }

  @Override
  public Multiset<String> extract(String text, int minGramSize, int maxGramSizeInclusive, ExtractionLevel extractionLevel, Set<String> matchedGrams) {
    // Prune the text - bail early if there's nothing there
    if (text == null || text.trim().length() == 0) return HashMultiset.create();
    text = text.trim();

    // Run the appropriate extraction
    if (extractionLevel == ExtractionLevel.NGRAMS) {
      Multiset<String> grams = HashMultiset.create();

      if (minGramSize >= maxGramSizeInclusive) throw new RuntimeException("The minGramSize must be less than the maxGramSizeInclusive");

      for (int windowSize = minGramSize; windowSize <= maxGramSizeInclusive; windowSize++) {
        Multiset<String> returnedGrams = extractNgrams(text, windowSize);
        grams.addAll(returnedGrams);
      }

      Set<String> badIdentifiedGrams = new HashSet<String>();

      // Build up a list of bad grams
      for (String matchedGram : grams) {
        if (blacklistedGrams.contains(matchedGram.toLowerCase())) {
          badIdentifiedGrams.add(matchedGram);
        }
      }

      grams.removeAll(badIdentifiedGrams);

      return grams;
    }
    else if (extractionLevel == ExtractionLevel.PHRASES) {
      return extractPhrases(text, minGramSize, maxGramSizeInclusive, matchedGrams);
    }
    else {
      throw new RuntimeException("Invalid Extraction Level");
    }
  }
}
