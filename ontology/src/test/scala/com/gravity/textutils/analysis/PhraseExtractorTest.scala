package com.gravity.textutils.analysis

import org.junit.Test
import org.junit.Assert._


class PhraseExtractorTest {
 import com.gravity.logging.Logging._

  def standardExtract(text: String) = {
    val pe = PhraseExtractorInterop.getLegacyExtractor
    pe.extract(text, 1, 8, ExtractionLevel.PHRASES)
  }

  @Test
  def extractNGrams() {
    val text = "I once read that pipers pecked pickled peppers, but Long Cat reigns supreme over the John Birch Society!  I also heard that Dick Cheney watches Sex and the City"
    val pe = PhraseExtractorInterop.getLegacyExtractor
    val matches = pe.extract(text, 3, 7, ExtractionLevel.NGRAMS)
    assertTrue(matches.contains("John Birch Society"))
    assertTrue(matches.contains("pecked pickled peppers"))
    assertTrue(matches.contains("pipers pecked pickled peppers"))
    assertTrue(matches.contains("I once read that pipers pecked pickled"))
    assertFalse(matches.contains("I once read that pipers pecked pickled peppers")) // out of bounds check
  }

  @Test
  def capitalizedHeadlines() {
    val text = "Here's Another Reason AOL Is Buying TechCrunch"
    val pe = PhraseExtractorInterop.getLegacyExtractor
    val matches = pe.extract(text, 1, 7, ExtractionLevel.NGRAMS)
    assertTrue(matches.contains("TechCrunch"))

    val text2 = "Belvedere Vodka";
    val matches2 = pe.extract(text2, 1, 7, ExtractionLevel.NGRAMS)
    assertTrue(matches2.contains("Belvedere Vodka"))
  }

  @Test
  def extractAndCountNGrams() {
    val text = "I once read that pipers pecked pickled peppers, but Long Cat reigns supreme over the John Birch Society!  I also heard that Dick Cheney watches Sex and the City"
    val pe = PhraseExtractorInterop.getLegacyExtractor
    val matches = pe.extract(text, 3, 7, ExtractionLevel.NGRAMS)
    assertEquals(1, matches.count("John Birch Society"))
  }

  @Test def absenseOfFragments() {
    val text = "I went to Mount Everest"
    val results = PhraseExtractorInterop.getLegacyExtractor.extract(text, 1, 7, ExtractionLevel.PHRASES)
    assertFalse(results.contains("I"))
    assertFalse(results.contains("went"))
    assertFalse(results.contains("to"))
  }

  @Test def multipleOccurrences() {
    val text = "I went to Mount Everest.  Turns out Mount Everest has a lot of snow!"
    val results = PhraseExtractorInterop.getLegacyExtractor.extract(text, 2, 7, ExtractionLevel.PHRASES)
    assertEquals(2, results.count("Mount Everest"))
  }

  @Test def princeOfPersia() {
    val text = "I played Prince of Persia last week"
    val results = PhraseExtractorInterop.getLegacyExtractor.extract(text, 3, 7, ExtractionLevel.PHRASES)
    assertEquals(1, results.count("Prince of Persia"))
  }

  @Test def differentLanguageModels() {
    val text = "I went to see Cirque du Soleil last night"
    val results = PhraseExtractorInterop.getLegacyExtractor.extract(text, 3, 7, ExtractionLevel.PHRASES)
    assertEquals(1, results.count("Cirque du Soleil"))
  }

  @Test def hyphenatedCompoundWords() {
    val text = "They are called the A-Team"
    val results = PhraseExtractorInterop.getLegacyExtractor.extract(text, 1, 7, ExtractionLevel.PHRASES)
    assertTrue(results.contains("A-Team"))
  }

  @Test def multipleProperNouns() {
    val text = "I went to Mount Everest with Edmund Hillary"
    val results = PhraseExtractorInterop.getLegacyExtractor.extract(text, 2, 7, ExtractionLevel.PHRASES)
    assertTrue(results.contains("Mount Everest"))
    assertTrue(results.contains("Edmund Hillary"))
  }

  @Test def prioritizationOfCompoundWords() {
    val text = "I went to Mount Everest"
    val results = PhraseExtractorInterop.getLegacyExtractor.extract(text, 2, 7, ExtractionLevel.PHRASES)
    assertTrue(results.contains("Mount Everest"))
    assertFalse(results.contains("Mount"))
    assertFalse(results.contains("Everest"))
  }

  @Test def extractionPipeline() {
    val text = "I once read that pipers pecked pickled peppers, but Long Cat reigns supreme over the John Birch Society!  I also heard that Dick Cheney watches Sex and the City";
    val pe = PhraseExtractorInterop.getLegacyExtractor

    // First let's extract the huge grams with no matching. In this example we should get back all the 3-7 grams
    val matches = pe.extract(text, 3, 7, ExtractionLevel.NGRAMS)
    assertTrue(matches.contains("John Birch Society"))
    assertTrue(matches.contains("pecked pickled peppers"))
    assertTrue(matches.contains("pipers pecked pickled peppers"))

    // Let's pretend we took the results of the above and found "John Birch Society" in the ontology.  We'll now pass that
    // back to the extractor so it knows not to match the sub-textutils in that larger phrase.

    val matchedGrams = new java.util.HashSet[String]()
    matchedGrams.add("John Birch Society")

    // Now that we've found the large grams, let's go back to the extractor and do the smaller matches will all the rules turned on.
    val secondLevelMatches = pe.extract(text, 1, 2, ExtractionLevel.PHRASES, matchedGrams)
    assertTrue(secondLevelMatches.contains("Long Cat"))
    assertFalse(secondLevelMatches.contains("John Birch Society"))
    assertFalse(secondLevelMatches.contains("John Birch"))
    assertFalse(secondLevelMatches.contains("Birch"))
    assertFalse(secondLevelMatches.contains("Society"))
  }

  @Test def extractionOfProperNouns() {
    val text = "I went to Mount Everest"
    val results = PhraseExtractorInterop.getLegacyExtractor.extract(text, 2, 7, ExtractionLevel.PHRASES)
    assertTrue(results.contains("Mount Everest"))
    assertEquals(1, results.count("Mount Everest"))

    // Deal with wonky punctuation
    val text2 = "Art Walk in Culver City! Fun!"
    val matches = standardExtract(text2)
    assertTrue(matches.contains("Culver City"))
  }

  @Test def removalOfPunctuation() {
    val text = "LOL!!! I had so much fun at the Green Gables!"
    val results = PhraseExtractorInterop.getLegacyExtractor.extract(text, 2, 7, ExtractionLevel.NGRAMS)
    assertFalse(results.contains("LOL!!!"))
    assertFalse(results.contains("!!!"))
    assertFalse(results.contains("!"))
    assertTrue(results.contains("Green Gables"))
    assertFalse(results.contains("Green Gables!"))

    val text1 = "Just watched Thing One sing the blues @ school w/her classmates. Wow, she's grown!"
    val matches = standardExtract(text1)
    assertFalse(matches.contains("Wow, she's"))
    assertFalse(matches.contains("blues @ school"))

    val text2 = "found an odd requirement to working with the Facebook GraphAPI with python. You must drink a Red Stripe Beer!"
    val redString = standardExtract(text2)
    assertFalse(redString.contains("Red Stripe Beer!"))
    assertTrue(redString.contains("Red Stripe Beer"))
  }

  @Test def removalOfCommonUnigrams() {
    // There is a difference between stop words and common unigrams (new file that has been setup)
    // The following should match school and should not match time
    val text = "I am going to order some food"
    val results = PhraseExtractorInterop.getLegacyExtractor.extract(text, 1, 7, ExtractionLevel.PHRASES)
    assertFalse(results.contains("order"))
    //assertFalse(results.contains("time"))

    val text2 = "I am looking for an opponent"
    val results2 = PhraseExtractorInterop.getLegacyExtractor.extract(text2, 1, 7, ExtractionLevel.PHRASES)
    assertFalse(results2.contains("opponent"))
  }

  @Test def treatmentOfSentenceBeginningAsNotProperNoun() {
    // Nouns at the beginning of a sentence are not necessarily matches
    // This is creating a lot of false positives

    // Should not return time
    val text = "Time to go to work!";
    val results = PhraseExtractorInterop.getLegacyExtractor.extract(text, 1, 7, ExtractionLevel.PHRASES)
    assertFalse(results.contains("Time"))

    // Should not return wow
    val text1 = "Just watched Thing One sing the blues @ school w/her classmates. Wow, she's grown!"
    val results1 = PhraseExtractorInterop.getLegacyExtractor.extract(text1, 1, 7, ExtractionLevel.PHRASES)
    assertFalse(results1.contains("Wow"))
  }

  @Test def ngramBlacklistTest() {
    val text = "New Convo: Check it out - You can now post your Gravity Convos into Facebook and Twitter! http://gravity.com/04tm1"
    val results = PhraseExtractorInterop.getLegacyExtractor.extract(text, 1, 7, ExtractionLevel.NGRAMS)
    assertFalse(results.contains("You"))
  }
}
