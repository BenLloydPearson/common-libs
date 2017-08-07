package com.gravity.textutils.analysis

/**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: 4/13/11
 * Time: 10:56 PM
 */

import org.junit.Assert._
import java.util.concurrent.CountDownLatch
import org.junit.{Ignore, Test}

class PhraseExtractorUtilTest {
  val printTests = false
  def printt(): Unit = {if (printTests) println()}
  def printt(msg: Any) {if(printTests) println(msg)}
  
  @Ignore
  @Test def testMultithreadedAccess() {
    val numThreads = 4

    val cdl = new CountDownLatch(numThreads)
    val cdl2 = new CountDownLatch(numThreads) //This one means the threads won't start until they're all ready.

    val threads = for (i <- 0 until numThreads) yield {
      new Thread(new Runnable() {
        override def run() {
          cdl2.countDown()
          cdl2.await()
          try {
            for (x <- 0 until 100) {
              PhraseExtractorUtil.extractPhrases(paragraphWithHtmlEntities, 1, 5)
            }
          } finally {
            cdl.countDown()
          }
        }
      }, "TestThread-" + i)
    }

    threads.foreach(_.start())


    cdl.await()

    printt("All done!")
  }

  val paragraphWithHtmlEntities = "I have been telling you for years about the importance of Vitamin D, &lsquo;the sunshine vitamin&rsquo;.&nbsp;&nbsp;Recently, you finally heard it from every TV channel&rsquo;s news program, &ldquo;Vitamin D reduces the risk of breast, prostate and colon cancer.&nbsp;&nbsp;If you develop breast cancer, your risk of metastasis is markedly reduced if you are not Vitamin D3 deficient.&rdquo; The only way you can know if you are deficient is to measure the level of Vitamin D3 in your blood, something we have done on almost every new patient in the last few years. However, for many of our previous patients, we may have missed testing you. I have been very concerned about Vitamin D deficiency. As most of you know by now, almost all of you had blood levels of Vitamin D3 well below the level of 45ng/ml, a level considered by the greatest authorities to be the optimal level.&nbsp;&nbsp;It isn&rsquo;t just the risk of cancer that Vitamin D reduces; it is also diabetes, heart disease, depression and a whole host of autoimmune diseases - how about a reduction of about 80 percent for most of these diseases, including cancer. We are supposed to get our Vitamin D from the sun, not from our food, but most of us have listened to the advice to stay out of the sun to avoid melanoma and other skin cancers. Expose yourselves to the optimal noontime sun for no more that 10-15 minutes; face, arms and legs, if possible.&nbsp;&nbsp;No sunscreens or sun blockers, as they will inhibit synthesis of&nbsp; &nbsp;&nbsp; &nbsp;Vitamin D. Do this 3 to 5 days a week. Have your blood levels of vitamin D3 (25-hydrozy vitamin D) measured at least once a year. We have arranged, through Quest Labs, to offer you this test for one hundred dollars - less than what you would normally have to pay. If your blood level is below 45ng/ml, you must supplement with oral Vitamin D3.&nbsp;&nbsp;We will calculate the dosage needed to raise your Vitamin D to the optimal level. Don&rsquo;t be alarmed into thinking Vitamin D3 is toxic. When you are exposed to the sun, your body naturally produces 10,000 to 20,000 I.U. in approximately a few minutes to a few hours. You are absolutely safe in taking up to 10,000 I.U. daily as a supplement. Children should also be tested and supplemented as needed. If you are dark skinned, you must take oral supplements and it is imperative that your blood levels be monitored.&nbsp; I can&rsquo;t think of a more cost-effective thing for you to do for yourselves and your loved ones. Call the Center if you would like to discuss this further and schedule your Vitamin D blood level testing, (843)572-1600.&nbsp;\n" +
          "\n" +
          "I have been telling you for years about the importance of Vitamin D, &lsquo;the sunshine vitamin&rsquo;.&nbsp;&nbsp;Recently, you finally heard it from every TV channel&rsquo;s news program, &ldquo;Vitamin D reduces the risk of breast, prostate and colon cancer.&nbsp;&nbsp;If you develop breast cancer, your risk of metastasis is markedly reduced if you are not Vitamin D3 deficient.&rdquo;\n" +
          "\n" +
          "The only way you can know if you are deficient is to measure the level of Vitamin D3 in your blood, something we have done on almost every new patient in the last few years. However, for many of our previous patients, we may have missed testing you.\n" +
          "\n" +
          "I have been very concerned about Vitamin D deficiency. As most of you know by now, almost all of you had blood levels of Vitamin D3 well below the level of 45ng/ml, a level considered by the greatest authorities to be the optimal level.&nbsp;&nbsp;It isn&rsquo;t just the risk of cancer that Vitamin D reduces; it is also diabetes, heart disease, depression and a whole host of autoimmune diseases - how about a reduction of about 80 percent for most of these diseases, including cancer.\n" +
          "\n" +
          "We are supposed to get our Vitamin D from the sun, not from our food, but most of us have listened to the advice to stay out of the sun to avoid melanoma and other skin cancers.\n" +
          "\n" +
          "I can&rsquo;t think of a more cost-effective thing for you to do for yourselves and your loved ones.\n" +
          "\n" +
          "Call the Center if you would like to discuss this further and schedule your Vitamin D blood level testing, (843)572-1600.&nbsp;\n" +
          "\n"


  val drWhoText = "I went to the Led Zeppelin concert and realized I was in a time warp because they no longer play, seeing as they aren't super young anym re. But I did watch Dr. Who and enjoyed Tom Baker and he could travel back in time.... Baker is the best Doctor"

  @Test def testForDrWho() {
    val res = PhraseExtractorUtil.extractPhrases(drWhoText, 1,5)
    
    res.foreach(printt)
  }

  @Test def testRichSplit() {
    val spaceMatcher = TextSplitter("\\s+".r)
    val input = "The quick brown fox jumpped over the lazy dog.\n\tWhat a nice sentence that was."
    val results = spaceMatcher.splitAndCaptureIndicies(input)

    results.captures foreach printt
  }

  @Test def testJim() {
    PhraseExtractorUtil.extractNgrams("anarchism a", 1, 3) foreach printt
  }

  @Test def testNothing() {
    PhraseExtractorInterop.getInstance.extractLegacy(paragraphWithHtmlEntities, 2, 4, ExtractionLevel.PHRASES)
  }

  @Test def testSentenceStuff() {
    val results = PhraseExtractorUtil.extractSentences(paragraphWithHtmlEntities)
    results foreach printt
  }

  @Test def testExtractPhrases() {
    printt("CORPUS:")
    printt(paragraphWithHtmlEntities)

    val results = PhraseExtractorUtil.extractPhrases(paragraphWithHtmlEntities, 1, 5)
    printt("RESULTS:")
    results foreach printt
  }

  @Test def extractNgramsLegacy() {
    val text = "I once read that pipers pecked pickled peppers, but Long Cat reigns supreme over the John Birch Society!  I also heard that Dick Cheney watches Sex and the City"
    val pe = PhraseExtractorInterop.getInstance

    val matches = pe.extractLegacy(text, 3, 7, ExtractionLevel.NGRAMS)

    assertTrue(matches.contains("John Birch Society"))
    assertTrue(matches.contains("pecked pickled peppers"))
    assertTrue(matches.contains("pipers pecked pickled peppers"))
    assertTrue(matches.contains("I once read that pipers pecked pickled"))
    assertFalse(matches.contains("I once read that pipers pecked pickled peppers")) // out of bounds check
  }

  @Test def extractNgrams() {
    val text = "I once read that pipers pecked pickled peppers, but Long Cat reigns supreme over the John Birch Society!  I also heard that Dick Cheney watches Sex and the City"

    val matches = PhraseExtractorUtil.extractNgrams(text, 3, 7)

    var expected = "John Birch Society"
    assertTrue("Could not find: " + expected, matches.contains(expected))

    expected = "pecked pickled peppers"
    assertTrue("Could not find: " + expected, matches.contains(expected))

    expected = "pipers pecked pickled peppers"
    assertTrue("Could not find: " + expected, matches.contains(expected))

    expected = "I once read that pipers pecked pickled"
    assertTrue("Could not find: " + expected, matches.contains(expected))

    expected = "I once read that pipers pecked pickled peppers"
    assertFalse("Too many grams! Max window was 7 and the following is 8: " + expected, matches.contains(expected)) // out of bounds check
  }

  @Test def extractionPipeline() {
    val text = "I once read that pipers pecked pickled peppers, but Long Cat reigns supreme over the John Birch Society!  I also heard that Dick Cheney watches Sex and the City"

    // First let's extract the huge grams with no matching. In this example we should get back all the 3-7 grams
    val matches = PhraseExtractorUtil.extractNgrams(text, 3, 7)
    assertTrue(matches.contains("John Birch Society"))
    assertTrue(matches.contains("pecked pickled peppers"))
    assertTrue(matches.contains("pipers pecked pickled peppers"))

    // Let's pretend we took the results of the above and found "John Birch Society" in the ontology.  We'll now pass that
    // back to the extractor so it knows not to match the sub-textutils in that larger phrase.
    val matchedGrams = Set("John Birch Society")

    // Now that we've found the large grams, let's go back to the extractor and do the smaller matches will all the rules turned on.
    val secondLevelMatches = PhraseExtractorUtil.extractPhrases(text, 3, 7, matchedGrams)

    secondLevelMatches.foreach(pi => printt("Phrase: '%s'; Start: %d; End: %d".format(pi.phrase, pi.startIndex, pi.endIndex)))

    assertTrue(secondLevelMatches.contains("Long Cat reigns supreme over the John"))
    assertFalse(secondLevelMatches.contains("John Birch Society"))
    assertFalse(secondLevelMatches.contains("John Birch"))
    assertFalse(secondLevelMatches.contains("Birch"))
    assertFalse(secondLevelMatches.contains("Society"))
  }

}
