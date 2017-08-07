package com.gravity.textutils.analysis; /**
 * User: chris
 * Date: Jul 27, 2010
 * Time: 4:48:34 PM
 */

import org.slf4j.*;
import org.junit.Test;

/**
 * These tests should, in a very targeted way, verify that the phrase extractor successfully extracts the right things from test data.  Every time we decide
 * something should work in a particular way, we'll add to these tests.  These tests are run against every build.
 */
public class AnandPhraseExtractorTest {
  private static Logger logger = LoggerFactory.getLogger(AnandPhraseExtractorTest.class);

  @Test
  public void testNothing() {
    AnandPhraseExtractorV2.Extractor.get().extract("I have been telling you for years about the importance of Vitamin D, &lsquo;the sunshine vitamin&rsquo;.&nbsp;&nbsp;Recently, you finally heard it from every TV channel&rsquo;s news program, &ldquo;Vitamin D reduces the risk of breast, prostate and colon cancer.&nbsp;&nbsp;If you develop breast cancer, your risk of metastasis is markedly reduced if you are not Vitamin D3 deficient.&rdquo; The only way you can know if you are deficient is to measure the level of Vitamin D3 in your blood, something we have done on almost every new patient in the last few years. However, for many of our previous patients, we may have missed testing you. I have been very concerned about Vitamin D deficiency. As most of you know by now, almost all of you had blood levels of Vitamin D3 well below the level of 45ng/ml, a level considered by the greatest authorities to be the optimal level.&nbsp;&nbsp;It isn&rsquo;t just the risk of cancer that Vitamin D reduces; it is also diabetes, heart disease, depression and a whole host of autoimmune diseases - how about a reduction of about 80 percent for most of these diseases, including cancer. We are supposed to get our Vitamin D from the sun, not from our food, but most of us have listened to the advice to stay out of the sun to avoid melanoma and other skin cancers. Expose yourselves to the optimal noontime sun for no more that 10-15 minutes; face, arms and legs, if possible.&nbsp;&nbsp;No sunscreens or sun blockers, as they will inhibit synthesis of&nbsp; &nbsp;&nbsp; &nbsp;Vitamin D. Do this 3 to 5 days a week. Have your blood levels of vitamin D3 (25-hydrozy vitamin D) measured at least once a year. We have arranged, through Quest Labs, to offer you this test for one hundred dollars - less than what you would normally have to pay. If your blood level is below 45ng/ml, you must supplement with oral Vitamin D3.&nbsp;&nbsp;We will calculate the dosage needed to raise your Vitamin D to the optimal level. Don&rsquo;t be alarmed into thinking Vitamin D3 is toxic. When you are exposed to the sun, your body naturally produces 10,000 to 20,000 I.U. in approximately a few minutes to a few hours. You are absolutely safe in taking up to 10,000 I.U. daily as a supplement. Children should also be tested and supplemented as needed. If you are dark skinned, you must take oral supplements and it is imperative that your blood levels be monitored.&nbsp; I can&rsquo;t think of a more cost-effective thing for you to do for yourselves and your loved ones. Call the Center if you would like to discuss this further and schedule your Vitamin D blood level testing, (843)572-1600.&nbsp;\n" +
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
            "\n" +
            "",2,4, ExtractionLevel.PHRASES);

  }

}
