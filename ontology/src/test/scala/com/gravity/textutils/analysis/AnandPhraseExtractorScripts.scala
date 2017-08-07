package com.gravity.textutils.analysis

import org.junit.Test
import org.apache.commons.lang.time.StopWatch
import com.gravity.utilities.{CSV, Streams}
import com.gravity.utilities.CSV.ValueReader

object cnnTweets extends App {
  AnandPhraseExtractorIT.extractAndLogPhrases("cnntweets.txt")
}

object photography extends App {
  AnandPhraseExtractorIT.extractAndLogPhrases("photography.txt")
}

object sports extends App {
  AnandPhraseExtractorIT.extractAndLogPhrases("sportstweets.txt")
}

object compareUpgradeExtractions extends App {
//  val longtext = Streams.getResource("largetext.txt", getClass)
//  val peANAND = AnandPhraseExtractorV2.Extractor.get()
//  val extractionsANAND = peANAND.compileExtractions(longtext)
//  val extractionsSCALA = PhraseExtractorInterop.compileExtractions(longtext)
//
//  info("Anand extraction count: {0}; Scala extraction count: {1}", extractionsANAND.size(), extractionsSCALA.size())
//
//  val reductionsANAND = AnandPhraseExtractorV2.upgradeExtractions(extractionsANAND)
//  val reductionsSCALA = PhraseExtractorInterop.upgradeExtractions(extractionsSCALA)
//
//  info("Anand reduction count: {0}; Scala reduction count: {1}", reductionsANAND.size(), reductionsSCALA.size())
//
//  val iterANAND = reductionsANAND.iterator()
//  val iterSCALA = reductionsSCALA.iterator()
//
//  while(iterANAND.hasNext) {
//    val phraseANAND = iterANAND.next
//    println(phraseANAND)
//    if (!reductionsSCALA.contains(phraseANAND)) info("Anand phrase: '{" + phraseANAND  + "}' was not present in Scala phrases!")
//  }
//
//  while(iterSCALA.hasNext) {
//    val phraseSCALA = iterSCALA.next
//    if (!reductionsANAND.contains(phraseSCALA)) info("Scala phrase: '{" + phraseSCALA + "}' was not present in Anand phrases!")
//  }
}

object compareAnandToScalaForLongText extends App {
 import com.gravity.logging.Logging._
  val longtext = Streams.getResource("largetext.txt", getClass)
  val peANAND = AnandPhraseExtractorV2.Extractor.get()
  val peSCALA = PhraseExtractorInterop.getInstance
  peANAND.extract("How is this for some text?", 2, 3, ExtractionLevel.PHRASES)
  peSCALA.extractLegacy("How is this for some text?", 2, 3, ExtractionLevel.PHRASES)

  val sw = new StopWatch()
  info("Now testing AnandPhraseExtractorV2.java")
  sw.start()
  val resultsANAND = peANAND.extract(longtext, 1, 6, ExtractionLevel.PHRASES)
  sw.stop()
  info("AnandPhraseExtractorV2.java :: Completed longtext extraction in: {0}. Total results = {1}.", sw.getTime, resultsANAND.size)
  sw.reset()

  info("Now testing PhraseExtractorUtil.scala")
  sw.start()
  val resultsSCALA = peSCALA.extractLegacy(longtext, 1, 6, ExtractionLevel.PHRASES)
  sw.stop()

  info("PhraseExtractorUtil.scala :: Completed longtext extraction in: {0}. Total results = {1}.", sw.getTime, resultsSCALA.size)

  if(resultsANAND.size() != resultsSCALA.size()) {
    warn("Results were not the same! Now printing out differences")

    var moreResultsThanExpected = resultsSCALA
    var lessResultsThanExpected = resultsANAND

    if (resultsSCALA.size() > resultsANAND.size()) {
      info("PhraseExtractorUtil.scala had {0} more results than AnandPhraseExtractorV2.java.\nHere are the extra:", resultsSCALA.size() - resultsANAND.size())
      moreResultsThanExpected = resultsSCALA
      lessResultsThanExpected = resultsANAND
    } else {
      info("AnandPhraseExtractorV2.java had {0} more results than PhraseExtractorUtil.scala.\nHere are the extra:", resultsANAND.size() - resultsSCALA.size())
      moreResultsThanExpected = resultsANAND
      lessResultsThanExpected = resultsSCALA
    }

    var i = 1
    val iter = moreResultsThanExpected.iterator()
    while(iter.hasNext) {
      val result = iter.next()
      if (!lessResultsThanExpected.contains(result)) {
        i += 1
        printf("#%d: <phrase>%s</phrase>%n", i, result)
      }
    }
  }
}

object robbieTweets extends App {
 import com.gravity.logging.Logging._
  val is = getClass.getResourceAsStream("robbietweets.txt")
  CSV.read(is, "<grv_delim>", new ValueReader() {
    def item(index: Int, item: String) {
      if (index == 1) {
        val results = AnandPhraseExtractorV2.Extractor.get().extract(item, 2, 4, ExtractionLevel.PHRASES)
        val itr = results.iterator()
        while(itr.hasNext)
          info("Got" + itr.next() + " from original " + item)
      }
    }
  })
}


object AnandPhraseExtractorIT {
 import com.gravity.logging.Logging._
  def extractAndLogPhrases(resourceName : String) {
    val resource = Streams.getResource(resourceName, this.getClass)
    val extracted = AnandPhraseExtractorV2.Extractor.get().extract(resource, 2, 4, ExtractionLevel.PHRASES)
    val arr = new Array[String](extracted.size())
    extracted.toArray[String](arr)
    arr.foreach(info(_))
  }
}
