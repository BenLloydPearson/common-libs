package com.gravity.utilities.web.extraction

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/28/14
 * Time: 3:15 PM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 * \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */

import org.junit.Assert._
import org.junit.Test

object testIsHuffPoLiveHighlightUrl extends App {
  val cases: Seq[(String, Boolean)] = Seq(
    "http://live.huffingtonpost.com/r/segment/elizabeth-mitchell-libertys-torch-book/5399f89278c90a20890002e1" -> false
    ,"http://live.huffingtonpost.com/r/segment/5270220cfe344421cd000901" -> false
    ,"http://live.huffingtonpost.com/r/archive/segment/53b2df66fe34446be00000eb" -> true
    ,"http://live.huffingtonpost.com/r/highlight/is-this-new-app-lesbians-answer-to-grindr/53b45def78c90adc28000304" -> true
    ,"http://live.huffingtonpost.com/r/highlight/brad-goreski-explains-how-to-wear-red-white-and-blue-with-style-for-july-4/53b2df66fe34446be00000eb" -> true
    ,"http://live.huffingtonpost.com/r/archive/segment/53dbd22778c90ae0c2000511" -> true
    ,"http://live.huffingtonpost.com/r/archive/segment/53ea3ed378c90ac7d40000d2" -> true
  )

  val shouldBeHighlightMsg = "SHOULD be identified as a Highlight and wasn't!"
  val shouldNotBeHighlightMsg = "should NOT be identified as a Highlight and WAS!"

  cases.foreach {
    case (url: String, shouldBeHighlight: Boolean) =>
      val msg = if(shouldBeHighlight) shouldBeHighlightMsg else shouldNotBeHighlightMsg
      assertEquals(s"URL `$url` $msg", shouldBeHighlight, HuffPoLiveArticleFetcher.isHuffPoLiveHighlightUrl(url))
  }
}