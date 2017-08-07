package com.gravity.utilities.web

import com.gravity.test.utilitiesTesting
import com.gravity.utilities.BaseScalaTest
import org.scalatest.ShouldMatchers

import scala.xml.Elem

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

class HtmlTagFilterTest extends BaseScalaTest with utilitiesTesting {
  def testElem1: Elem = xml.XML.loadString("<em><strong>hello <i>world</i></strong> <a href='#'>hi</a></em>")

  test("empty tag set") {
    val blacklist = new HtmlTagFilter(isWhitelist = false, Set.empty)
    val e1 = testElem1
    blacklist.validateElem(e1) should be('success)

    val whitelist = new HtmlTagFilter(isWhitelist = true, Set.empty)
    whitelist.validateElem(e1) should be('failure)
  }

  test("whitelist") {
    val e1 = testElem1
    val w1 = new HtmlTagFilter(isWhitelist = true, Set("EM", "sTrOnG", "I", "A"))
    w1.validateElem(e1) should be('success)

    val w2 = new HtmlTagFilter(isWhitelist = true, Set("Em", "stRoNg", "A"))
    w2.validateElem(e1) should be('failure)
  }

  test("blacklist") {
    val e1 = testElem1
    val b1 = new HtmlTagFilter(isWhitelist = false, Set("script", "img"))
    b1.validateElem(e1) should be('success)

    val b2 = new HtmlTagFilter(isWhitelist = false, Set("script", "img", "I"))
    b2.validateElem(e1) should be('failure)
  }
}