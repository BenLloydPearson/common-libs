package com.gravity.utilities

/**Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 5/25/12
 * Time: 3:08 PM
 */

import grvxml._
import org.junit.Assert._
import org.junit.Test

import scala.xml.Elem
import scalaz.std.option._
import scalaz.syntax.apply._

class GrvXmlTest {

  val xml: Elem = {
    <root xmlns:foo="http://foo.com/foo">
      <inner_one attr1="1">nice text</inner_one>
      <inner_one attr1="1">more text</inner_one>
      <inner_one attr1="0">
        <inner_two attr2="2">deep text</inner_two>
      </inner_one>
      <foo:bar attr3="3">namespaced element text</foo:bar>
    </root>
  }

  @Test def testFilterAttribute() {
    val innerText = xml \ "inner_one" \@\("attr1", _ == "1")
    innerText.lift(0) tuple innerText.lift(1) match {
      case Some((t1, t2)) => {
        assertEquals("nice text", t1.text)
        assertEquals("more text", t2.text)
      }
      case None => fail("Failed to get both attribute elements!")
    }
  }

  @Test def testGetAttributeText() {
    (xml \\ "inner_two").headOption match {
      case Some(n) => n.getAttributeText("attr2") match {
        case Some(t) => assertEquals("2", t)
        case None => fail("Failed to find attribute: attr2")
      }
      case None => fail("Failed to find element: inner_two")
    }
  }

  @Test def testGetElementText() {
    (xml \\ "inner_one").getElementText("inner_two") match {
      case Some(t) => assertEquals("deep text", t)
      case None => fail("Failed to find element: inner_two")
    }
  }

  @Test def testGetAttributeTextFromElement() {
    (xml \\ "inner_one").getAttributeTextFromElement("inner_two", "attr2") match {
      case Some(t) => assertEquals("2", t)
      case None => fail("Failed to get attribute: attr2 from element: inner_two")
    }
  }

  @Test def testPrefixedElements() {
    xml \:\ "foo:bar" getText match {
      case Some(t) => assertEquals("namespaced element text", t)
      case None => fail("Failed to get prefixed element text!")
    }
  }

  @Test def testGetPrefixedElementText() {
    xml getElementText ("foo:bar") match {
      case Some(t) => assertEquals("namespaced element text", t)
      case None => fail("Failed to get prefixed element text!")
    }
  }
}