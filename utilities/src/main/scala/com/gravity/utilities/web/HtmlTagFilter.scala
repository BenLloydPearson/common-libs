package com.gravity.utilities.web

import scala.collection.Set
import scala.xml.Elem
import scalaz.ValidationNel
import scalaz.syntax.validation._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/


/** @param isWhitelist FALSE for blacklist. */
class HtmlTagFilter(val isWhitelist: Boolean, _tags: Set[String]) {
  val tags: Set[String] = _tags.map(_.toLowerCase)

  /** @return The original elem on success or a String error on failure. */
  def validateElem(e: Elem, validateRootNode: Boolean = true): ValidationNel[String, Elem] =
    if(isWhitelist) validateElemWhitelist(e, validateRootNode) else validateElemBlacklist(e, validateRootNode)

  protected def validateElemWhitelist(e: Elem, validateRootNode: Boolean = true): ValidationNel[String, Elem] = {
    val nodes = if(validateRootNode) e.descendant_or_self else e.descendant
    nodes.find(
      node => !node.label.startsWith("#") && !tags.contains(node.label.toLowerCase)
    ).fold[ValidationNel[String, Elem]](e.successNel) {
      case nodeNotOnWhitelist =>
        s"Input contains a `${nodeNotOnWhitelist.label.toLowerCase}` tag, which is not on the whitelist.".failureNel
    }
  }

  protected def validateElemBlacklist(e: Elem, validateRootNode: Boolean = true): ValidationNel[String, Elem] = {
    val nodes = if(validateRootNode) e.descendant_or_self else e.descendant
    nodes.find(node => tags.contains(node.label.toLowerCase)).fold[ValidationNel[String, Elem]](e.successNel) {
      case blacklistedNode =>
        s"Input contains a `${blacklistedNode.label.toLowerCase}` tag, which is blacklisted.".failureNel
    }
  }
}

object HtmlTagFilter {
  val blacklistScriptTags: HtmlTagFilter = new HtmlTagFilter(isWhitelist = false, Set("script"))
}