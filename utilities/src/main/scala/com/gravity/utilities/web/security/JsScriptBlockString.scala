package com.gravity.utilities.web.security

import org.apache.commons.lang.StringEscapeUtils


case class JsScriptBlockString(str: String) {
  def isEmpty: Boolean = str.isEmpty

  override def toString: String = "'" + StringEscapeUtils.escapeJavaScript(str) + "'"
}