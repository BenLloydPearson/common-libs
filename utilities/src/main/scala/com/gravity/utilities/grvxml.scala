package com.gravity.utilities

import scala.xml._

/** Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 12/19/11
 * Time: 5:34 PM
 */

object grvxml {

  implicit def richNode(n: Node): RichNode = new RichNode(n)

  implicit def richNodeSeq(ns: NodeSeq): RichNodeSeq = new RichNodeSeq(ns)

  private[grvxml] def &(a: String): String = "@" + a

  private[grvxml] val namedEntityToHexEntity = Map(
    "&quot;" -> "&#x22;",
    "&amp;" -> "&#x26;",
    "&apos;" -> "&#x27;",
    "&lt;" -> "&#x3c;",
    "&gt;" -> "&#x3e;",
    "&nbsp;" -> "&#xa0;",
    "&iexcl;" -> "&#xa1;",
    "&cent;" -> "&#xa2;",
    "&pound;" -> "&#xa3;",
    "&curren;" -> "&#xa4;",
    "&yen;" -> "&#xa5;",
    "&brvbar;" -> "&#xa6;",
    "&sect;" -> "&#xa7;",
    "&uml;" -> "&#xa8;",
    "&copy;" -> "&#xa9;",
    "&ordf;" -> "&#xaa;",
    "&laquo;" -> "&#xab;",
    "&not;" -> "&#xac;",
    "&shy;" -> "&#xad;",
    "&reg;" -> "&#xae;",
    "&macr;" -> "&#xaf;",
    "&deg;" -> "&#xb0;",
    "&plusmn;" -> "&#xb1;",
    "&sup2;" -> "&#xb2;",
    "&sup3;" -> "&#xb3;",
    "&acute;" -> "&#xb4;",
    "&micro;" -> "&#xb5;",
    "&para;" -> "&#xb6;",
    "&middot;" -> "&#xb7;",
    "&cedil;" -> "&#xb8;",
    "&sup1;" -> "&#xb9;",
    "&ordm;" -> "&#xba;",
    "&raquo;" -> "&#xbb;",
    "&frac14;" -> "&#xbc;",
    "&frac12;" -> "&#xbd;",
    "&frac34;" -> "&#xbe;",
    "&iquest;" -> "&#xbf;",
    "&agrave;" -> "&#xc0;",
    "&aacute;" -> "&#xc1;",
    "&acirc;" -> "&#xc2;",
    "&atilde;" -> "&#xc3;",
    "&auml;" -> "&#xc4;",
    "&aring;" -> "&#xc5;",
    "&aelig;" -> "&#xc6;",
    "&ccedil;" -> "&#xc7;",
    "&egrave;" -> "&#xc8;",
    "&eacute;" -> "&#xc9;",
    "&ecirc;" -> "&#xca;",
    "&euml;" -> "&#xcb;",
    "&igrave;" -> "&#xcc;",
    "&iacute;" -> "&#xcd;",
    "&icirc;" -> "&#xce;",
    "&iuml;" -> "&#xcf;",
    "&eth;" -> "&#xd0;",
    "&ntilde;" -> "&#xd1;",
    "&ograve;" -> "&#xd2;",
    "&oacute;" -> "&#xd3;",
    "&ocirc;" -> "&#xd4;",
    "&otilde;" -> "&#xd5;",
    "&ouml;" -> "&#xd6;",
    "&times;" -> "&#xd7;",
    "&oslash;" -> "&#xd8;",
    "&ugrave;" -> "&#xd9;",
    "&uacute;" -> "&#xda;",
    "&ucirc;" -> "&#xdb;",
    "&uuml;" -> "&#xdc;",
    "&yacute;" -> "&#xdd;",
    "&thorn;" -> "&#xde;",
    "&szlig;" -> "&#xdf;",
    "&agrave;" -> "&#xe0;",
    "&aacute;" -> "&#xe1;",
    "&acirc;" -> "&#xe2;",
    "&atilde;" -> "&#xe3;",
    "&auml;" -> "&#xe4;",
    "&aring;" -> "&#xe5;",
    "&aelig;" -> "&#xe6;",
    "&ccedil;" -> "&#xe7;",
    "&egrave;" -> "&#xe8;",
    "&eacute;" -> "&#xe9;",
    "&ecirc;" -> "&#xea;",
    "&euml;" -> "&#xeb;",
    "&igrave;" -> "&#xec;",
    "&iacute;" -> "&#xed;",
    "&icirc;" -> "&#xee;",
    "&iuml;" -> "&#xef;",
    "&eth;" -> "&#xf0;",
    "&ntilde;" -> "&#xf1;",
    "&ograve;" -> "&#xf2;",
    "&oacute;" -> "&#xf3;",
    "&ocirc;" -> "&#xf4;",
    "&otilde;" -> "&#xf5;",
    "&ouml;" -> "&#xf6;",
    "&divide;" -> "&#xf7;",
    "&oslash;" -> "&#xf8;",
    "&ugrave;" -> "&#xf9;",
    "&uacute;" -> "&#xfa;",
    "&ucirc;" -> "&#xfb;",
    "&uuml;" -> "&#xfc;",
    "&yacute;" -> "&#xfd;",
    "&thorn;" -> "&#xfe;",
    "&yuml;" -> "&#xff;",
    "&oelig;" -> "&#x152;",
    "&oelig;" -> "&#x153;",
    "&scaron;" -> "&#x160;",
    "&scaron;" -> "&#x161;",
    "&yuml;" -> "&#x178;",
    "&fnof;" -> "&#x192;",
    "&circ;" -> "&#x2c6;",
    "&tilde;" -> "&#x2dc;",
    "&alpha;" -> "&#x391;",
    "&beta;" -> "&#x392;",
    "&gamma;" -> "&#x393;",
    "&delta;" -> "&#x394;",
    "&epsilon;" -> "&#x395;",
    "&zeta;" -> "&#x396;",
    "&eta;" -> "&#x397;",
    "&theta;" -> "&#x398;",
    "&iota;" -> "&#x399;",
    "&kappa;" -> "&#x39a;",
    "&lambda;" -> "&#x39b;",
    "&mu;" -> "&#x39c;",
    "&nu;" -> "&#x39d;",
    "&xi;" -> "&#x39e;",
    "&omicron;" -> "&#x39f;",
    "&pi;" -> "&#x3a0;",
    "&rho;" -> "&#x3a1;",
    "&sigma;" -> "&#x3a3;",
    "&tau;" -> "&#x3a4;",
    "&upsilon;" -> "&#x3a5;",
    "&phi;" -> "&#x3a6;",
    "&chi;" -> "&#x3a7;",
    "&psi;" -> "&#x3a8;",
    "&omega;" -> "&#x3a9;",
    "&alpha;" -> "&#x3b1;",
    "&beta;" -> "&#x3b2;",
    "&gamma;" -> "&#x3b3;",
    "&delta;" -> "&#x3b4;",
    "&epsilon;" -> "&#x3b5;",
    "&zeta;" -> "&#x3b6;",
    "&eta;" -> "&#x3b7;",
    "&theta;" -> "&#x3b8;",
    "&iota;" -> "&#x3b9;",
    "&kappa;" -> "&#x3ba;",
    "&lambda;" -> "&#x3bb;",
    "&mu;" -> "&#x3bc;",
    "&nu;" -> "&#x3bd;",
    "&xi;" -> "&#x3be;",
    "&omicron;" -> "&#x3bf;",
    "&pi;" -> "&#x3c0;",
    "&rho;" -> "&#x3c1;",
    "&sigmaf;" -> "&#x3c2;",
    "&sigma;" -> "&#x3c3;",
    "&tau;" -> "&#x3c4;",
    "&upsilon;" -> "&#x3c5;",
    "&phi;" -> "&#x3c6;",
    "&chi;" -> "&#x3c7;",
    "&psi;" -> "&#x3c8;",
    "&omega;" -> "&#x3c9;",
    "&thetasym;" -> "&#x3d1;",
    "&upsih;" -> "&#x3d2;",
    "&piv;" -> "&#x3d6;",
    "&ensp;" -> "&#x2002;",
    "&emsp;" -> "&#x2003;",
    "&thinsp;" -> "&#x2009;",
    "&zwnj;" -> "&#x200c;",
    "&zwj;" -> "&#x200d;",
    "&lrm;" -> "&#x200e;",
    "&rlm;" -> "&#x200f;",
    "&ndash;" -> "&#x2013;",
    "&mdash;" -> "&#x2014;",
    "&lsquo;" -> "&#x2018;",
    "&rsquo;" -> "&#x2019;",
    "&sbquo;" -> "&#x201a;",
    "&ldquo;" -> "&#x201c;",
    "&rdquo;" -> "&#x201d;",
    "&bdquo;" -> "&#x201e;",
    "&dagger;" -> "&#x2020;",
    "&dagger;" -> "&#x2021;",
    "&bull;" -> "&#x2022;",
    "&hellip;" -> "&#x2026;",
    "&permil;" -> "&#x2030;",
    "&prime;" -> "&#x2032;",
    "&prime;" -> "&#x2033;",
    "&lsaquo;" -> "&#x2039;",
    "&rsaquo;" -> "&#x203a;",
    "&oline;" -> "&#x203e;",
    "&frasl;" -> "&#x2044;",
    "&euro;" -> "&#x20ac;",
    "&image;" -> "&#x2111;",
    "&weierp;" -> "&#x2118;",
    "&real;" -> "&#x211c;",
    "&trade;" -> "&#x2122;",
    "&alefsym;" -> "&#x2135;",
    "&larr;" -> "&#x2190;",
    "&uarr;" -> "&#x2191;",
    "&rarr;" -> "&#x2192;",
    "&darr;" -> "&#x2193;",
    "&harr;" -> "&#x2194;",
    "&crarr;" -> "&#x21b5;",
    "&larr;" -> "&#x21d0;",
    "&uarr;" -> "&#x21d1;",
    "&rarr;" -> "&#x21d2;",
    "&darr;" -> "&#x21d3;",
    "&harr;" -> "&#x21d4;",
    "&forall;" -> "&#x2200;",
    "&part;" -> "&#x2202;",
    "&exist;" -> "&#x2203;",
    "&empty;" -> "&#x2205;",
    "&nabla;" -> "&#x2207;",
    "&isin;" -> "&#x2208;",
    "&notin;" -> "&#x2209;",
    "&ni;" -> "&#x220b;",
    "&prod;" -> "&#x220f;",
    "&sum;" -> "&#x2211;",
    "&minus;" -> "&#x2212;",
    "&lowast;" -> "&#x2217;",
    "&radic;" -> "&#x221a;",
    "&prop;" -> "&#x221d;",
    "&infin;" -> "&#x221e;",
    "&ang;" -> "&#x2220;",
    "&and;" -> "&#x2227;",
    "&or;" -> "&#x2228;",
    "&cap;" -> "&#x2229;",
    "&cup;" -> "&#x222a;",
    "&int;" -> "&#x222b;",
    "&there4;" -> "&#x2234;",
    "&sim;" -> "&#x223c;",
    "&cong;" -> "&#x2245;",
    "&asymp;" -> "&#x2248;",
    "&ne;" -> "&#x2260;",
    "&equiv;" -> "&#x2261;",
    "&le;" -> "&#x2264;",
    "&ge;" -> "&#x2265;",
    "&sub;" -> "&#x2282;",
    "&sup;" -> "&#x2283;",
    "&nsub;" -> "&#x2284;",
    "&sube;" -> "&#x2286;",
    "&supe;" -> "&#x2287;",
    "&oplus;" -> "&#x2295;",
    "&otimes;" -> "&#x2297;",
    "&perp;" -> "&#x22a5;",
    "&sdot;" -> "&#x22c5;",
    "&vellip;" -> "&#x22ee;",
    "&lceil;" -> "&#x2308;",
    "&rceil;" -> "&#x2309;",
    "&lfloor;" -> "&#x230a;",
    "&rfloor;" -> "&#x230b;",
    "&lang;" -> "&#x2329;",
    "&rang;" -> "&#x232a;",
    "&loz;" -> "&#x25ca;",
    "&spades;" -> "&#x2660;",
    "&clubs;" -> "&#x2663;",
    "&hearts;" -> "&#x2665;",
    "&diams;" -> "&#x2666;"
  )

  private[grvxml] val entityRegex = """&\w+;""".r

  def forceHexEntities(input: String): String = entityRegex.replaceAllIn(input, m => {
    val matched = m.group(0)
    namedEntityToHexEntity.get(matched) match {
      case Some(replaceWithMe) => replaceWithMe
      case None => matched
    }
  })

  // see http://www.w3.org/TR/2006/REC-xml-20060816/#charsets
  def isCharacterAllowed(input: Int): Boolean = input match {
    case 9 => true
    case 10 => true
    case 13 => true
    case range1 if range1 > 31 && range1 < 55296 => true
    case range2 if range2 > 57343 && range2 < 65534 => true
    case range3 if range3 > 65535 && range3 < 1114110 => true
    case _ => false
  }

  class RichNode(n: Node) {

    /**
     * Looks for the attribute value of the `attrName` within this [[scala.xml.Node]]
   *
     * @param attrName the attribute name to look for
     * @return if an attribute exists by the name `attrName` and it has a non-empty value, then [[scala.Some]]`[`[[java.lang.String]]`]` otherwise `None`
     */
    def getAttributeText(attrName: String): Option[String] = {
      (for {
        a <- n \ &(attrName)
        t = a.text.trim
        if (!t.isEmpty)
      } yield t).headOption
    }

    /**
     * Looks for a child [[scala.xml.Node]] named `elemName` and returns the trimmed text it contains
   *
     * @param elemName the name of the child element ([[scala.xml.Node]]) that contains the text desired
     * @param elemIndex if greater than -1, it will only return the text from the element of this index and None if no element exists at this index
     * @return if a child element exists by the name `elemName` and it contains non-empty text, then [[scala.Some]]`[`[[java.lang.String]]`]` otherwise `None`
     */
    def getElementText(elemName: String, elemIndex: Int = -1, separator: String = ""): Option[String] = {
      val x = n \:\ elemName
      val t = if (elemIndex < 0) {
        x.map(_.text.trim).filter(_.nonEmpty).mkString(separator)
      } else {
        x.lift(elemIndex) match {
          case Some(e) => e.text.trim
          case None => return None
        }
      }
      if (t.isEmpty) None else Some(t)
    }

    def getElementTexts(elemName: String): Iterable[String] =
      (n \:\ elemName).map(_.text.trim).filter(_.nonEmpty)

    /**
     * Looks for the attribute value of the `attrName` within the child [[scala.xml.Node]] named `elemName` of this [[scala.xml.Node]]
   *
     * @param elemName the name of the child element ([[scala.xml.Node]]) that contains the attribute named `attrName`
     * @param attrName the attribute name to look for
     * @return if a child element exists by the name `elemName` and it contains an attribute by the name `attrName` and it has a non-empty value, then [[scala.Some]]`[`[[java.lang.String]]`]` otherwise `None`
     */
    def getAttributeTextFromElement(elemName: String, attrName: String): Option[String] = getAttributeTextsFromElement(elemName, attrName).headOption

    /**
     * Looks for the attribute value of the `attrName` within the child [[scala.xml.Node]] named `elemName` of this [[scala.xml.Node]]
   *
     * @param elemName the name of the child element ([[scala.xml.Node]]) that contains the attribute named `attrName`
     * @param attrName the attribute name to look for
     * @return the [[scala.Seq]]`[`[[java.lang.String]] of child attribute values on elements by the name `elemName` that contain an attribute by the name `attrName` with a non-empty value
     */
    def getAttributeTextsFromElement(elemName: String, attrName: String): Seq[String] = {
      for {
        e <- n \:\ elemName
        a <- e \ &(attrName)
        t = a.text.trim
        if (!t.isEmpty)
      } yield t
    }

    /**
     * Similar to [[com.gravity.utilities.grvxml.RichNode#getAttributeText]] returns an empty [[java.lang.String]] if `None`
   *
     * @param attrib the attribute name to look for
     * @return the actual [[java.lang.String]] value of the attribute if it exists, otherwise an empty [[java.lang.String]]
     */
    def :@(attrib: String): String = getAttributeText(attrib).getOrElse("")

    /**
     * Gets the trimmed text contained within this [[scala.xml.Node]]
   *
     * @return if the text of this [[scala.xml.Node]] is non-empty, then [[scala.Some]]`[`[[java.lang.String]]`]` otherwise `None`
     */
    def getText: Option[String] = {
      val t = n.text.trim
      if (t.isEmpty) None else Some(t)
    }
  }

  class RichNodeSeq(ns: NodeSeq) {

    /**
     * Looks for a child [[scala.xml.Node]] named `elemName` and returns the trimmed text it contains.
   *
     * @see [[com.gravity.utilities.grvxml.RichNode#getElementText]]
     * @param elemName the name of the child element ([[scala.xml.Node]]) that contains the text desired
     * @param elementOrdinal Since this is really a [[scala.collection.Seq]] of [[scala.xml.Node]]'s, you must identify which to use. By default the first is selected.
     * @return if a child element exists by the name `elemName` and it contains non-empty text, then [[scala.Some]]`[`[[java.lang.String]]`]` otherwise `None`
     */
    def getElementText(elemName: String, elementOrdinal: Int = 0): Option[String] = {
      for {
        n <- ns \:\ elemName lift elementOrdinal
        t = n.text.trim
        if !t.isEmpty
      } yield t
    }

    /**
     * Looks for the attribute value of the `attrName` within the child [[scala.xml.Node]] named `elemName` of this [[scala.xml.NodeSeq]]
   *
     * @see [[com.gravity.utilities.grvxml.RichNode#getAttributeTextFromElement]]
     * @param elemName the name of the child element ([[scala.xml.Node]]) that contains the attribute named `attrName`
     * @param attrName the attribute name to look for
     * @param elementOrdinal since this is really a [[scala.collection.Seq]] of [[scala.xml.Node]]'s, you must identify which to use. By default the first is selected.
     * @return if a child element exists by the name `elemName` and it contains an attribute by the name `attrName` and it has a non-empty value, then [[scala.Some]]`[`[[java.lang.String]]`]` otherwise `None`
     */
    def getAttributeTextFromElement(elemName: String, attrName: String, elementOrdinal: Int = 0): Option[String] = {
      for {
        e <- ns \:\ elemName lift elementOrdinal
        a <- (e \ &(attrName)).headOption
        t = a.text.trim
        if !t.isEmpty
      } yield t
    }

    /**
     * Extends the built-in `\` projection of [[scala.xml.NodeSeq]] by allowing to also specify the namespace prefix
   *
     * @param prefix the namespace prefix of the desired `Node`s.
     * @param label the element label of the desired `Node`s.
     * @example `ns \: ("uri", "foo")` // will return all child nodes like &lt;uri:foo&gt;
     * @note May NOT be used for attribute searches
     * @return ...
     */
    def \:\(prefix: String, label: String): NodeSeq = {
      if (ScalaMagic.isNullOrEmpty(prefix)) {
        ns \ label
      } else {
        NodeSeq.fromSeq(ns.flatMap(_.child).filter(n => n.prefix == prefix && n.label == label))
      }
    }

    /**
     * Extends the built-in `\` projection of [[scala.xml.NodeSeq]] by allowing to also specify the namespace prefix
   *
     * @param that specifies the match of the desired nodes. To specify prefixed elements, pass in as `"prefix:label"`.
     * @example `ns \:\ "uri:foo"` // will return all child nodes like &lt;uri:foo&gt;
     * @note May NOT be used for attribute searches
     * @return ...
     */
    def \:\(that: String): NodeSeq = {
      that indexOf ":" match {
        case -1 => ns \ that
        case 0 => ns \ that.substring(1)
        case x if x == that.length - 1 => ns \ that.substring(0, x - 1)
        case d => \:\(that.substring(0, d), that.substring(d + 1))
      }
    }

    /**
     * Filters all children down to those that pass the `attribMatch` function
   *
     * @param attribMatch A function that contains the attribute name to use and a function that will be passed each of those attribute values to test
     * @return Only the child [[scala.xml.NodeSeq]] that passed the `attribMatch` check
     */
    def \@\(attribMatch: (String, String => Boolean)): NodeSeq = {
      ns filter {
        _ \\ &(attribMatch._1) exists (s => attribMatch._2(s.text.trim))
      }
    }

    /**
     * Gets the trimmed text contained within this [[scala.xml.NodeSeq]]
   *
     * @see [[com.gravity.utilities.grvxml.RichNode#getText]]
     * @return if the text of this [[scala.xml.NodeSeq]] is non-empty, then [[scala.Some]]`[`[[java.lang.String]]`]` otherwise `None`
     */
    def getText: Option[String] = {
      val t = ns.text.trim
      if (t.isEmpty) None else Some(t)
    }
  }

}