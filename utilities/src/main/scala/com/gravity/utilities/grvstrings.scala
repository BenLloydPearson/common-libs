package com.gravity.utilities

/**
 * Created by runger on 3/12/14.
 */

import java.net._
import java.text.SimpleDateFormat
import java.util.Date

import com.google.common.base.Charsets
import com.google.common.net.InternetDomainName
import com.gravity.utilities.ScalaMagic._
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvevent.HasMessage
import com.gravity.utilities.grvprimitives._
import com.gravity.utilities.grvz._
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import com.gravity.utilities.time.GrvDateMidnight
import com.gravity.utilities.web.MimeTypes
import com.gravity.utilities.web.MimeTypes.MimeType
import com.gravity.valueclasses.ValueClassesForUtilities._
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}
import play.api.libs.functional.syntax._
import play.api.libs.json._
import sun.net.www.protocol.http.Handler

import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.macros.blackbox.Context
import scala.util.matching.Regex
import scala.util.matching.Regex.MatchIterator
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, NonEmptyList, Success, Validation, ValidationNel, Index => _}

object grvstrings {
  import grvtime._

  class NonEmptyString protected(val str: String)
  object NonEmptyString {
    def apply(str: String): Option[NonEmptyString] = {
      if (str == null) return None
      str.trim.some.filter(_.nonEmpty).map(s => new NonEmptyString(s))
    }
  }

  val urlHandler: Handler = new sun.net.www.protocol.http.Handler()

  def encodeUrlParams(params: Seq[(String, String)]): String =
    params.map { case (k, v) => s"$k=${URLUtils.urlEncode(v)}" }.mkString("&")

  def decodeUrlParams(urlString: String): Map[String, String] = {
    urlString.splitBetter("?").lift(1).getOrElse("").splitBetter("&").toSeq.map { keyEqVal =>
      keyEqVal.splitBoringly("=") match {
        case Array(k)    => k -> ""
        case Array(k, v) => k -> URLUtils.urlDecode(v)
      }
    }.toMap
  }

  implicit class GrvString(val orig: String) extends AnyVal {

    def containsIgnoreCase(find: String): Boolean = StringUtils.containsIgnoreCase(orig, find)

    def startsWithIgnoreCase(find: String): Boolean = StringUtils.startsWithIgnoreCase(orig, find)

    def endsWithIgnoreCase(find: String): Boolean = StringUtils.endsWithIgnoreCase(orig, find)

    def elide(posLimit: Int): String = {
      if (orig.length <= posLimit)
        orig
      else if (posLimit <= 0)
        ""
      else
        orig.take(posLimit - 1) + "…"
    }

    def safeSubstring(start: Index): Option[String] = {
      safeSubstring(start, orig.length.asIndex)
    }

    def safeSubstring(start: Index, end: Index): Option[String] = {
      if (start.raw < 0 || end.raw < 0) return None

      val a = for {
        origStr <- Option(orig)
        if end.raw <= orig.length
        if end.raw - start.raw > -1
      } yield origStr.substring(start.raw, end.raw)
      a
    }

    def safeIndexOf(str: String): Option[Index] = {
      orig.indexOf(str) match {
        case index if index < 0 => None
        case index => Option(index.asIndex)
      }
    }

    def safeIndexOf(str: String, ix: Index): Option[Index] = {
      orig.indexOf(str, ix.raw) match {
        case index if index < 0 => None
        case index => Option(index.asIndex)
      }
    }

    def toNonEmptyString: Option[NonEmptyString] = NonEmptyString(orig)

    def escapeForSqlLike: String = {
      val esc = '\\'
      val b = new StringBuilder

      for(char <- orig) {
        if(char == '%' || char == '_') b.append(esc)
        b.append(char)
      }

      b.toString()
    }

    def failureResult: FailureResult = FailureResult(orig)

    def isNumeric: Boolean = {
      if (isNullOrEmpty(orig)) {
        false
      } else {
        import java.text.{NumberFormat, ParsePosition}

        val formatter = NumberFormat.getInstance
        val pos = new ParsePosition(0)
        formatter.parse(orig, pos)
        orig.length == pos.getIndex
      }
    }

    /**
     * Quite dumbly and aggressively strips XML tags. Notably:
     *
     *   - Things that resemble tags are stripped, even if they're not real. ("<foobar>" is stripped; "< foobar>" is not.)
     *   - XML structure is disregarded, meaning hanging tags, invalid nesting, etc. doesn't matter. I.e. a single
     *     hanging "</foobar>" tag will get stripped.
     *
     * @param keepTags Case-insensitive white list of tags to keep in result.
     */
    def stripXmlTags(keepTags: String*): String = keepTags.toList match {
      case Nil => xmlTagReg.replaceAllIn(orig, emptyString)

      // Some tags to keep
      case _ =>
        val lcaseKeepTags = keepTags.map(_.toLowerCase).toSet

        xmlTagReg.replaceSomeIn(orig, matched => {
          val tagName = matched.group("tagName").toLowerCase
          lcaseKeepTags.contains(tagName).asOpt(emptyString)
        })
    }

    def tryToInt: Option[Int] = {
      if (orig == null || orig.isEmpty) return None
      try {
        Some(orig.toInt)
      } catch {
        case _: NumberFormatException => None
      }
    }

    def tryToLong: Option[Long] = {
      if (orig == null || orig.isEmpty) return None
      try {
        Some(orig.toLong)
      } catch {
        case _: NumberFormatException => None
      }
    }

    def tryToShort: Option[Short] = {
      if (orig == null || orig.isEmpty) return None
      try {
        Some(orig.toShort)
      } catch {
        case _: NumberFormatException => None
      }
    }

    def tryToByte: Option[Byte] = {
      if (orig == null || orig.isEmpty) return None
      try {
        Some(orig.toByte)
      } catch {
        case _: NumberFormatException => None
      }
    }

    def tryToDouble: Option[Double] = {
      if (orig == null || orig.isEmpty) return None
      try {
        Some(orig.toDouble)
      } catch {
        case _: NumberFormatException => None
      }
    }

    def tryToFloat: Option[Float] = {
      if (orig == null || orig.isEmpty) return None
      try {
        Some(orig.toFloat)
      } catch {
        case _: NumberFormatException => None
      }
    }

    def tryToBigDecimal: Option[BigDecimal] = {
      if (orig == null || orig.isEmpty) return None
      try {
        Some(BigDecimal(orig))
      } catch {
        case _: Exception => None
      }
    }

    /**
     * This adds a special handling for cases where the original value is a 1 or "on" (for true) or 0 or "off" (for false)
     */
    def tryToBoolean: Option[Boolean] = {
      if (orig == null || orig.isEmpty) return None

      orig.toLowerCase match {
        case "1" => return Some(true)
        case "0" => return Some(false)
        case "on" => return Some(true)
        case "off" => return Some(false)
        case _ =>
      }

      try {
        Some(orig.toBoolean)
      } catch {
        case _: NumberFormatException => None
        case _: IllegalArgumentException => None
      }
    }

    def tryToDateTime(format: String): Option[DateTime] = {
      if (orig == null || orig.isEmpty) return None
      try {
        tryToDateTime(DateTimeFormat.forPattern(format))
      }
      catch {
        case _: Exception => None
      }
    }

    def tryToMimeType: Option[MimeType] = MimeTypes.bySimpleName.get(orig)

    def urlEncode(encoding: String = Charsets.UTF_8.name()): String = {
      if (isNullOrEmpty(orig)) emptyString else URLEncoder.encode(orig, encoding)
    }

    def urlDecode(encoding: String = Charsets.UTF_8.name()): String = {
      if (isNullOrEmpty(orig)) emptyString else URLDecoder.decode(orig, encoding)
    }

    def base64Encode: String = {
      if (isNullOrEmpty(orig)) return emptyString

      encodeBase64(orig)
    }

    def base64Decode: String = {
      if (isNullOrEmpty(orig)) return emptyString

      decodeBase64(orig)
    }

    def validateDateTime(formatter: DateTimeFormatter = ISODateTimeFormat.dateHourMinuteSecond()): ValidationNel[FailureResult, DateTime] = {
      //this will check for null twice if the original is null but that is pretty unlikely.
      val origAsLong = orig.tryToLong
      if(origAsLong.isDefined) {
        tryToSuccessNEL(new DateTime(origAsLong.get), ex => FailureResult("Failed to create date time from long " + orig, ex))
      }
      else {
        for {
          notEmpty <- Option(orig).toValidationNel(FailureResult("Must be a not_null / non-empty string!"))
          dateTime <- tryToSuccessNEL(formatter.parseDateTime(notEmpty), ex => FailureResult("Failed to parse DateTime from string: `" + notEmpty + "`!", ex))
        } yield dateTime
      }
    }

    def tryToDateTime(formatter: DateTimeFormatter = ISODateTimeFormat.dateHourMinuteSecond()): Option[DateTime] = validateDateTime(formatter).toOption

    def tryToDateMidnight(format: String): Option[GrvDateMidnight] = {
      if (orig == null || orig.isEmpty) return None
      try {
        tryToDateMidnight(DateTimeFormat.forPattern(format))
      }
      catch {
        case _: Exception => None
      }
    }

    def tryToDateMidnight(formatter: DateTimeFormatter = ISODateTimeFormat.dateHourMinuteSecond()): Option[GrvDateMidnight] = {
      if (orig == null || orig.isEmpty) return None
      try {
        Some(formatter.parseDateTime(orig).toGrvDateMidnight)
      }
      catch {
        case _: Exception => None
      }
    }

    def validateDate(format: String): ValidationNel[FailureResult, Date] = {
      if (orig == null || orig.isEmpty) return FailureResult("String to convert MUST be not-null and not-empty!").failureNel

      for {
        formatter <- tryToSuccessNEL(new SimpleDateFormat(format), ex => FailureResult(s"Failed to create SimpleDateFormat for `$format`!", ex))
        date <- tryToSuccessNEL(formatter.parse(orig), ex => FailureResult(s"Failed to parse string: `$orig` with format: `$format`!", ex))
      } yield date
    }

    def tryToDate(format: String): Option[Date] = {
      if (orig == null || orig.isEmpty) return None
      try {
        tryToDate(new SimpleDateFormat(format))
      } catch {
        case _: Exception => None
      }
    }

    def tryToDate(formatter: SimpleDateFormat): Option[Date] = {
      if (orig == null || orig.isEmpty) return None
      try {
        Some(formatter.parse(orig))
      } catch {
        case _: Exception => None
      }
    }

    def tryToURL: Option[URL] = validateUrl.toOption

    def validateUrl: Validation[FailureResult, URL] = {
      try {
        URLUtils.tryToFixUrl(orig).flatMap(urlStr => {
          val url = new URL(null, urlStr, urlHandler)
          Option(url.getAuthority).flatMap(auth => if (auth.isEmpty) None else Some(auth)) match {
            case Some(auth) if auth.contains(' ') =>
              return FailureResult(s"Failed to parse url: '$orig' because host portion contains a space!").failure
            case Some(_) =>
              Some(url)
            case _ =>
              None
          }
        }).toValidation(FailureResult("Failed to parse url: " + orig))
      } catch {
        case ex: Exception => FailureResult("Failed to parse url: " + orig, ex).failure
      }
    }

    def tryToURI: Option[URI] = {
      if (orig == null || orig.isEmpty) return None

      try {
        Some(new URI(orig))
      } catch {
        case _: Exception => None
      }
    }

    def getMurmurHash: Long = murmurHash(orig)

    def truncTo(maxLength: Int, noteTruncationWith: String = emptyString): String = truncStringTo(orig, maxLength, noteTruncationWith)

    def noneForEmpty: Option[String] = noneForEmpty(trim = false)

    def noneForEmpty(trim: Boolean): Option[String] = {
      if (orig == null)
        None
      else {
        trim ? orig.trim | orig match {
          case mt if mt.isEmpty => None
          case good => Some(good)
        }
      }
    }

    def noneForShorterThan(minLength: Int): Option[String] = orig.noneForEmpty match {
      case Some(str) if str.length < minLength => None
      case other => other
    }

    def countUpperCase(): Int = grvstrings.countUpperCase(orig)

    def countNonLetters(): Int = grvstrings.countNonLetters(orig)

    def appendQueryParam(name: String, value: String): String = URLUtils.appendParameter(orig, name + "=" + value)

    def splitBetter(delim: String, maxTokens: Int = -1): Array[String] = tokenize(orig, delim, maxTokens)

    /**
     * Interprets things boringly, such as an empty string means 1 token of the empty string (whereas splitBetter()
     * considers empty string to be 0 tokens) and trailing delimiter means one trailing empty string token. This is a
     * literal, no-frills interpretation of tokenize.
     */
    def splitBoringly(delim: String, maxTokens: Int = -1): Array[String] = tokenizeBoringly(orig, delim, maxTokens)

    def findIndex(str: String): Option[Int] = {
      val idx = orig.indexOf(str)

      if (idx == -1) None else Some(idx)
    }

    def padLeft(totalLen: Int, padChr: Char = ' '): String = orig.reverse.padTo(totalLen, padChr).mkString("").reverse

    def trimLeft(trimChar: Char): String = {
      if(orig.isEmpty)
        return orig

      var pos: Int = 0
      while(pos < orig.length && orig.charAt(pos) == trimChar)
        pos += 1
      orig.substring(pos)
    }

    def trimRight(trimChar: Char): String = {
      if(orig.isEmpty)
        return orig

      var pos: Int = orig.length - 1
      while(pos >= 0 && orig.charAt(pos) == trimChar)
        pos -= 1
      orig.substring(0, 1 + pos)
    }

    /**
     * @return The right side of the original string up until (and excluding) the first char such that stopWhen(...) == TRUE.
     *         The string is traversed from right to left.
     */
    def takeRightUntil(stopWhen: Char => Boolean): String = {
      val keepIfTrue = stopWhen.andThen(!_)
      orig.reverseIterator.takeWhile(keepIfTrue).mkString("").reverse
    }

    def compareIgnoreCase(that: String): Int = compare(orig, that, ignoreCase = true)

    def equalsIgnoreCase(that: String): Boolean = equals(orig, that)
  }

  def formatNel[E <: HasMessage, X <: AnyRef](nel: ValidationNel[E, X]): String = {
    nel match {
      case Success(res: TraversableOnce[_]) => "success: " + res.size + " items"
      case Success(res) => "success: " + ClassName.simpleName(res)
      case Failure(fail) => "failure: " + fail.list.map(f => ClassName.simpleName(f) + ": " + f.message).mkString(", ")
    }
  }

  implicit class GrvURL(val url: URL) extends AnyVal {
    /**
     * Like URL.toExternalForm(), except without the URL path, query string, and fragment parts.
     *
     * @return String like "http://www.example.com/".
     */
    def baseUrl: String = {
      val authorityOpt = url.getAuthority.noneForEmpty
      val baseUrlBuffer = new StringBuffer(
        url.getProtocol.length + // http
        1 + // :
        authorityOpt.map(2 + _.length).getOrElse(0) + // //foobar.com
        1 // /
      )

      baseUrlBuffer.append(url.getProtocol).append(":")
      authorityOpt.foreach(baseUrlBuffer.append("//").append)
      baseUrlBuffer.append("/")

      baseUrlBuffer.toString
    }

    def getDomain: String = url.getAuthority.noneForEmpty match {
      case Some(authority) => authority match {
        case auth if auth.startsWith("www.") => auth.substring(4)
        case other => other
      }
      case None => emptyString
    }

    def getDomainOption: Option[String] = getDomain.noneForEmpty

    def getTopLevelDomain: Option[String] =
      try
        InternetDomainName.from(url.getHost).topPrivateDomain.toString.some
      catch {
        case e: Exception => None
      }
  }

  implicit class GrvStringMap(val map: scala.collection.Map[String, String]) extends AnyVal {
    def getBoolean(key: String): Option[Boolean] = map.get(key) flatMap (_.tryToBoolean)
    def getDouble(key: String): Option[Double] = map.get(key) flatMap (_.tryToDouble)
    def getInt(key: String): Option[Int] = map.get(key) flatMap (_.tryToInt)
    def getByte(key: String): Option[Byte] = map.get(key) flatMap (_.tryToByte)
    def getLong(key: String): Option[Long] = map.get(key) flatMap (_.tryToLong)
    def getMimeType(key: String): Option[MimeType] = map.get(key) flatMap (_.tryToMimeType)
    def getShort(key: String): Option[Short] = map.get(key) flatMap (_.tryToShort)
    def getURL(key: String): Option[URL] = map.get(key) flatMap (_.tryToURL)
  }

  def renderCaseClassOfOptions(caseClassInstance: Product, withClassnameWrapper: Boolean = true)(customFormats: PartialFunction[Any, String]): String = {
    val default: PartialFunction[Any, String] = {
      case any => any.toString
    }
    val customFormatsWithDefault = customFormats orElse default
    val caseClassName = caseClassInstance.productPrefix
    val somes = caseClassInstance.productIterator.collect {
      case Some(t) => t
    }
    val unwrap: PartialFunction[Any, String] = customFormats orElse {
      case Success(t) => customFormatsWithDefault(t)
      case Failure(t) if t.isInstanceOf[NonEmptyList[_]]=> s"Nel(${(t.asInstanceOf[NonEmptyList[Any]] map customFormatsWithDefault).mkString(", ")})"
      case Failure(t) => customFormatsWithDefault(t)
      case nel: NonEmptyList[_] => s"Nel(${(nel map customFormatsWithDefault).mkString(", ")})"
      case any => any.toString
    }
    val renders = somes map unwrap
    val commaSep = renders.mkString(", ")
    if(withClassnameWrapper) s"$caseClassName($commaSep)"
    else s"$commaSep"
  }

  val xmlTagReg: Regex = """</?\s*([^>]+)\s*>""".r("tagName")

  def hashAndBase64(str:String): String = {
    val longue = MurmurHash.hash64(str)
    Base64.encodeBase64String(Bytes.toBytes(longue))
  }

  def encodeBase64(str: String): String = {
    Base64.encodeBase64String(str.getBytes(Charsets.UTF_8))
  }

  def decodeBase64(encoded: String): String = {
    new String(Base64.decodeBase64(encoded), Charsets.UTF_8)
  }

  val COMPARE_WIN: Int = -1
  val COMPARE_LOSE: Int = 1
  val COMPARE_TIE: Int = 0
  val emptyString: String = ""
  val nullString: String = "NULL"

  def equals(s1: String, s2: String, ignoreCase: Boolean = false): Boolean = compare(s1, s2, ignoreCase) == COMPARE_TIE

  def compare(s1: String, s2: String, ignoreCase: Boolean = false): Int = {
    if (s1 == s2) return COMPARE_TIE
    if (s1 == null) return COMPARE_LOSE
    if (s2 == null) return COMPARE_WIN
    if (ignoreCase) return s1.compareToIgnoreCase(s2)
    s1.compareTo(s2)
  }

  /*
  * useful for padding output debug data using fixed width columns for easy reading
  * pass in the string and number of spaces you wanted padded to the right
  */
  def pad(elem: Any, spaces: Int, separator: String = " "): String = {
    val elemString = elem.toString
    val cleanStr = if (elemString.length() >= spaces) elemString.substring(0, spaces - 4) + "..." else elemString
    cleanStr.padTo(spaces, separator).mkString(emptyString)

  }

  /**
   * dumps out a more readable println with a heading so you can see your debug statements easier
   */
  def pr(heading: String, body: Any = emptyString): Unit = {
    println(s"\n**********\n$heading\n\n$body\n**********\n")
  }

  def murmurHash(text: String): Long = MurmurHash.hash64(text)

  def combineNonEmptyStrings(delim: String, parts: String*): String = {
    if (parts == null) return null

    val useDelim = !ScalaMagic.isNullOrEmpty(delim)
    val sb = new StringBuilder

    var pastFirst = false
    for {
      part <- parts
      if !ScalaMagic.isNullOrEmpty(part)
    } {
      if (pastFirst) {
        if (useDelim) sb.append(delim)
      } else pastFirst = true

      sb.append(part)
    }

    sb.toString()
  }

  def nullOrEmptyToNone(input: String): Option[String] = for {
    nonNull <- Option(input)
    if nonNull.nonEmpty
  } yield nonNull

  def nullToEmpty(input: String): String = nullOrEmptyToNone(input).getOrElse(emptyString)

  def countUpperCase(candidate: String): Int = candidate.foldLeft(0)((total: Int, c: Char) => if (Character.isUpperCase(c)) total + 1 else total)

  val singleQuote = '\''

  def countNonLetters(candidate: String): Int = candidate.foldLeft(0)((total: Int, c: Char) => {
    if (!Character.isSpaceChar(c) && !Character.isUpperCase(c) && !Character.isLowerCase(c) && !singleQuote.equals(c)) total + 1 else total
  })

  def buildReplacement(pattern: String, replaceWith: String): Replacement = Replacement(pattern.r, replaceWith)

  /**
   * Safely truncates `input` so it is no longer longer than `maxLength`.
   *
   * @param noteTruncationWith if the string is shortened, append this string to it (typically this param is an ellipsis).
   * @param preserveWords if true, trims the end of the string until it does not contain an incomplete word (word defined as delimited by whitespace).
   *  Also trims trailing whitespace. Note this can result in an empty string being returned if the input's first word is long.
   */
  def truncStringTo(input: String, maxLength: Int, noteTruncationWith: String = emptyString, preserveWords: Boolean = false): String = {
    def takePreserveWords(takeChars: Int) = {
      if (preserveWords && !input.charAt(takeChars).isWhitespace) {
        val taken = input.substring(0, takeChars)
        taken.take(taken.lastIndexWhere(_.isWhitespace) + 1).trim
      }
      else {
        input.substring(0, takeChars)
      }
    }

    input match {
      case null => emptyString
      case withinMax if withinMax.length() <= maxLength => withinMax
      case _ => if (noteTruncationWith.isEmpty) {
        takePreserveWords(maxLength)
      } else {
        takePreserveWords(maxLength - noteTruncationWith.length()) + noteTruncationWith
      }
    }
  }

  def truncStringBetween(input: String, maxCharsAtStart: Int, maxCharsAtEnd: Int, noteTruncationWith: String = emptyString): String = {
    if (input.length <= maxCharsAtStart + maxCharsAtEnd) {
      return input
    }

    val truncdStart = truncStringTo(input, maxCharsAtStart, noteTruncationWith)
    if (truncdStart.length == input.length) {
      return input
    }

    val truncdEnd = input.takeRight(maxCharsAtEnd)
    truncdStart + truncdEnd
  }

  private val regexReservedCharacters = Set('.', '$', '^', '{', '[', '(', '|', ')', ']', '}', '*', '+', '?', '\\')

  def containsRegexReservedCharacters(input: String): Boolean = input.exists(regexReservedCharacters.contains)

  /**
    * Tokenizes an input string separated by
    * a delim string and returns a max array of maxTokens.
    */
  def tokenize(input: String, delim: String, maxTokens: Int = -1): Array[String] = {
    if (ScalaMagic.isNullOrEmpty(input)) return Array.empty[String]
    if (ScalaMagic.isNullOrEmpty(delim)) return Array(input)

    val isDelimSingleChar = delim.length == 1
    val isLimitInUse = maxTokens > 0
    val limitedSingleCharDelim = isLimitInUse && isDelimSingleChar

    if (limitedSingleCharDelim || containsRegexReservedCharacters(delim)) {
      // Use our old way to protect against unintended results due to split's handling

      val buffer = new ArrayBuffer[String]

      val scanMe = if (input.endsWith(delim)) input else input + delim

      val delimLen = delim.length()
      var start = 0
      var delimIdx = scanMe.indexOf(delim)

      while (delimIdx > -1 && (maxTokens < 1 || buffer.size < maxTokens)) {
        buffer += scanMe.substring(start, delimIdx)
        start = delimIdx + delimLen
        delimIdx = scanMe.indexOf(delim, start)
      }

      buffer.toArray
    }
    else {
      // we can benefit from the optimized split method from Java 7

      if (isLimitInUse) {
        // tokenize is expected to handle limiting of tokens differently than String#split
        // therefore, we may need to do some cleanup after the call to split
        val rawResult = input.split(delim, maxTokens)
        rawResult.lastOption match {
          case Some(last) =>
            // we have a last element. Let's see if it contains any of our delim
            val idx = last.indexOf(delim)
            if (idx > -1) {
              // we have some stripping to do

              // strip away from the delim on
              val strippedLast = last.substring(0, idx)

              // drop the original last element
              val withoutEnd = rawResult.dropRight(1)

              // combine that with our stripped last element for the final result
              withoutEnd ++ Array(strippedLast)
            }
            else {
              // the last element does NOT contain anymore delims. nothing to strip.
              rawResult
            }

          case None =>
            // empty array. nothing to strip.
            rawResult
        }
      }
      else {
        // without a set limit, we can simply use the split method

        // If we have a single char delim, pass as a char
        if (isDelimSingleChar) input.split(delim.head) else input.split(delim)
      }

    }

  }

  /**
   * Whereas tokenize() will make decisions such as "empty string means 0 tokens" and "trailing delimiter should be
   * discarded", this function will quite boringly tokenize a string without being so helpful. Namely, an empty string
   * is one token, the empty string, and a trailing delimiter means the last token is an empty string.
   */
  def tokenizeBoringly(input: String, delim: String, maxTokens: Int = -1): Array[String] = {
    if (ScalaMagic.isNullOrEmpty(input)) return Array(emptyString)
    if (ScalaMagic.isNullOrEmpty(delim)) return Array(input)

    val tokens = new ArrayBuffer[StringBuffer]
    val inputLength = input.length
    val delimLength = delim.length
    var tokenPos = 0
    while (tokenPos < inputLength && (maxTokens < 1 || tokens.size < maxTokens)) {
      val delimPos = input.indexOf(delim, tokenPos)
      tokens += new StringBuffer(input.substring(tokenPos, if (delimPos == -1) inputLength else delimPos))
      tokenPos = if (delimPos == -1) inputLength else delimPos + delimLength
    }

    // Didn't finish parsing string; i.e. hit max tokens first
    if (tokenPos < inputLength)
      tokens.last.append(delim).append(input.substring(tokenPos))
    // Finished parsing string according to max tokens but string ends with delimiter
    else if (input endsWith delim)
      tokens += new StringBuffer

    tokens.toArray.map(_.toString)
  }

  def camelCaseToTitleCase(camelCased: String): String = {
    val titleCased = new StringBuilder(camelCased.length, camelCased.headOption.map(_.toUpper.toString).getOrElse(""))

    if(camelCased.length >= 2) {
      for(ch <- camelCased.tail) {
        if(ch.isUpper)
          titleCased.append(s" $ch")
        else
          titleCased.append(ch)
      }
    }

    titleCased.toString()
  }

  case class Replacement(regex: Regex, replaceWith: String) {
    def replaceAll(input: CharSequence): String = if (ScalaMagic.isNullOrEmpty(input)) grvstrings.emptyString else regex.replaceAllIn(input, replaceWith)

    def chain(pattern: String, replaceWith: String): ReplaceSequence = chain(pattern.r, replaceWith)

    def chain(regex: Regex, replaceWith: String): ReplaceSequence = ReplaceSequence(Seq(this, Replacement(regex, replaceWith)))
  }

  case class ReplaceSequence(replacements: Seq[Replacement]) {
    def replaceAll(input: CharSequence): String = {
      if (input == null || input.length() == 0) return ""

      replacements.foldLeft(input)((in: CharSequence, rep: Replacement) => rep.replaceAll(in)).toString
    }

    def chain(pattern: String, replaceWith: String): ReplaceSequence = chain(pattern.r, replaceWith)

    def chain(regex: Regex, replaceWith: String): ReplaceSequence = ReplaceSequence(this.replacements.:+(Replacement(regex, replaceWith)))
  }

  implicit val regexFormat: Format[Regex] = (
    (__ \ "regex").format[String] and
    (__ \ "groupNames").format[Seq[String]]
  )(
    { case (regex: String, groupNames: Seq[String]) => new Regex(regex, groupNames: _*) },
    { case r: Regex => (r.pattern.pattern(), Seq.empty) }
  )

  implicit val regexDefaultValueWriter: DefaultValueWriter[Regex] = new DefaultValueWriter[Regex] {
    override def serialize(t: Regex): String = t.pattern.pattern()
  }

  implicit val regexMatchIteratorFormat: Format[Regex.MatchIterator] = (
    (__ \ "source").format[java.lang.String] and
    (__ \ "regex").format[Regex] and
    (__ \ "groupNames").format[Seq[String]]
  )(
    new MatchIterator(_: java.lang.String, _: Regex, _: Seq[String]),
    it => (it.source.toString, it.regex, it.groupNames)
  )

  implicit val regexMatchIteratorDefaultValueWriter: DefaultValueWriter[MatchIterator] = new DefaultValueWriter[Regex.MatchIterator] {
    override def serialize(t: Regex.MatchIterator): String = t.regex.pattern.pattern()
  }

  import scala.language.experimental.macros
  def argNamesAndValues: String = macro argNamesAndValuesImpl
  def argNamesAndValuesImpl(c: Context): c.Tree = {
    import c.universe._

    // class for which we want to display toString
    val klass = c.internal.enclosingOwner.owner


    // we keep the getter fields created by the user
    val fields: Iterable[c.Symbol] = klass.asClass.toType.decls
      .filter(sym => sym.isMethod && sym.asTerm.isParamAccessor && sym.isPublic) // we might want to do more/different filtering here


    // print one field as <name of the field>+": "+fieldName
    def printField(field: Symbol) = {
      val fieldName = field.name

      q"""${fieldName.decodedName.toString}+${": "}+this.$field"""
    }

    val emptyStrStructure = q"${""}"
    val params = fields.foldLeft(emptyStrStructure) { (res, acc) =>
      val field = printField(acc)
      //separate the fields by a comma if the previous result isn't just the empty string/tree
      val delimiter = if (res.equalsStructure(emptyStrStructure) || field.equalsStructure(emptyStrStructure)) "" else ", "
      q"$res + $delimiter + $field"
    }

    // print the class and all the parameters with their values
    q"$params"
  }
}