package com.gravity.utilities

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 5/31/11
 * Time: 3:47 PM
 */

import java.lang.Double
import java.math.BigDecimal
import java.net.URL

import com.gravity.test.utilitiesTesting
import com.gravity.utilities.grvstrings._
import com.gravity.valueclasses.ValueClassesForUtilities._
import org.apache.commons.lang.time.StopWatch
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.Assert._

import scala.util.matching.Regex
import scalaz.syntax.std.option._
import scalaz.{Failure, Success}

class grvstringsTest extends BaseScalaTest with utilitiesTesting {
  val text = "this is my message\nwith new lines\r\nmulti-cr's and the odd sing back-slash r's\ras well\nIt also contains a^crap^ton^of^freaking^car^rots^to^annoy^the fuck^out\r\nof^anyone!"
  val cleanReps: ReplaceSequence = buildReplacement("\\^", "<CAR>").chain("(\n|\r\n|\r)", "<BR>")
  val titleReplacements: ReplaceSequence = buildReplacement("<BR>", " ").chain("\t", emptyString).chain(" {2,}", " ")

  test("EscapeForSqlLike") {
    assertEquals("", "".escapeForSqlLike)
    assertEquals("test123", "test123".escapeForSqlLike)
    assertEquals("\\%hello\\_world\\%", "%hello_world%".escapeForSqlLike)
  }

  test("HashCompression") {
    println(grvstrings.hashAndBase64("3ojpfo3jpo2j45po3j4po3j4"))
  }

  test("TitleReplacements") {
    val origTitle = "This has a \ttab in it and some   extra     spaces<BR>also a line break "
    val cleanTitle = titleReplacements.replaceAll(origTitle).toString

    println("Orig:  " + origTitle)
    println("Clean: " + cleanTitle)
  }

  test("Tokenize") {
    val input = "2011-02-19 12:28:51^newpost^TEST93f88b0dda2a035be1e9998b8014^googleChrome^http://www.pbnation.com/forumdisplay.php?s=ceb^http://www.pbnation.com/^127.0.0.1^Unregistered^dd497cee28d01bcdcfd6e2e2704c61db^88^^999^9999^Post Title Here^BaseballCategory^FootballTag^^^this is my message^{\"tags_as_tags\":true}^1286435516000^Jim Bonez^M^key,words,tend,to,suck^1^<html>goflyers</html>^this is too much stuff^and so is this^^now we've gone way too far!"
    val tokens = tokenize(input, "^", 27)

    assertEquals(27, tokens.size)
    assertEquals("newpost", tokens(1))
    assertEquals("this is too much stuff", tokens(26))
  }

  test("TokenizeBoringly") {
    assertEquals("empty input", 1, tokenizeBoringly("", ",").length)
    assertEquals("empty input", "", tokenizeBoringly("", ",")(0))

    assertEquals("empty tokens", 2, tokenizeBoringly(",", ",").length)
    assertEquals("empty tokens", "", tokenizeBoringly(",", ",")(0))
    assertEquals("empty tokens", "", tokenizeBoringly(",", ",")(1))

    assertEquals("one non-empty token", 4, tokenizeBoringly(",foo,,", ",").length)
    assertEquals("one non-empty token", "", tokenizeBoringly(",foo,,", ",")(0))
    assertEquals("one non-empty token", "foo", tokenizeBoringly(",foo,,", ",")(1))
    assertEquals("one non-empty token", "", tokenizeBoringly(",foo,,", ",")(2))
    assertEquals("one non-empty token", "", tokenizeBoringly(",foo,,", ",")(3))

    assertEquals("non-empty tokens", 3, tokenizeBoringly("foo,bar,baz", ",").length)
    assertEquals("non-empty tokens", "foo", tokenizeBoringly("foo,bar,baz", ",")(0))
    assertEquals("non-empty tokens", "bar", tokenizeBoringly("foo,bar,baz", ",")(1))
    assertEquals("non-empty tokens", "baz", tokenizeBoringly("foo,bar,baz", ",")(2))
    assertEquals("non-empty tokens with limit", 2, tokenizeBoringly("foo,bar,baz", ",", 2).size)
    assertEquals("non-empty tokens with limit", "foo", tokenizeBoringly("foo,bar,baz", ",", 2)(0))
    assertEquals("non-empty tokens with limit", "bar,baz", tokenizeBoringly("foo,bar,baz", ",", 2)(1))

    assertEquals("empty input", 1, tokenizeBoringly("", ",").length)
    assertEquals("empty input", "", tokenizeBoringly("", ",")(0))

    assertEquals("empty input, empty delim", 1, tokenizeBoringly("", "").length)
    assertEquals("empty input, empty delim", "", tokenizeBoringly("", "")(0))

    assertEquals("empty delim", 1, tokenizeBoringly("foobar", "").length)
    assertEquals("empty delim", "foobar", tokenizeBoringly("foobar", "")(0))

    assertEquals("mult-char delim", 3, tokenizeBoringly("foo..bar..baz", "..").length)
    assertEquals("mult-char delim", "foo", tokenizeBoringly("foo..bar..baz", "..")(0))
    assertEquals("mult-char delim", "bar", tokenizeBoringly("foo..bar..baz", "..")(1))
    assertEquals("mult-char delim", "baz", tokenizeBoringly("foo..bar..baz", "..")(2))
    assertEquals("mult-char delim with limit", 2, tokenizeBoringly("foo..bar..baz", "..", 2).size)
    assertEquals("mult-char delim with limit", "foo", tokenizeBoringly("foo..bar..baz", "..", 2)(0))
    assertEquals("mult-char delim with limit", "bar..baz", tokenizeBoringly("foo..bar..baz", "..", 2)(1))
  }

  test("ReplacingShit") {
    println(text)

    val actual = cleanReps.replaceAll(text)
    println("       BECOMES:")
    println(actual)

    assertEquals("this is my message<BR>with new lines<BR>multi-cr's and the odd sing back-slash r's<BR>as well<BR>It also contains a<CAR>crap<CAR>ton<CAR>of<CAR>freaking<CAR>car<CAR>rots<CAR>to<CAR>annoy<CAR>the fuck<CAR>out<BR>of<CAR>anyone!", actual)
  }

  test("NoneForEmpty") {
    val nullWrapped = giveNullString
    nullWrapped.noneForEmpty match {
      case Some(oops) => fail("null should be considered empty!")
      case None => println("YAY!")
    }


  }

  def giveNullString: String = null

  test("CountUpperCase") {
    assertEquals("Baron John Rosen has 3 uppercase!", 3, countUpperCase("Baron John Rosen"))
    assertEquals("LOL has 3 uppercase!", 3, countUpperCase("LOL"))
    assertEquals("Chris_Bissell has 2 uppercase!", 2, countUpperCase("Chris_Bissell"))
    assertEquals("John Dunne has 2 uppercase!", 2, countUpperCase("John Dunne"))
    assertEquals("efrem zimbalist jr has zero uppercase!", 0, countUpperCase("efrem zimbalist jr"))
  }

  test("PimpCountUpperCase") {
    assertEquals("Baron John Rosen has 3 uppercase!", 3, "Baron John Rosen".countUpperCase())
    assertEquals("LOL has 3 uppercase!", 3, "LOL".countUpperCase())
    assertEquals("Chris_Bissell has 2 uppercase!", 2, "Chris_Bissell".countUpperCase())
    assertEquals("John Dunne has 2 uppercase!", 2, "John Dunne".countUpperCase())
    assertEquals("efrem zimbalist jr has zero uppercase!", 0, "efrem zimbalist jr".countUpperCase())
  }

  test("CountNonLetters") {
    assertEquals(":) has 2 non letters!", 2, countNonLetters(":)"))
    assertEquals("Hi there peeps :) has 2 non letters!", 2, countNonLetters("Hi there peeps :)"))
  }

  test("TryToConversions") {
    // tryToBoolean
    assertEquals("\"1\".tryToBoolean should return Some(true)!", Some(true), "1".tryToBoolean)
    assertEquals("\"0\".tryToBoolean should return Some(false)!", Some(false), "0".tryToBoolean)
    assertEquals("\"5\".tryToBoolean should return None!", None, "5".tryToBoolean)
    assertEquals("\"true\".tryToBoolean should return Some(true)!", Some(true), "true".tryToBoolean)
    assertEquals("\"false\".tryToBoolean should return Some(false)!", Some(false), "false".tryToBoolean)
    assertEquals("\"foobar\".tryToBoolean should return None!", None, "foobar".tryToBoolean)

    // tryToByte
    val byte1: Byte = 1
    val byte127: Byte = 127
    assertEquals("\"1\".tryToByte should return Some(1)!", Some(byte1), "1".tryToByte)
    assertEquals("\"127\".tryToByte should return Some(127)!", Some(byte127), "127".tryToByte)
    assertEquals("\"128\".tryToByte should return None!", None, "128".tryToByte)

    // tryToShort
    val short1: Short = 1
    val shortNeg1: Short = -1
    assertEquals("\"1\".tryToShort should return Some(1)!", Some(short1), "1".tryToShort)
    assertEquals("\"-1\".tryToShort should return Some(-1)!", Some(shortNeg1), "-1".tryToShort)
    assertEquals("\"12345678\".tryToShort should return None!", None, "12345678".tryToShort)

    // tryToInt
    assertEquals("\"1\".tryToInt should return Some(1)!", Some(1), "1".tryToInt)
    assertEquals("\"-1\".tryToInt should return Some(-1)!", Some(-1), "-1".tryToInt)
    assertEquals("\"1234567890\".tryToInt should return Some(1234567890)!", Some(1234567890), "1234567890".tryToInt)
    assertEquals("\"12345678900\".tryToInt should return None!", None, "12345678900".tryToInt)

    // tryToLong
    assertEquals("\"1\".tryToLong should return Some(1l)!", Some(1l), "1".tryToLong)
    assertEquals("\"-1\".tryToLong should return Some(-1l)!", Some(-1l), "-1".tryToLong)
    assertEquals("\"1234567890\".tryToLong should return Some(12345678900)!", Some(12345678900l), "12345678900".tryToLong)
    assertEquals("\"92233720368547758070\".tryToLong should return None!", None, "92233720368547758070".tryToLong)

    // tryToDouble
    val double1: Double = 1d
    val doubleMax = Double.MAX_VALUE
    val doubleTooBig = BigDecimal.valueOf(doubleMax).add(BigDecimal.valueOf(1l))
    println("doubleMax: " + doubleMax)
    println("doubleTooBig: " + doubleTooBig)
    assertEquals("\"1\".tryToDouble should return Some(1)!", Some(double1), "1".tryToDouble)
    assertEquals("\"%s\".tryToDouble should return Some(%s)!".format(doubleMax, doubleMax), Some(doubleMax), doubleMax.toString.tryToDouble)


    //tryToDate
    val dateTimeFmtString = "yyyy-MM-dd HH:mm:ss"
    val dateTimeFmt = DateTimeFormat.forPattern(dateTimeFmtString)
    val now = new DateTime()
    val nowLong = now.getMillis
    val roundedLong = now.minusMillis(now.getMillisOfSecond).getMillis
    val nowString = now.toString(dateTimeFmt)
    assertEquals(nowLong, nowLong.toString.tryToDateTime(dateTimeFmt).get.getMillis)
    assertEquals(roundedLong, nowString.tryToDateTime(dateTimeFmt).get.getMillis, roundedLong)
    assertEquals(nowLong, nowLong.toString.tryToDateTime(dateTimeFmtString).get.getMillis)
    assertEquals(roundedLong, nowString.tryToDateTime(dateTimeFmtString).get.getMillis)

    // tryToURL
    val nullStr: String = null
    assertEquals(None, nullStr.tryToURL)
    assertEquals(None, "".tryToURL)
    assertEquals(None, "ftp://ftp.example.com/path/file.txt".tryToURL)
    assertEquals(None, "http:// www.example.com/url/with/a/space/should/fail".tryToURL)
    assertEquals(Option(new URL("http://www.sample.com/path")),
                                "http://www.sample.com/path".tryToURL)
    assertEquals(Option(new URL("http://s3.r29static.com/bin/entry/ecb/x,80/1263664/vesperpag.jpg")),
                            "http:\\/\\/s3.r29static.com/bin/entry/ecb/x,80/1263664/vesperpag.jpg".tryToURL)
    assertEquals(Option(new URL("https://s3.r29static.com/bin/entry/ecb/x,80/1263664/vesperpag.jpg")).map(_.toString),
                            "https:\\/\\/s3.r29static.com/bin/entry/ecb/x,80/1263664/vesperpag.jpg".tryToURL.map(_.toString))
  }

  test("tryToURL special cases") {
    val evilUrl = "https:\\/\\/s3.r29static.com/bin/entry/ecb/x,80/1263664/vesperpag.jpg"
    val evilUrlFixed = "https://s3.r29static.com/bin/entry/ecb/x,80/1263664/vesperpag.jpg"

    evilUrl.tryToURL.map(_.toString) match {
      case Some(parsedEvil) => assertEquals("We did not get the expected parsed URL", evilUrlFixed, parsedEvil)
      case None => fail(s"Unable to parse evilUrl '$evilUrl' but we really should be able to.")
    }

    val queenBae = "http://www.huffingtonpost.com/entry/yes-everyones-flipping-out-over-beyoncé-but-this-tweet-is-a-must-see_us_58924314e4b070cf8b805f33"
    val queenBaeFixed = "http://www.huffingtonpost.com/entry/yes-everyones-flipping-out-over-beyonc%C3%A9-but-this-tweet-is-a-must-see_us_58924314e4b070cf8b805f33"

    queenBae.tryToURL.map(_.toString) match {
      case Some(parsedBae) => assertEquals("We did not get the expected parsed URL", queenBaeFixed, parsedBae)
      case None => fail(s"Unable to parse queenBae '$queenBae' but we really should be able to.")
    }
  }

  test("ValidateUrlWithSpaceInHostname") {
    "http:// www.example.com/url/with/a/space/should/fail".validateUrl match {
      case Success(url) =>
        fail("URLs MUST NOT contain spaces in the hostname/authority portion: " + url.toString)

      case Failure(failed) =>
        println(s"Expected failure received: '${failed.message}'")
        assertTrue("Failure should identify space in hostname.", failed.message.endsWith("because host portion contains a space!"))
    }
  }

  test("GetMurmurHash") {
    val string = "http://dbpedia.org/resource/YouTube"
    println(string.getMurmurHash)
  }

  val COMMA_REGEX: Regex = ",".r

  ignore("Performance") {
    val iterz = 10000
    val csv = "hello,this,is,a,pretty,lame,comma,separated,value,example,but,what,the,fukk," * 10000

    val sw = new StopWatch
    var i = 1
    sw.start()
    while (i < iterz) {
      splitLazyWay(csv)
      i += 1
    }
    sw.stop()

    val lazyTime = sw.getTime
    i = 1
    sw.reset()

    sw.start()
    while (i < iterz) {
      splitOldWay(csv)
      i += 1
    }
    sw.stop()

    val oldTime = sw.getTime
    i = 1
    sw.reset()

    sw.start()
    while (i < iterz) {
      splitNewWay(csv)
      i += 1
    }

    sw.stop()

    val newTime = sw.getTime

    println(f"LAST RUN: ${grvtime.currentTime.toString("MM/dd/yyyy hh:mm a")}\nResults for $iterz%,d iterations per method:\n\tOld way:\t\t${grvtime.millisToLongFormat(oldTime)}\n\tNew way:\t\t${grvtime.millisToLongFormat(newTime)}\n\tLazy way:\t\t${grvtime.millisToLongFormat(lazyTime)}\n\n\tPercent\n\tBetter (old->new):\t\t${((oldTime.toDouble / newTime.toDouble) * 100.0) - 100.0}%.2f%%")
    /*
      LAST RUN: 04/22/2016 05:39 PM
      Results for 10,000 iterations per method:
        Old way:		52 seconds and 767 milliseconds
        New way:		29 seconds and 136 milliseconds
        Lazy way:		37 seconds and 632 milliseconds

        Percent
        Better (old->new):		81.11%
     */
  }

  def splitNewWay(input: String): Array[String] = input.splitBetter(",")

  def splitLazyWay(input: String): Array[String] = input.split(",")

  def splitOldWay(input: String): Array[String] = COMMA_REGEX.split(input)


  test("truncString") {
    val s = "My Beautiful Dark Twisted Fantasy"
    truncStringTo(s, 10) should equal ("My Beautif")
    truncStringTo(s, 10, preserveWords = true) should equal ("My")
    truncStringTo(s, 10, "...") should equal ("My Beau...")
    truncStringTo(s, 10, "...", preserveWords = true) should equal ("My...")
    truncStringTo(s, 12, "...", preserveWords = true) should equal ("My...")
    truncStringTo(s, 12) should equal ("My Beautiful")
    truncStringTo(s, 12, preserveWords = true) should equal ("My Beautiful")
    truncStringTo(s, 34) should equal (s)
    truncStringBetween(s, 11, 6, "…") should equal ("My Beautif…antasy")
    truncStringBetween(s, 22, 14, "…") should equal (s)
  }

  test("SafeIndex") {
    "amilli".safeIndexOf("m") should equal(Option(1.asIndex))
    "amilli".safeIndexOf("zz") should equal(None)
  }

  test("SafeSubstring") {
    "amilli".safeSubstring(0.asIndex) should equal("amilli".substring(0).some)
    "amilli".safeSubstring(0.asIndex, 5.asIndex) should equal("amilli".substring(0,5).some)
    "amilli".safeSubstring(5.asIndex) should equal("amilli".substring(5).some)
    "amilli".safeSubstring(1.asIndex) should equal("amilli".substring(1).some)
    "amilli".safeSubstring(1.asIndex, 4.asIndex) should equal("amilli".substring(1,4).some)
    "amilli".safeSubstring(4.asIndex) should equal("amilli".substring(4).some)
    "amilli".safeSubstring(4.asIndex, 4.asIndex) should equal("amilli".substring(4,4).some)
    "amilli".safeSubstring("amilli".length.asIndex, "amilli".length.asIndex) should equal("amilli".substring("amilli".length, "amilli".length).some)

    "amilli".safeSubstring((-1).asIndex) should equal(None)
    "amilli".safeSubstring(3.asIndex, 0.asIndex) should equal(None)
    "amilli".safeSubstring(("amilli".length+1).asIndex) should equal(None)
    "amilli".safeSubstring(2.asIndex, ("amilli".length+1).asIndex) should equal(None)
    "amilli".safeSubstring(2.asIndex, (-1).asIndex) should equal(None)
  }

  test("TrimLeft") {
    "".trimLeft(''') should be("")
    " ''hello".trimLeft(''') should be(" ''hello")
    "''hello".trimLeft(''') should be("hello")
    "''".trimLeft(''') should be("")
  }

  test("TrimRight") {
    "".trimRight(''') should be("")
    "hello'' ".trimRight(''') should be("hello'' ")
    "hello''".trimRight(''') should be("hello")
    "''".trimRight(''') should be("")
  }

  test("TakeRightUntil") {
    "".takeRightUntil(_ => true) should be("")
    "".takeRightUntil(_ => false) should be("")
    "foobar".takeRightUntil(_ => false) should be("foobar")
    "foobar".takeRightUntil(_ => true) should be("")
    "foobar".takeRightUntil(_ == 'o') should be("bar")
  }

  test("camelCaseToTitleCase") {
    grvstrings.camelCaseToTitleCase("") should be("")
    grvstrings.camelCaseToTitleCase("foo") should be("Foo")
    grvstrings.camelCaseToTitleCase("fooBar") should be("Foo Bar")
  }
}
