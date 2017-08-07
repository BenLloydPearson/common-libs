package com.gravity.interests.jobs.intelligence.algorithms.graphing

import com.gravity.utilities.{FileHelper, Settings}
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.web.ContentUtils
import com.gravity.utilities.web.ContentUtils.ZZ_BetterElements
import org.jsoup.Jsoup

import scala.io.Source
import scalaz.{Failure, Success}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/14/12
 * Time: 3:52 PM
 */

object YahooOlympicsCsvTool {
  val comma = ","
  val sportsRel = "IS A CONCEPT UNDER"
  val olympicUriPrefix = "http://www.london2012.com/"
  val olympicUriPrefixLength = olympicUriPrefix.length
  val sportImageBase = olympicUriPrefix + "imgml/mascot/sports/160x139/"
  val athletePrefix = olympicUriPrefix + "athlete/"
  val defaultColor = "#bbbbbb"
  val headers = Map("accept" -> "*.*")

  def trimFirstAndLast(s: String): String = s.substring(1, s.length - 1)

  def buildLine(line: String): Option[YahooOlympicsCsvLine] = {
    val parts = tokenize(line, comma, 6)

    if (parts.length < 4) return None

    val color = parts.lift(4) match {
      case Some(clr) => trimFirstAndLast(clr)
      case None => emptyString
    }

    val image = parts.lift(5) match {
      case Some(img) => trimFirstAndLast(img)
      case None => emptyString
    }

    Some(
      YahooOlympicsCsvLine(
        trimFirstAndLast(parts(0)),
        trimFirstAndLast(parts(1)),
        trimFirstAndLast(parts(2)),
        trimFirstAndLast(parts(3)),
        color,
        image
      )
    )
  }

  lazy val lines = (for {
    line <- Source.fromInputStream(getClass.getResourceAsStream("yahoo_olympics_graphing_data.csv")).getLines()
    csvLine <- buildLine(line)
  } yield csvLine).toSeq

  lazy val lineMap = lines.map(csvLine => csvLine.uri -> csvLine).toMap

  lazy val sportToColorMap = (for {
    line <- Source.fromInputStream(getClass.getResourceAsStream("sports.txt")).getLines()
    parts = tokenize(line, "\t", 2)
    if (parts.length == 2)
  } yield parts(0) -> parts(1)).toMap

  def lineByUri(uri: String): Option[YahooOlympicsCsvLine] = lineMap.get(uri)

  def colorByUri(uri: String): String = lineByUri(uri) match {
    case Some(line) => line.getColor
    case None => defaultColor
  }

  def getNameOrElse(uri: String, orElse: String): String = lineByUri(uri) match {
    case Some(line) => line.name
    case None => orElse
  }

  def isAthlete(uri: String): Boolean = uri.startsWith(athletePrefix)
  def isEvent(uri: String): Boolean = uri.endsWith("/index.html") && uri.contains("/event/")
  def isSport(uri: String): Boolean = !isAthlete(uri) && !isEvent(uri)

  def writeNewCsvWithExtras() {
    val file = s"${Settings.tmpDir}/olympics.csv"
    var i = 0
    FileHelper.withFileWriter(file) {
      out => lines.foreach(l => {
        i += 1
        val isTimeToPrint = (i < 20 || i % 100 == 0)
        val fullLine = l.fullString(isTimeToPrint)
        if (isTimeToPrint) println("writing line #" + i + ":\n\t" + fullLine)
        out.writeLine(fullLine)

        if (i % 200 == 0) out.flush()
      })
    }
  }
}

case class YahooOlympicsCsvLine(uri: String, name: String, rel: String, obj: String, color: String = emptyString, image: String = emptyString) {
  import YahooOlympicsCsvTool.{olympicUriPrefix, olympicUriPrefixLength, sportImageBase, headers, sportToColorMap, defaultColor}

  lazy val isAthlete: Boolean = YahooOlympicsCsvTool.isAthlete(uri)
  lazy val isEvent: Boolean = YahooOlympicsCsvTool.isEvent(uri)
  lazy val isSport: Boolean = YahooOlympicsCsvTool.isSport(uri)
  lazy val parentOption: Option[YahooOlympicsCsvLine] = YahooOlympicsCsvTool.lineByUri(obj)

  lazy val colorByParent = if (isSport) {
    sportToColorMap.getOrElse(uri, defaultColor)
  } else {
    sportToColorMap.getOrElse(obj, defaultColor)
  }

  def getColor: String = if (color.isEmpty) colorByParent else color

  def getThumbnail(isDebug: Boolean = false): String = {
    if (!image.isEmpty) return image

    if (isSport) {
      sportImageBase +
        uri.substring(olympicUriPrefixLength).dropRight(1) + ".jpg"
    } else if (isEvent) {
      sportImageBase +
        uri.substring(olympicUriPrefixLength).split("/")(0) + ".jpg"
    } else {
      if (isDebug) println("About to scrape: " + uri)
      ContentUtils.getWebContentAsValidationString(uri, headers) match {
        case Success(html) => {
          if (isDebug) println("Successfully got HTML...")
          val doc = Jsoup.parse(html)
          ZZ_BetterElements(doc.select("div.athletePhoto > img")).headOption match {
            case Some(img) => {
              if (isDebug) println("Successfully got img: " + img.toString)
              olympicUriPrefix + img.attr("src").trim.drop(1)
            }
            case None => {
              if (isDebug) println("FAILED to find img element!")
              emptyString
            }
          }
        }
        case Failure(failed) => {
          if (isDebug) println("FAILED to load HTML: " + failed.message)
          emptyString
        }
      }
    }
  }

  def fullString(isDebug: Boolean = false): String = {
    val dq = '"'
    val cm = ','
    val b = new StringBuilder
    b.append(dq).append(uri).append(dq).append(cm)
    b.append(dq).append(name).append(dq).append(cm)
    b.append(dq).append(rel).append(dq).append(cm)
    b.append(dq).append(obj).append(dq).append(cm)
    b.append(dq).append(getColor).append(dq).append(cm)
    b.append(dq).append(getThumbnail(isDebug)).append(dq)
    b.toString()
  }
}

object YahooOlympicsCsvSportsWriter extends App {

  import YahooOlympicsCsvTool._

  for {
    line <- lines
    if (line.rel == sportsRel)
  } println(line.name + "\t" + line.uri)
}

object TestYa extends App {
  val sportUri = "http://www.london2012.com/judo/"
  val expectedColor = "#3ed0fd"

  val color = YahooOlympicsCsvTool.colorByUri(sportUri)

  println(color)
}