package com.gravity.utilities.web.extraction

import java.util.Date

import com.gravity.goose.Article
import com.gravity.goose.images.Image
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.web.{ContentUtils, HttpArgumentsOverrides}
import net.liftweb.json.JsonAST.JValue
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.collection._
import scala.util.matching.Regex
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 7/1/14
 * Time: 5:56 PM
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
object HuffPoLiveArticleFetcher {
 import com.gravity.logging.Logging._
  val apiBaseUrl = "http://live.huffingtonpost.com/api/segments/"
  val huffPoLiveBaseUrl = "http://live.huffingtonpost.com/r/"
  val huffPoLiveHighlightBaseUrl = "http://live.huffingtonpost.com/r/highlight/"
  val huffPoLiveHighlightBaseUrl2 = "http://live.huffingtonpost.com/r/archive/segment/"

  val huffPoLiveHighlightArticleGuidRegEx: Regex = """^http://live\.huffingtonpost\.com/r/highlight/[a-zA-Z0-9-]+/([a-f0-9]{24})$""".r
  val huffPoLiveHighlightArticleGuidRegEx2: Regex = """^http://live\.huffingtonpost\.com/r/archive/segment/([a-f0-9]{24})$""".r
  val articleGuidRegEx: Regex = """^http://live\.huffingtonpost\.com/r/[a-z]+/[a-zA-Z0-9-]+/([a-f0-9]{24})$""".r
  val articleGuidAltRegEx: Regex = """^http://live\.huffingtonpost\.com/r/segment/([a-f0-9]{24})$""".r
  val httpArgumentsOverrides: HttpArgumentsOverrides = HttpArgumentsOverrides(optUserAgent = "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36".some)

  val headers: Map[String, String] = Map("accept" -> "*.*")

  def getApiUrl(url: String): Option[String] = {
    if (!url.startsWith(huffPoLiveBaseUrl)) return None

    url match {
      case huffPoLiveHighlightArticleGuidRegEx(guid) => Some(apiBaseUrl + guid + ".json")
      case huffPoLiveHighlightArticleGuidRegEx2(guid) => Some(apiBaseUrl + guid + ".json")
      case articleGuidRegEx(guid) => Some(apiBaseUrl + guid + ".json")
      case articleGuidAltRegEx(guid) => Some(apiBaseUrl + guid + ".json")
      case _ => None
    }
  }

  def isHuffPoLiveUrl(url: String): Boolean = {
    if (!url.startsWith(huffPoLiveBaseUrl)) return false

    getApiUrl(url).isDefined
  }

  def isHuffPoLiveHighlightUrl(url: String): Boolean = {
    if (!url.startsWith(huffPoLiveHighlightBaseUrl) && !url.startsWith(huffPoLiveHighlightBaseUrl2)) return false

    getApiUrl(url).isDefined
  }

  def getGooseArticle(url: String): ValidationNel[FailureResult, Article] = {
    for {
      apiUrl <- getApiUrl(url).toValidationNel(FailureResult(s"Submitted URL `$url` is not a HuffPoLive Article URL."))
      jsonString <- ContentUtils.getWebContentAsValidationString(apiUrl, headers = headers, argsOverrides = httpArgumentsOverrides.some).liftFailNel
      json <- tryParse(jsonString)
      data <- json.parseValue("data").liftFailNel
      article <- convertJValueToArticle(data, url)
    } yield article
  }

  def getArrayToSet(jsonData: JValue, name: String): Option[Set[String]] = {
    for {
      jarray <- jsonData.getJArray(name)
      items = jarray.arr.flatMap(_.stringOption).flatMap(_.trim.noneForEmpty)
      if items.nonEmpty
    } yield items.map(_.toLowerCase).toSet
  }

  def convertJValueToArticle(jsonData: JValue, articleUrl: String): ValidationNel[FailureResult, Article] = {
    val failures = mutable.Buffer[FailureResult]()

    val title = jsonData.parseString("title") valueOr(failure => {
      failures += failure
      ""
    })

    val description = jsonData.parseString("description") valueOr (failure => {
      failures += failure
      ""
    })

    val pubDate = parseArticlePubDate(jsonData) valueOr(fails => {
      failures ++= fails.list
      new Date()
    })

    val image = (for {
      images <- jsonData.parseValue("images")
      fullImage <- images.parseString("full")
      _ <- fullImage.tryToURL.toValidation(FailureResult(s"Extracted image url `$fullImage` is not a valid URL!"))
    } yield fullImage) valueOr(failure => {
      failures += failure
      ""
    })

    failures.toNel match {
      case Some(fails) => fails.failure
      case None =>
        val article = new Article
        article.canonicalLink = articleUrl
        article.title = title
        article.cleanedArticleText = description
        article.publishDate = pubDate
        article.topImage = {
          val img = new Image()
          img.imageSrc = image
          img
        }

        getArrayToSet(jsonData, "seo_tags").foreach(tags => article.tags = tags)

        val additionalData = mutable.Map[String, String]()

        getArrayToSet(jsonData, "verticals").foreach(verticals => additionalData += "verticals" -> verticals.mkString(","))
        getArrayToSet(jsonData, "content_flags").foreach(flags => additionalData += "content_flags" -> flags.mkString(","))

        if (additionalData.nonEmpty) {
          article.additionalData = additionalData.toMap
        }

        article.successNel
    }
  }

  def parseArticlePubDate(jsonData: JValue): ValidationNel[FailureResult, Date] = {
    jsonData.getString("segment_start_date_time").foreach(dateTimeString => {
      return parseDate(dateTimeString)
    })

    for {
      schedule <- jsonData.parseValue("schedule").liftFailNel
      dateTimeString <- schedule.parseString("started_at").liftFailNel
      date <- parseDate(dateTimeString)
    } yield date
  }

  def parseDate(dateTimeString: String): ValidationNel[FailureResult, Date] = dateTimeString.validateDateTime(ISODateTimeFormat.dateTimeNoMillis()).map(_.toDate)
}

object CheckItBro extends App {
//  val url = "http://live.huffingtonpost.com/r/segment/elizabeth-mitchell-libertys-torch-book/5399f89278c90a20890002e1"
//  val url = "http://live.huffingtonpost.com/r/archive/segment/53b2df66fe34446be00000eb"
//  val url = "http://live.huffingtonpost.com/r/highlight/is-this-new-app-lesbians-answer-to-grindr/53b45def78c90adc28000304"
//  val url = "http://live.huffingtonpost.com/r/highlight/brad-goreski-explains-how-to-wear-red-white-and-blue-with-style-for-july-4/53b2df66fe34446be00000eb"
//  val url = "http://live.huffingtonpost.com/r/archive/segment/53dbd22778c90ae0c2000511"
//  val url = "http://live.huffingtonpost.com/r/segment/5270220cfe344421cd000901"
  val url = "http://live.huffingtonpost.com/r/archive/segment/53ea3ed378c90ac7d40000d2"

  println(s"Converting `$url` to its meta-data API call URL")
  HuffPoLiveArticleFetcher.getApiUrl(url) match {
    case Some(apiUrl) => println(s"We converted `$url` into `$apiUrl` as expected :D")
    case None => println("Y U NO FINDY API URL FOR: " + url)
  }

  println()

  val badUrl = "http://example.com/this-should-not-convert-to/an/api/url"
  println(s"Verifying that a bad URL ($badUrl) will not convert to an meta-data API URL...")
  HuffPoLiveArticleFetcher.getApiUrl(badUrl) match {
    case Some(apiUrl) => println(s"Y DID WE convert our bad URL ($badUrl) into `$apiUrl`?! O_o")
    case None => println(s"We did not convert the bad URL ($badUrl) as expected :D")
  }

  println()
  println("We'll now create a goose article for the HuffPoLive URL: " + url)

  HuffPoLiveArticleFetcher.getGooseArticle(url) match {
    case Success(article) =>
      println("YAY! We successfully got an article!")
      println(s"canonicalLink: ${article.canonicalLink}")
      println(s"title: ${article.title}")
      println(s"cleanedArticleText: ${article.cleanedArticleText}")
      println(s"topImage.getImageSrc: ${article.topImage.getImageSrc}")
      println(s"tags: ${article.tags}")
      println(s"pubDate: ${new DateTime(article.publishDate.getTime)}")
      println("additionalData: " + article.additionalData)
    case Failure(fails) =>
      println("We done failed!")
      fails.foreach(println)
  }

}
