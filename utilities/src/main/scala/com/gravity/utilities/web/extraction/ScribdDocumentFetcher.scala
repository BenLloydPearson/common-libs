package com.gravity.utilities.web.extraction

import java.util.regex.Pattern

import com.gravity.goose.Article
import com.gravity.goose.images.Image
import com.gravity.goose.outputformatters.StandardOutputFormatter
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.time.GrvDateMidnight
import com.gravity.utilities.web.ContentUtils.{ZZ_elements2BetterElements, gravityBotUserAgent}
import com.gravity.utilities.web.{ContentUtils, HttpArgumentsOverrides}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Selector

import scala.collection._
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 1/11/13
 * Time: 3:19 PM
 */
object ScribdDocumentFetcher {
 import com.gravity.logging.Logging._
  val scribdHostEndsWith = ".scribd.com"
  val maxBytes = 5242880
  val headSelector = "head"
  val tagsSelector = "meta[name=keywords]"
  val infoUrlPrepend = "http://www.scribd.com/mobile/doc/"
  val infoUrlPostpend = "/info"
  val docNumberPattern: Pattern = """^http://[a-z]+\.scribd\.com/doc/(\d{3,})/.*$""".r.pattern
  val dateParser: DateTimeFormatter = DateTimeFormat.forPattern("MM / dd / yyyy")
  val contentSelector = "div.html_text.plain"
  val brRemoverReplacements: ReplaceSequence = buildReplacement("<BR>", " ").chain("""\s{2,}""", " ")
  val httpArgumentsOverrides: HttpArgumentsOverrides = HttpArgumentsOverrides(optUserAgent = gravityBotUserAgent.some)

  def getInfoUrl(url: String): Option[String] = {
    val matcher = docNumberPattern.matcher(url)
    if (matcher.find()) {
      val docNumber = matcher.group(1)
      Some(infoUrlPrepend + docNumber + infoUrlPostpend)
    } else {
      None
    }
  }

  def getLargeImage(url: String): Option[String] = {
    def getBackgroundUrl(div: Element): Option[String] = {
      val style = div.attr("style")
      val startAt = style.indexOf("background-image: url(") + 22
      if (startAt < 22) return None

      val endAt = style.indexOf(");", startAt)

      val imgUrl = style.substring(startAt, endAt)

      imgUrl.tryToURL match {
        case Some(_) => imgUrl.some
        case None => None
      }
    }

    for {
      infoUrl <- getInfoUrl(url)
      html <- ContentUtils.getWebContentAsValidationString(infoUrl, maxBytes = maxBytes, argsOverrides = httpArgumentsOverrides.some).toOption
      doc <- tryToSuccessNEL(Jsoup.parse(html), (ex: Exception) => ex).toOption
      imageDiv <- Selector.select("div#doc-info div.thumbnail", doc).headOption
      largeImageUrl <- getBackgroundUrl(imageDiv)
    } yield largeImageUrl
  }

  def isScribdUrl(url: String): Boolean = validateScribdUrl(url).toOption.isDefined

  def validateScribdUrl(url: String): ValidationNel[FailureResult, String] = {
    url.tryToURL match {
      case Some(u) if u.getAuthority != null && u.getAuthority.endsWith(scribdHostEndsWith) => url.successNel
      case None => InvalidUrlFailureResult(url).failureNel
      case _ => NonScribdUrlFailureResult(url).failureNel
    }
  }

  def getAsGooseArticle(url: String): ValidationNel[FailureResult, Article] = {
    validateScribdUrl(url) match {
      case Success(_) =>
        ContentUtils.getWebContentAsValidationString(url, maxBytes = maxBytes, argsOverrides = httpArgumentsOverrides.some) match {
          case Success(html) =>
            for {
              doc <- tryToSuccessNEL(Jsoup.parse(html), (ex: Exception) => UnableToParseHtmlFailureResult(ex, html, url))
              contentElem <- Selector.select(contentSelector, doc).headOption.toValidationNel(ElementParsingFailureResult(contentSelector, url, html, doc))
              _ <- validateElementTextNonEmpty(contentElem, "content")
              head <- Selector.select(headSelector, doc).headOption.toValidationNel(ElementParsingFailureResult(headSelector, url, html, doc))
              script <- findScribdCurrentDocScript(head).toValidationNel(ElementParsingFailureResult("script", url, html, doc))
              fields <- getScribdJsonDoc(script.html()).toValidationNel(ScribdJsonParsingFailureResult(script, url))
            } yield {
              val article = new Article

              Selector.select(tagsSelector, head).headOption.foreach(tags => {
                val tagsString = tags.attr("content")
                if (tagsString.nonEmpty) {
                  val tagSet = tokenize(tagsString, ", ").toSet
                  if (tagSet.nonEmpty) {
                    article.tags = tagSet
                  }
                }
              })

              article.title = brRemoverReplacements.replaceAll(fields.title).trim
              article.cleanedArticleText = StandardOutputFormatter.getFormattedText(contentElem)
              article.canonicalLink = fields.url
              article.topImage = fields.topImage
              article.publishDate = fields.publishDate.toDate

              article.additionalData = Map(
                "author" -> fields.author.name,
                "authorLink" -> fields.author.url,
                "summary" -> fields.description,
                "isPaid" -> fields.isPaid.toString
              )

              article
            }
          case Failure(f) => f.failureNel
        }
      case Failure(fails) => fails.failure
    }
  }

  def validateElementTextNonEmpty(elem: Element, label: String): ValidationNel[FailureResult, String] = {
    val text = elem.text()
    if (text.isEmpty) {
      RequiredElementEmptyFailureResult(elem, label).failureNel
    } else {
      text.successNel
    }
  }

  def findScribdCurrentDocScript(head: Element): Option[Element] = {
    import scala.collection.JavaConversions._
    Selector.select("script", head).find(e => {
      val text = e.html()
      text.startsWith("Scribd.current_doc = ")
    })
  }

  def getScribdJsonDoc(script: String): Option[ScribdDocumentFields] = {
    trace("Attempting to parse Scribd JavaScript: " + script)

    import com.gravity.utilities.grvjson._
    import net.liftweb.json._

    implicit val formats = baseFormats

    if (script.isEmpty) return None

    val jsonStringRaw = script.stripPrefix("Scribd.current_doc = ")
    val jsonString = {
      val endIndexHint = jsonStringRaw.indexOf("};$(document)")
      if (endIndexHint > 1) {
        jsonStringRaw.substring(0, endIndexHint + 1)
      } else {
        jsonStringRaw
      }
    }

    trace("Attempting to parse Scribd JSON: " + jsonString)

    try {
      val json = parse(jsonString)
      for {
        url <- (json \ "url").stringOption
        title <- (json \ "title").stringOption
        description <- (json \ "description").stringOption
        author <- (json \ "sharing_data" \ "author").tryExtract[ScribdAuthor]
        isPaid <- (json \ "is_paid").booleanOption
        image <- (json \ "thumbnail_url").stringOption
        createAt <- (json \ "created_at").stringOption
        pubDate <- createAt.tryToDateMidnight(dateParser)
      } yield ScribdDocumentFields(url, title, description, author, isPaid, image, pubDate)
    } catch {
      case ex: Exception =>
        trace(ex, "Failed to parse Scribd JSON:\n" + jsonString)
        None
    }
  }
}

case class InvalidUrlFailureResult(url: String) extends FailureResult("'" + url + "' is not a valid URL!", None)

case class NonScribdUrlFailureResult(url: String) extends FailureResult("'" + url + "' is not a valid Scribd URL!", None)

case class UnableToParseHtmlFailureResult(ex: Exception, html: String, url: String) extends FailureResult("Failed to parse html from: '" + url + "' into a Jsoup.Document!", ex.some)

case class RequiredElementEmptyFailureResult(elem: Element, label: String) extends FailureResult("Element '" + label + "' must contain non-empty text!", None)

case class ElementParsingFailureResult(selector: String, url: String, html: String, doc: Document) extends FailureResult("Failed to get element by '" + selector + "' from URL: '" + url + "'!", None)

case class ScribdJsonParsingFailureResult(script: Element, url: String) extends FailureResult("Failed to parse Scribd Document JSON from script string: '" + script.text() + "' and URL: '" + url + "'!", None)

case class ScribdAuthor(url: String, name: String)
case class ScribdDocumentFields(url: String, title: String, description: String, author: ScribdAuthor, isPaid: Boolean, image: String, publishDate: GrvDateMidnight) {
  lazy val largeImageOption: Option[String] = ScribdDocumentFetcher.getLargeImage(url)

  def topImage: Image = {
    val topImg = new Image
    topImg.imageSrc = largeImageOption.getOrElse(image)
    topImg
  }
}
