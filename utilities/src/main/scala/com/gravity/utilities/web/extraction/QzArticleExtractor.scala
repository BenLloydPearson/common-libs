package com.gravity.utilities.web.extraction

import scalaz.Scalaz._
import com.gravity.utilities.grvz._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities._
import com.gravity.utilities.web.ContentUtils
import com.gravity.utilities.web.ContentUtils.{ZZ_elements2BetterElements}
import com.gravity.goose.Article
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.select.{Elements, Selector}
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}
import scalaz.Validation

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 9/27/13
  * Time: 10:31 AM
  */
object QzArticleExtractor {
 import com.gravity.logging.Logging._

  val counterCategory = "QZ Article Extractor"

  val maxBytes = 5242880

  // "2013-04-11 22:23:51"
  val pubDatePattern = "yyyy-MM-dd HH:mm:ss"
  val pubDateFormat: DateTimeFormatter = DateTimeFormat.forPattern(pubDatePattern)

  val noContentFailure: Validation[FailureResult, String] = FailureResult("No content paragraphs found in document!").failure[String]

  def validateSelectionNonEmpty(selector: String, fromElem: Element): Validation[FailureResult, Elements] = {
    val elems = new Elements(fromElem)
    validateSelectionNonEmpty(selector, elems)
  }

  def validateSelectionNonEmpty(selector: String, fromElems: Elements): Validation[FailureResult, Elements] = {
    val res = Selector.select(selector, fromElems)
    if (res.isEmpty) {
      FailureResult("Selector returned empty result: " + selector).failure
    } else {
      res.success
    }
  }

  def validateAttributeNonEmpty(attr: String, elem: Element): Validation[FailureResult, String] = {
    val res = elem.attr(attr)
    if (ScalaMagic.isNullOrEmpty(res)) {
      FailureResult("Attribute value empty or does not exists for attr: " + attr + " within element: " + elem).failure
    } else {
      res.success
    }
  }

  def validateContent(elem: Element): Validation[FailureResult, String] = {
    val paragraphElems = Selector.select("div.item-body > div > p", elem)
    if (paragraphElems.isEmpty) {
      noContentFailure
    } else {
      import scala.collection.JavaConversions._
      val iter = paragraphElems.iterator()
      val paragraphs = for {
        p <- iter
        text = p.text()
        if text.nonEmpty
      } yield text

      if (paragraphs.isEmpty) {
        noContentFailure
      } else {
        paragraphs.mkString("\n").success
      }
    }
  }

  def validateElementTextNonEmpty(elems: Elements, label: String): Validation[FailureResult, String] = {
    val res = elems.text()
    if (ScalaMagic.isNullOrEmpty(res)) {
      FailureResult("Element text for `" + label + "` was empty or missing!\nElements:\n" + elems).failure
    } else {
      res.success
    }
  }

  def validateQzUrl(url: String): Validation[FailureResult, String] = {
    for {
      asUrl <- url.tryToURL.toValidation(FailureResult("Invalid URL: " + url))
      host <- asUrl.getAuthority.noneForEmpty.toValidation(FailureResult("No URL authority found for URL: " + url))
      _ <- if (host === "qz.com") true.success else FailureResult("Not a QZ URL: " + url).failure
    } yield url
  }

  def getAsGooseArticle(url: String): Validation[FailureResult, Article] = {
    for {
      _ <- validateQzUrl(url)
      html <- ContentUtils.getWebContentAsValidationString(url, maxBytes = maxBytes)
      doc <- tryToSuccess(Jsoup.parse(html), (ex: Exception) => UnableToParseHtmlFailureResult(ex, html, url))
      header <- validateSelectionNonEmpty("div.item-header > header", doc)
      _ = println("header: " + header)
      titleElem <- validateSelectionNonEmpty("h1", header)
      title <- validateElementTextNonEmpty(titleElem, "title")
      pubDateElem <- validateSelectionNonEmpty("div.byline > div.byline-info > span.timestamp", header) <+> validateSelectionNonEmpty("p.byline > span.timestamp", header)
      _ = println("pubDateElem: " + pubDateElem)
      pubDateString <- validateAttributeNonEmpty("title", pubDateElem.headOption.get)
      pubDate <- pubDateString.tryToDateTime(pubDateFormat).toValidation(FailureResult("Failed to parse pubDate string: `" + pubDateString + "` for format: `" + pubDatePattern + "`!"))
      content <- validateContent(doc)
    } yield {
      val article = new Article
      article.title = title
      article.cleanedArticleText = content
      article.publishDate = pubDate.toDate

      // <meta property="twitter:image" content="http://qzprod.files.wordpress.com/2013/09/reuters-tablets.jpg?w=640">
      Selector.select("meta[property=twitter:image]", doc).headOption.foreach(metaImage => {
        metaImage.attr("content").tryToURL.foreach(imgUrl => article.topImage.imageSrc = imgUrl.toExternalForm)
      })

      article
    }
  }
}
