package com.gravity.utilities.web.extraction

import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.web.ContentUtils.{ZZ_elements2BetterElements, gravityBotUserAgent}
import com.gravity.utilities.web.{ContentUtils, HttpArgumentsOverrides}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Selector

import scala.collection.JavaConversions._
import scala.collection._
import scala.util.matching.Regex
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 1/16/13
 * Time: 10:26 AM
 */
object ProBoardsDirectoryScraper {
  val directoryIndexUrl = "http://www.proboards.com/forum-directory"
  val boardCountRegex: Regex = """^([a-zA-Z -]+)(\s\(\d{1,5}\))\s*$""".r
  val httpArgumentsOverrides: HttpArgumentsOverrides = HttpArgumentsOverrides(optUserAgent = gravityBotUserAgent.some)

  def getAllDirectories: ValidationNel[FailureResult, Seq[ProBoardsDirectory]] = {
    for {
      headers <- getDirectoryHeaders
      dirs <- getDirectories(headers)
    } yield dirs
  }

  def getDirectories(headers: Seq[ProBoardsDirectoryHeader]): ValidationNel[FailureResult, Seq[ProBoardsDirectory]] = {
    val failures = mutable.Buffer[FailureResult]()

    val dirs = for {
      header <- headers
      dir = getDirectory(header) match {
        case Success(d) => d
        case Failure(fails) => return fails.failure
      }
    } yield dir

    dirs.successNel
  }

  def getDirectory(header: ProBoardsDirectoryHeader): ValidationNel[FailureResult, ProBoardsDirectory] = {
    var pageNum = 1
    var noMore = false

    val boards = mutable.Buffer[ProBoardsBoard]()

    while (!noMore) {
      getPageOfBoards(header, pageNum) match {
        case Success(more) if more.nonEmpty =>
          boards ++= more
          pageNum += 1
        case Success(_) => noMore = true
        case Failure(fails) => return fails.failure
      }
    }
    println("Just got " + boards.size + " boards for header: " + header.name)
    ProBoardsDirectory(header, boards.toSeq).successNel
  }

  def getPageOfBoards(header: ProBoardsDirectoryHeader, pageNumber: Int): ValidationNel[FailureResult, Seq[ProBoardsBoard]] = {
    if (pageNumber < 1) return FailureResult("pageNumber MUST be greater than zero! Vaule received was: " + pageNumber).failureNel

    val url = header.url + "/" + pageNumber

    for {
      doc <- getJsoupDoc(url)
      _ <- getElement("div#forum-directory-category > p", doc) match {
        case Some(p) => if (p.text().startsWith("No results found.")) {
          return Seq.empty[ProBoardsBoard].successNel
        } else {
          true.successNel
        }
        case None => true.successNel
      }
      ul <- selectElement("ul#directory-list", doc)
      boards <- extractBoards(ul)
    } yield boards
  }

  def extractBoards(ul: Element): ValidationNel[FailureResult, Seq[ProBoardsBoard]] = {
    val listItems = Selector.select("li", ul)

    val boards = listItems.flatMap((elem: Element) => {
      for {
        a <- Selector.select("a[href]", elem).headOption
        name <- a.text() match {
          case good if good.nonEmpty => good.some
          case bad => None
        }
        url <- a.absUrl("href").tryToURL match {
          case Some(u) => u.toExternalForm.some
          case None => None
        }
        p <- Selector.select("p", elem).headOption
        desc <- p.text() match {
          case good if good.nonEmpty => good.some
          case bad => None
        }
      } yield ProBoardsBoard(name, url, desc)
    })

    if (boards.isEmpty) {
      FailureResult("No boards found in UL element: " + ul.html()).failureNel
    } else {
      boards.toSeq.successNel
    }
  }

  def getDirectoryHeaders: ValidationNel[FailureResult, Seq[ProBoardsDirectoryHeader]] = {
    for {
      doc <- getJsoupDoc(directoryIndexUrl)
      ul <- selectElement("ul#directory-index", doc)
      dirs <- extractDirectories(ul)
    } yield dirs
  }

  def getJsoupDoc(url: String): ValidationNel[FailureResult, Document] = {
    for {
      html <- ContentUtils.getWebContentAsValidationString(url, argsOverrides = httpArgumentsOverrides.some).toNel
      doc <- tryToSuccessNEL(Jsoup.parse(html, url), (ex: Exception) => FailureResult("Jsoup failed to parse ProBoard URL: '" + url + "' and HTML: " + html, ex))
    } yield doc
  }

  def selectElement(query: String, fromElement: Element): ValidationNel[FailureResult, Element] = {
    getElement(query, fromElement).toValidationNel(FailureResult("Failed to select \"" + query + "\" from HTML: " + fromElement))
  }

  def getElement(query: String, fromElement: Element): Option[Element] = Selector.select(query, fromElement).headOption

  def extractDirectories(ul: Element): ValidationNel[FailureResult, Seq[ProBoardsDirectoryHeader]] = {
    val listItems = Selector.select("li > a", ul)

    val dirs = listItems.flatMap((elem: Element) => {
      for {
        text <- elem.text() match {
          case good if good.endsWith(")") => good.some
          case bad => None
        }
        url <- elem.absUrl("href").tryToURL match {
          case Some(u) => u.toExternalForm.some
          case None => None
        }
      } yield {
        val name = boardCountRegex.replaceFirstIn(text, "$1")
        ProBoardsDirectoryHeader(name, url)
      }
    })

    if (dirs.isEmpty) {
      FailureResult("No Directory items found in UL element: " + ul.html()).failureNel
    } else {
      println("Just extracted " + dirs.size + " directory headers")
      dirs.toSeq.successNel
    }
  }

}

trait ProBoardsWebItem {
  def name: String
  def url: String
  def description: String
}

case class ProBoardsDirectoryHeader(name: String, url: String) extends ProBoardsWebItem {
  val description: String = emptyString
}

object ProBoardsDirectoryHeader {
  val empty: ProBoardsDirectoryHeader = ProBoardsDirectoryHeader(emptyString, emptyString)
}

case class ProBoardsBoard(name: String, url: String, description: String) extends ProBoardsWebItem

case class ProBoardsDirectory(header: ProBoardsDirectoryHeader, forums: Seq[ProBoardsBoard])

object ProBoardsDirectory {
  val empty: ProBoardsDirectory = ProBoardsDirectory(ProBoardsDirectoryHeader.empty, Seq.empty[ProBoardsBoard])
}
