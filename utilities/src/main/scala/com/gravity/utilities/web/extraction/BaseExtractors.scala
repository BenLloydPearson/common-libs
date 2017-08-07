/**
 * Place any extraction logic to be consumed by [[com.gravity.goose.Goose]] here
 */
package com.gravity.utilities.web.extraction

import com.gravity.goose.Article
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.ScalaMagic._
import com.gravity.goose.extractors.{AdditionalDataExtractor, ContentExtractor}
import org.jsoup.nodes.Element
import scala.Predef
import scala.collection._
import org.jsoup.select.Selector

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 4/13/12
 * Time: 8:41 AM
 */

/**
 * Overrides the `getTitle` method of the [[com.gravity.goose.extractors.ContentExtractor]] trait and provides an
 * abstract method: `cleanTitle(rawTitle: String): String` that will be called from the `getTitle` method of [[com.gravity.goose.extractors.ContentExtractor]]
 * Note that if the article's title text is null or empty, `cleanTitle` will not be called and an empty `String` ("") will be returned instead.
 */
abstract class TitleCleanerExtractor extends ContentExtractor {
  /**
   * Implement your own title cleaning logic here. It will be called for all non-empty non-null titles
   * @param rawTitle The text extracted from the title element within the head of an article's html
   * @return the cleaned title to be set on this article
   */
  def cleanTitle(rawTitle: String): String

  override def getTitle(article: Article): String = {
    val titleElem = article.doc.getElementsByTag("title")
    if (titleElem == null || titleElem.isEmpty) return emptyString

    cleanTitle(titleElem.first().text())
  }
}

/**
 * Use this to make sure nothing is modified from the article's title text
 */
object LeaveTitleAsIsContentExtractor extends TitleCleanerExtractor {
  override def cleanTitle(rawTitle: String): String = rawTitle
}

/**
 * Extracts one piece of the raw article title that is split by the specified `delimiter`
 * @param selectTitleIndex Specifies which of the split pieces to return. A positive value counts from the beginning
 *                         whereas a negative value counts back from the end
 * @param delimiter specifies which string to split on
 */
class DelimitedTitleContentExtractor(selectTitleIndex: Int, delimiter: String = " - ") extends TitleCleanerExtractor {

  override def cleanTitle(rawTitle: String): String = {
    val pieces = tokenize(rawTitle, delimiter)
    val useIndex = if (selectTitleIndex < 0) {
      pieces.length + selectTitleIndex
    } else {
      selectTitleIndex
    }

    val titlePiece = if (pieces.isDefinedAt(useIndex)) {
      pieces(useIndex)
    } else {
      return emptyString
    }

    if (isNullOrEmpty(titlePiece)) return emptyString

    TITLE_REPLACEMENTS.replaceAll(titlePiece) match {
      case mt if (isNullOrEmpty(mt)) => rawTitle
      case gotIt => gotIt
    }
  }
}

object PipeTitleFirstPieceContentExtractor extends DelimitedTitleContentExtractor(0, " | ")

object DashTitleFirstPieceContentExtractor extends DelimitedTitleContentExtractor(0)

object DashTitleSecondPieceContentExtractor extends DelimitedTitleContentExtractor(1)

object DashTitleLastPieceContentExtractor extends DelimitedTitleContentExtractor(-1)

/**
 * Removes the specified `suffix` from the end of the title if the raw title ends with that `suffix`
 * @param suffix The text to remove from the end of the raw title
 */
class RemoveSuffixTitleCleaner(suffix: String) extends TitleCleanerExtractor {
  def cleanTitle(rawTitle: String): String = {
    if (rawTitle.endsWith(suffix)) {
      rawTitle.substring(0, rawTitle.length - suffix.length)
    } else {
      rawTitle
    }
  }
}

abstract class GravityBaseAdditionalDataExtractor extends AdditionalDataExtractor {
  import com.gravity.utilities.web.ContentUtils.ZZ_elements2BetterElements
  import com.gravity.utilities.grvstrings

  def customExtract(rootElement: Element): immutable.Map[String, String] = immutable.Map.empty[String, String]

  override def extract(rootElement: Element): immutable.Map[String, String] = {
    val baseMap = customExtract(rootElement) ++ super.extract(rootElement)

    (for {
      metaElem <- Selector.select("meta[property=section]", rootElement).headOption
      section <- grvstrings.nullOrEmptyToNone(metaElem.attr("content"))
    } yield {
      baseMap ++ immutable.Map("section" -> section)
    }).getOrElse(baseMap)
  }
}

/**
 * Provides a simple method to implement for overriding the default tag handling done by [[com.gravity.goose.Goose]]
 */
abstract class TagsOverrideAdditionalDataExtractor extends GravityBaseAdditionalDataExtractor {
  /**
   * Receives the full document [[org.jsoup.nodes.Element]] so that you can extract tags that will be returned
   * within the [[scala.collection.Map]]`[String,String]` that [[com.gravity.goose.extractors.AdditionalDataExtractor]] returns
   * @param rootElem the full document [[org.jsoup.nodes.Element]]
   * @return the tags as a [[scala.collection.Set]]`[String]`
   */
  def extractTags(rootElem: Element): Set[String]

  override def customExtract(rootElem: Element): Predef.Map[String, String] = {
    val baseMap = super.customExtract(rootElem)

    val tags = extractTags(rootElem)

    if (tags.isEmpty) {
      baseMap
    } else {
      baseMap ++ immutable.Map("tags" -> tags.mkString(","))
    }
  }

}