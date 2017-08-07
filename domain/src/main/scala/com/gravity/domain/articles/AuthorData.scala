package com.gravity.domain.articles

import net.liftweb.json.JsonAST.JArray
import net.liftweb.json._
import play.api.libs.json._
import com.gravity.utilities.grvjson._
import scala.collection._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.ScalaMagic

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 7/19/13
 * Time: 10:21 AM
 */
case class AuthorData(authors: Seq[Author]) extends Seq[Author] with SeqLike[Author, Seq[Author]] {

  def length: Int = authors.length

  def iterator: Iterator[Author] = authors.iterator

  override def apply(idx: Int): Author = authors(idx)

  def nonEmptyOnly: AuthorData = AuthorData(filter(_.nonEmpty))

  def nonEmptyOrNone: Option[AuthorData] = {
    val neData = nonEmptyOnly
    if (neData.isEmpty) None else Some(neData)
  }

  lazy val headAuthor: Option[Author] = headOption

  def plainOldAuthorName: String = headAuthor.map(_.name).getOrElse(emptyString)
  def plainOldAuthorLink: String = headAuthor.flatMap(_.urlOption).getOrElse(emptyString)

  override lazy val toString: String = {
    nonEmptyOrNone match {
      case Some(nonEmptyAuthors) => {
        if (nonEmptyAuthors.length > 1) {
          val sb = new StringBuilder
          var pastFirst = false
          for (author <- nonEmptyAuthors) {
            if (pastFirst) {
              sb.append(" & ")
            } else {
              pastFirst = true
            }
            author.buildToString(sb)
          }
          sb.toString()
        } else {
          nonEmptyAuthors.head.toString
        }
      }
      case None => "NO_AUTHOR"
    }
  }
}

object AuthorData {
  implicit val authorFormat: Format[Author] = Author.jsonFormat

  implicit val jsonFormat: Format[AuthorData] = Format(
    Reads[AuthorData] {
      case obj: JsObject if obj.iffFieldNames("authors") => (obj \ "authors").validate[Seq[Author]].map(AuthorData.apply)

      // This is here to support reading in Liftweb-serialized AuthorData, which takes SeqLike and changes
      // serialization from { "authors": [...] } to [...].
      case JsArray(vals) => vals.map(Json.fromJson[Author](_)).extrude.map(AuthorData.apply)

      case x => JsError(s"Couldn't read $x to AuthorData.")
    },
    Writes[AuthorData](ad => Json.obj("authors" -> ad.authors))
  )

  /** @deprecated Will die as soon as Liftweb in our codebase dies soon. */
  object LiftwebLenientAuthorDataSerializer extends Serializer[AuthorData] {
    override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), AuthorData] = {
      case (TypeInfo(cl, _), JArray(vals)) if cl.toString.indexOf("AuthorData") != -1 =>
        AuthorData(vals.toSeq.map(Extraction.extract[Author]))

      case (TypeInfo(cl, _), obj @ JObject(fields)) if cl.toString.indexOf("AuthorData") != -1 =>
        AuthorData(fields.find(_.name == "authors").toSeq.flatMap(_.value match {
          case JArray(vals) => vals.map(Extraction.extract[Author])
          case _ => throw new MappingException(s"Couldn't extract $obj to AuthorData.")
        }))
    }

    override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case ad: AuthorData => JObject(List(JField("authors", JArray(ad.authors.toList.map(Extraction.decompose(_))))))
    }
  }

  def apply(singleAuthor: Author): AuthorData = AuthorData(Seq(singleAuthor))
  def apply(singleAuthorName: String): AuthorData = apply(Author(singleAuthorName))

  val imgStartToken = "<img src=\""
  val imgStartTokenLen: Int = imgStartToken.length
  val imgEndToken = "\"/>"
  val imgEndTokenLen: Int = imgEndToken.length

  def fromImageUrlAndByline(imgUrl: java.net.URL, byline: String): AuthorData = {
    val imageOption = Some(imgUrl.toExternalForm)

    val ampPos = byline.indexOf('&')
    if (ampPos > -1) {
      val author1 = Author(byline.substring(0, ampPos).trim, imageOption = imageOption)
      val author2 = Author(byline.substring(ampPos + 1).trim)

      return if (author2.nonEmpty) AuthorData(Seq(author1, author2)) else AuthorData(author1)
    }

    val commaPos = byline.indexOf(',')
    if (commaPos > -1) {
      val titleOption = byline.substring(commaPos + 1).trim match {
        case title if title.nonEmpty => Some(title)
        case _ => None
      }
      return AuthorData(Author(byline.substring(0, commaPos).trim, titleOption = titleOption, imageOption = imageOption))
    }

    AuthorData(Author(byline.trim, imageOption = imageOption))
  }

  def fromByline(byline: String): AuthorData = {
    if (ScalaMagic.isNullOrEmpty(byline)) return empty

    val imgStart = byline.indexOf(imgStartToken)

    if (imgStart > -1) {
      println("We have an image in this byline don't we:")
      println(byline)
      val imgEnd = byline.indexOf(imgEndToken, imgStart)
      val image = byline.substring(imgStart + imgStartTokenLen, imgEnd)
      val imageOption = image.tryToURL.map(_ => image)

      val ampPos = byline.indexOf('&', imgEnd)
      if (ampPos > -1) {
        val author1 = Author(byline.substring(imgEnd + imgEndTokenLen, ampPos).trim, imageOption = imageOption)
        val author2 = Author(byline.substring(ampPos + 1).trim)

        return if (author2.nonEmpty) AuthorData(Seq(author1, author2)) else AuthorData(author1)
      }

      val commaPos = byline.indexOf(',', imgEnd)
      if (commaPos > -1) {
        val titleOption = byline.substring(commaPos + 1).trim match {
          case title if title.nonEmpty => Some(title)
          case _ => None
        }
        return AuthorData(Author(byline.substring(imgEnd + imgEndTokenLen, commaPos).trim, titleOption = titleOption, imageOption = imageOption))
      }
    }

    apply(byline)
  }
  val empty: AuthorData = AuthorData(Seq.empty[Author])
}

case class Author(name: String, urlOption: Option[String] = None, titleOption: Option[String] = None, imageOption: Option[String] = None) {
  def isEmpty: Boolean = this == Author.empty
  def nonEmpty: Boolean = !isEmpty
  def noneIfEmpty: Option[Author] = if (isEmpty) None else Some(this)

  def buildToString(sb: StringBuilder): StringBuilder = {
    sb.append(name)
    urlOption.foreach(url => sb.append(", link: '").append(url).append("'"))
    titleOption.foreach(title => sb.append(", title: '").append(title).append("'"))
    imageOption.foreach(image => sb.append(", image: '").append(image).append("'"))
    sb
  }

  override lazy val toString: String = {
    buildToString(new StringBuilder).toString()
  }
}

object Author {
  val empty: Author = Author(emptyString)

  implicit val jsonFormat: Format[Author] = Json.format[Author]
}
