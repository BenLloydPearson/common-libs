package com.gravity.domain.aol

import com.gravity.utilities.grvstrings._
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import play.api.data.validation.ValidationError
import play.api.libs.functional.syntax._
import play.api.libs.json._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 12/12/13
 * Time: 4:51 PM
 */
case class AolLink(url: String, text: String, showVideoIcon: Boolean = AolLink.showVideoIconDefault) {
  def textWithOptionalVideoText: String = if (showVideoIcon) s"$text *video" else text

  override def toString: String = Json.stringify(Json.toJson(this)(AolLink.jsonWrites))

  def urlOpt: Option[String] = url.noneForEmpty
}

object AolLink {
  val showVideoIconDefault = false

  implicit val jsonWrites: Writes[AolLink] = Json.writes[AolLink]

  val internalJsonReads: Reads[AolLink] = (
    (__ \ "url").readNullable[String].filter(ValidationError("Invalid URL."))
                                            (sOpt => sOpt.isEmpty || sOpt.exists(s => s.isEmpty || s.tryToURL.isDefined)) and
    (__ \ "text").read[String] and
    (__ \ "showVideoIcon").readNullable[Boolean]
  )(
    (urlOpt: Option[String], text: String, showVideoIconOpt: Option[Boolean]) =>
      AolLink(urlOpt.getOrElse(emptyString), text, showVideoIconOpt.getOrElse(showVideoIconDefault))
  )

  val internalJsonFormat: Format[AolLink] = Format(internalJsonReads, jsonWrites)
  val internalJsonSeqFormat: Format[Seq[AolLink]] = Format(Reads.seq(internalJsonReads), Writes.seq(jsonWrites))
  val internalJsonListFormat: Format[List[AolLink]] = Format(Reads.list(internalJsonReads), Writes.list(jsonWrites))

  val aolFeedJsonReads: Reads[AolLink] = (
    (__ \ "href").readNullable[String].map(_.map(_.trim))
                                      .filter(ValidationError("Invalid URL."))
                                             (sOpt => sOpt.isEmpty || sOpt.exists(s => s.isEmpty || s.tryToURL.isDefined)) and
    (__ \ "text").read[String] and
    (__ \ "showVideoIcon").readNullable[Boolean]
  )(
    (hrefOpt: Option[String], text: String, showVideoIconOpt: Option[Boolean]) =>
      AolLink(hrefOpt.filter(_.nonEmpty).getOrElse("http://www.aol.com/"), text, showVideoIconOpt.getOrElse(showVideoIconDefault))
  )

  implicit val listDefaultValueWriter: DefaultValueWriter[List[AolLink]] = DefaultValueWriter.jsonDefaultValueWriter[List[AolLink]]
}

case class AolCategoryMeta(id: Int, slug: String)