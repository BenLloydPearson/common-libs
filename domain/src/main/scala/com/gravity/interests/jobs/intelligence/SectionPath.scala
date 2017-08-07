package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvstrings._
import com.gravity.utilities.swagger.adapter.{DefaultValueWriter, ParameterType}
import com.gravity.utilities.web.{ApiParam, ApiRequest}
import play.api.libs.json._

import scala.collection._
import scala.collection.immutable.IndexedSeq
import scalaz.syntax.std.option._


/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
@SerialVersionUID(7780049395882608928l)
case class SectionPath(paths:Seq[String]) {
  override def toString: String = paths.mkString("|")

  def isEmpty: Boolean = paths.isEmpty
  def nonEmpty: Boolean = !isEmpty

  def noneIfEmpty: Option[SectionPath] = if (isEmpty) None else Some(this)

  def sectionKeys(siteGuid: String): Set[SectionKey] = {
    sectionStrings.map(SectionKey(siteGuid, _)).toSet
  }
  def sectionKeys(siteKey: SiteKey): Set[SectionKey] = {

    sectionStrings.map(SectionKey(siteKey,_)).toSet
  }

  def sections: IndexedSeq[Seq[String]] = (1 to paths.size) map (idx=>{
    paths.slice(0, idx)
  })

  def sectionStrings: IndexedSeq[String] = sections.map(_.mkString("|"))
}

object SectionPath {
 import com.gravity.logging.Logging._
  def apply(singleValue: String) : SectionPath = SectionPath(Seq(singleValue))

  val empty: SectionPath = SectionPath(Seq.empty[String])

  val default: SectionPath = SectionPath(Seq("default"))

  def fromParam(path:String) : Option[SectionPath] = {
    if (path.isEmpty) return None

    try {
      SectionPath(path.splitBetter("|").toSeq).some
    } catch {
      case ex: Exception => {
        warn(ex,"Exception whilst trying to split a SectionPath with the following string: {0}", path)
        None
      }
    }
  }

  implicit val jsonFormat: Format[SectionPath] = Format[SectionPath](
    Reads[SectionPath] {
      case obj: JsObject => (obj \ "paths").validate[Seq[String]].map(SectionPath.apply)
      case _ => JsError()
    },
    Writes[SectionPath](sp => Json.obj("paths" -> Json.toJson(sp.paths)))
  )

  implicit val defaultValueWriter: DefaultValueWriter[SectionPath] with Object {def serialize(t: SectionPath): String} = new DefaultValueWriter[SectionPath] {
    override def serialize(t: SectionPath): String = t.sectionStrings.mkString(",")
  }
}

case class ApiSectionPathParam(key: String, required: Boolean, description: String,
                               defaultValue: SectionPath = SectionPath(Seq.empty),
                               exampleValue: String = "most_specific|least_specific")
extends ApiParam[SectionPath](key, required, description, defaultValue, exampleValue, maxChars = 64000, minChars = 0,
                              typeDescription = "A pipe delimited list of section names, going from most general to most specific") {
  override def apply(implicit request: ApiRequest): Option[SectionPath] = {
    request.get(key) match {
      case Some(str) => SectionPath.fromParam(str)
      case None => None
    }
  }

  override def swaggerType: ParameterType = ParameterType.string
}
